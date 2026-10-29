#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""
Subspace MCP relay

A thin per-session stdio MCP server. It owns no schedule and no persistence; all
of that lives in the subspace server (server/subspace-server.py in the subspace
repo). This relay only knows how to:

  - find its own Claude Code session id and stay current on it (SessionStart hook
    + state file, see ../lib/session_state.py)
  - open a WebSocket to the server and register under that session id
  - receive notifications from the server and deliver them to the agent
  - acknowledge delivered notifications back to the server
  - forward the agent's schedule/subscribe tool calls to the server
  - at a resume, hold the messages that arrived while the session was stopped
    for the SessionStart hook (../hooks/session-start.py), which claims them
    over a local socket and puts them into the first turn

Delivery is mode-aware. The relay reads Claude Code's per-server MCP log (see
../lib/channel_detect.py) to learn whether it was loaded as a channel:
  - loaded as a channel  -> push events as notifications/claude/channel (auto-ack)
  - not a channel        -> buffer events (no auto-ack) and expose a `catch_up`
                            tool the agent calls to pull and ack them
Until detection resolves, events are buffered (never lost). The session is loaded
as a channel when launched with e.g.
`claude --dangerously-load-development-channels plugin:subspace@subspace`.

The server must be running (this relay never starts it). If it isn't reachable,
the relay keeps retrying and the tools report it as unavailable.

Run directly: ./subspace-relay.py
"""

# /// script
# requires-python = ">=3.12"
# dependencies = ["mcp", "anyio", "websockets"]
# ///

import json
import os
import random
import re
import sys
import time
from pathlib import Path

import anyio
from anyio.streams.buffered import BufferedByteReceiveStream
from mcp.server.fastmcp import FastMCP
from mcp.server.lowlevel.server import NotificationOptions
from mcp.server.stdio import stdio_server
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, JSONRPCNotification
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidURI, WebSocketException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import channel_detect
import messaging
import session_state
import wsproto

CHANNEL_METHOD = "notifications/claude/channel"
SERVER_NAME = "subspace"  # used to locate this server's Claude Code MCP log
# How long to wait for Claude Code to log whether we were loaded as a channel.
CHANNEL_DETECT_TIMEOUT_SECONDS = 12.0
CHANNEL_DETECT_POLL_SECONDS = 0.5
# If no marker is found in the first window, keep looking at a slower rate for this
# long after startup, so a slow harness does not leave the session in pull mode.
CHANNEL_LATE_DETECT_SECONDS = 300.0
CHANNEL_LATE_DETECT_POLL_SECONDS = 5.0
# Start time of this process. Channel markers older than this belong to earlier runs
# of a resumed session.
PROCESS_STARTED_AT = time.time()
SESSION_POLL_SECONDS = 5.0
# Settle time before connecting, so a recovered (past-due) event isn't pushed
# into the channel before the client has finished the MCP/channel handshake.
STARTUP_GRACE_SECONDS = 3.0
REQUEST_TIMEOUT_SECONDS = 10.0

# Delivery at resume. The SessionStart hook claims the messages that arrived
# while the session was stopped and puts them into the first turn. Until it
# claims or declines, nothing goes to the channel. The hold ends on its own if
# no hook contacts the relay in time.
HOLD_SECONDS = 15.0
# The hook writes the session state file just before it claims or declines. A
# relay that finds an older file started later in the session (an MCP
# reconnect): no hook is coming, so it does not hold at start. The process age
# would not do: a hot-spare worker is old by the time its relay starts.
START_HOLD_MAX_AGE_SECONDS = 60.0
# The hook waits about 10 s in total, so a claim gives up on the connection and
# on whoami before that.
CLAIM_CONNECT_SECONDS = 5.0
# The server answers whoami after its delivery pass, or after 5 s.
WHOAMI_TIMEOUT_SECONDS = 6.0
# Hook IPC: newline-delimited JSON on a local socket. The hook sends its request
# right after it connects, and its "done" right after it wrote its output.
IPC_LINE_MAX_BYTES = 65536
IPC_REQUEST_SECONDS = 5.0
IPC_DONE_SECONDS = 30.0

# Debounce/coalesce window for push (channel) delivery. A burst of notifications
# (e.g. several PR check/review/comment events landing in one poll) is coalesced
# into ONE channel event instead of several separate interruptions. The window is
# a quiet period, reset on each new arrival, capped so a continuous stream still
# flushes periodically. NOTIFICATIONS_DEBOUNCE_SECONDS overrides the quiet window
# (a float; <= 0 disables debounce and delivers each notification immediately).
DEBOUNCE_DEFAULT_SECONDS = 2.0


def _debounce_window() -> float:
    raw = os.environ.get("NOTIFICATIONS_DEBOUNCE_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return DEBOUNCE_DEFAULT_SECONDS


def _debounce_max(window: float) -> float:
    """Max hold since the first pending item, so a steady stream still flushes."""
    return max(window * 5, 10.0)


# Bounded catch_up: one drain returns at most this many messages / characters, so a
# large backlog (post-eviction/outage/wedge) comes back in ordered chunks instead of
# one unbounded tool result that could jump a turn's context past the window (and
# re-wedge a freshly-recovered session). Overridable for tuning/tests.
CATCHUP_MAX_MESSAGES = 25
CATCHUP_MAX_CHARS = 50_000
# Wedge hint: a connected recipient with a message pending whose relay has not acked
# anything in this long is flagged 'possibly context-wedged' in message_status.
WEDGE_HINT_DEFAULT_SECONDS = 600.0


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return default


def _catchup_max_messages() -> int:
    return _positive_int_env("NOTIFICATIONS_CATCHUP_MAX_MESSAGES", CATCHUP_MAX_MESSAGES)


def _catchup_max_chars() -> int:
    return _positive_int_env("NOTIFICATIONS_CATCHUP_MAX_CHARS", CATCHUP_MAX_CHARS)


def _wedge_hint_seconds() -> float:
    raw = os.environ.get("NOTIFICATIONS_WEDGE_HINT_SECONDS")
    if raw:
        try:
            value = float(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return WEDGE_HINT_DEFAULT_SECONDS


# Reconnect backoff: binary (exponential) growth with +/- jitter, capped at a
# jittered 30 minutes, after which it retries roughly every half hour forever.
# The counter resets only after a connection that stayed up at least
# RECONNECT_STABLE_SECONDS, so a flapping server still backs off.
RECONNECT_INITIAL_SECONDS = 1.0
RECONNECT_MAX_SECONDS = 30 * 60
RECONNECT_JITTER = 0.2  # +/- 20%
RECONNECT_STABLE_SECONDS = 30.0
# Cap the exponent so 2**failures can't blow up (saturates well past the cap).
_RECONNECT_MAX_FAILURES = 16


def _reconnect_delay(failures: int) -> float:
    """Exponential-with-jitter backoff for `failures` consecutive bad attempts."""
    nominal = min(RECONNECT_MAX_SECONDS, RECONNECT_INITIAL_SECONDS * 2**failures)
    return nominal * random.uniform(1.0 - RECONNECT_JITTER, 1.0 + RECONNECT_JITTER)


INSTRUCTIONS = (
    "This plugin delivers notifications (scheduled callbacks, GitHub PR updates, and "
    "agent messages) through the Claude Code channels feature. Events normally arrive "
    'on their own as <channel source="subspace" ...> messages; when one arrives, '
    "surface it to the user. Quiet agent messages that fall below your wake threshold "
    "are held silently — call `catch_up` periodically (and after long pauses) to drain "
    "them. If this session was NOT loaded as a channel, nothing is pushed automatically "
    "and `catch_up` is the only way to retrieve pending updates."
)

mcp = FastMCP(SERVER_NAME, instructions=INSTRUCTIONS)


class DaemonClient:
    """Maintains the WebSocket to the server and bridges it to the channel."""

    def __init__(self) -> None:
        self._ws = None
        self._write_stream = None
        self._req_id = 0
        self._pending: dict[int, object] = {}  # req_id -> memory send stream
        self._registered_session: str | None = None
        self._connected_once = False  # did the current attempt establish a connection?
        # A foreground tool call sets this to break the reconnect backoff and force an
        # immediate attempt, so an active agent never waits out an idle-tuned long sleep.
        self._reconnect_now = anyio.Event()
        self._mode: str | None = (
            None  # None=detecting, "push" (channel), "pull" (catch_up)
        )
        # Why pull mode, when known: the harness's own reason from its 'Channel
        # notifications skipped: <reason>' log line (e.g. "server not in --channels
        # list"), or a detection-timeout note. Surfaced in catch_up so a session
        # that SHOULD have been a channel self-announces instead of sitting in
        # silent pull mode until someone notices it never pushes.
        self._mode_detail: str | None = None
        self._buffer: dict[
            str, dict
        ] = {}  # notification id -> {content, meta}; held until acked
        # Wake-gating buffer (Phase B): message notifies the shim chose not to surface
        # (sub-threshold, or this session isn't a channel). Kept UNACKED so a reconnect
        # re-ships them; flushed when a surfacing message arrives or catch_up drains them.
        self._held: list[dict] = []  # [{id, content, meta}] in arrival order
        # Push-mode debounce: a burst of notifications is coalesced into one
        # channel event. Items wait here until the burst goes quiet (or caps out).
        self._pending_debounce: list[dict] = []  # [{id, content, meta}]
        self._debounce_event = anyio.Event()  # signalled on each push-mode arrival
        self._debounce_first: float | None = None  # monotonic ts of first pending item
        self._debounce_last: float | None = None  # monotonic ts of most recent arrival
        # Delivery at resume: while the hold is on (monotonic deadline in the
        # future), push delivery waits for the SessionStart hook's claim or decline.
        self._hold_deadline: float | None = None
        # Items the hook claimed, unacked until it confirms that it wrote them,
        # and the session they belong to.
        self._claimed: list[dict] = []
        self._claimed_session = ""
        # Ids the hook wrote (id -> session), kept until the next session
        # change so that a replay of one of them is not delivered again; the
        # subset whose ack did not reach the server, to ack on the next live
        # registration of that session.
        self._confirmed: dict[str, str] = {}
        self._ack_pending: set[str] = set()

    def attach_write_stream(self, write_stream) -> None:
        self._write_stream = write_stream

    @property
    def connected(self) -> bool:
        return self._ws is not None

    @property
    def channel_label(self) -> str:
        return {
            "push": "active (delivered as channel events)",
            "pull": "inactive (not a channel) — call catch_up to pull updates",
        }.get(self._mode or "", "detecting")

    def delivery_hint(self) -> str:
        """How updates reach the agent, phrased for the current channel mode — so a
        subscribe confirmation doesn't promise <channel> events to a pull-mode session."""
        if self._mode == "pull":
            detail = f" ({self._mode_detail})" if self._mode_detail else ""
            return (
                f"This session was not loaded as a channel{detail}, so updates won't "
                "arrive automatically — call catch_up to retrieve them."
            )
        if self._mode == "push":
            return "Updates will arrive as <channel> events."
        # Detection not resolved yet (usually resolves to push); cover both.
        return (
            "Updates will arrive as <channel> events, or via the catch_up tool if "
            "this session turns out not to be a channel."
        )

    async def wait_connected(self, timeout: float = 8.0) -> bool:
        """Wait briefly for the connection (covers startup grace / reconnects).

        This is the foreground path — a tool the agent is actively invoking. Nudge the
        reconnect loop to retry NOW instead of waiting out its idle-tuned backoff sleep,
        then poll until connected or `timeout`. (Idle push delivery keeps the long
        backoff; only an actively-waiting caller forces the immediate attempt.)"""
        if self.connected:
            return True
        self._reconnect_now.set()  # an agent is actively waiting — break the backoff
        with anyio.move_on_after(timeout):
            while not self.connected:
                await anyio.sleep(0.1)
        return self.connected

    async def _send_raw(self, method: str, params: dict) -> None:
        if self._write_stream is None:
            return
        notification = JSONRPCNotification(jsonrpc="2.0", method=method, params=params)
        await self._write_stream.send(
            SessionMessage(message=JSONRPCMessage(notification))
        )

    async def _deliver_channel(self, content: str, meta: dict | None) -> None:
        params: dict[str, object] = {"content": content}
        if meta:
            params["meta"] = meta
        await self._send_raw(CHANNEL_METHOD, params)

    async def _ack(self, notification_id) -> bool:
        """Ack `notification_id`; False if the server is not connected now."""
        ws = self._ws
        if ws is None or not notification_id:
            return False
        try:
            await ws.send(json.dumps({"type": wsproto.ACK, "id": notification_id}))
        except (ConnectionClosed, WebSocketException):
            return False
        return True

    async def _handle_message_notify(
        self, notification_id, content: str, meta: dict
    ) -> None:
        """The receiver-side gate for agent messages (Phase B).

        Surface a message only when it is a channel (push) session AND the stamped
        ``level`` clears this session's ``threshold``. On surface, it goes to the
        debounce coalescer and the whole held backlog is flushed alongside it, and
        every surfaced/flushed message is acked. Otherwise the message is held UNACKED
        (sub-threshold, or this session can't push), to be drained by a later surfacing
        message or by catch_up. The ack-on-surface invariant — never ack on mere
        receipt — is what lets a reconnect re-ship anything still held."""
        if self._mode == "push" and messaging.should_surface(
            meta.get("level", "ambient"), meta.get("threshold", "direct")
        ):
            held, self._held = self._held, []
            for item in held:  # flush the quiet backlog in the same channel event
                await self._enqueue_push(item["id"], item["content"], item["meta"])
            await self._enqueue_push(notification_id, content, meta)
        else:
            self._held.append({"id": notification_id, "content": content, "meta": meta})

    async def _enqueue_push(
        self, notification_id, content: str, meta: dict | None
    ) -> None:
        """Deliver a push-mode notification, coalescing bursts via the debounce buffer.

        With debounce disabled (window <= 0) it is delivered (and acked) immediately;
        otherwise it is buffered and the debounce loop flushes the burst as one event.
        A resume hold buffers in both cases.
        """
        if _debounce_window() <= 0 and not self.holding:
            await self._deliver_channel(content, meta)
            await self._ack(notification_id)
            return
        now = time.monotonic()
        if self._debounce_first is None:
            self._debounce_first = now
        self._debounce_last = now
        self._pending_debounce.append(
            {"id": notification_id, "content": content, "meta": meta}
        )
        self._debounce_event.set()

    async def _flush_debounce(self) -> None:
        """Coalesce the pending push notifications into ONE channel event, then ack each.

        Content is the individual messages joined by a blank line; the coalesced meta
        carries the highest severity, kind="batch" when >1 item (else the lone item's
        kind), and count. The server's per-id acked-set is unchanged: every id is acked.
        """
        if self.holding:
            return  # the hook may still claim these for the first turn
        pending = self._pending_debounce
        self._pending_debounce = []
        self._debounce_first = None
        self._debounce_last = None
        if not pending:
            return
        content = "\n\n".join(item["content"] for item in pending)
        severity = (
            "high"
            if any((item["meta"] or {}).get("severity") == "high" for item in pending)
            else "info"
        )
        if len(pending) > 1:
            kind = "batch"
        else:
            kind = str((pending[0]["meta"] or {}).get("kind") or "notification")
        meta = {"severity": severity, "kind": kind, "count": str(len(pending))}
        await self._deliver_channel(content, meta)
        for item in pending:
            await self._ack(item["id"])

    async def debounce_loop(self) -> None:
        """Flush coalesced push notifications once a burst goes quiet (or hits the cap).

        Sleeps on an event each arrival signals, recomputing the flush deadline from the
        last arrival (quiet window) and the first pending item (max hold). No busy loop.
        """
        while True:
            await self._debounce_event.wait()
            self._debounce_event = anyio.Event()
            while self._pending_debounce:
                window = _debounce_window()
                deadline = min(
                    (self._debounce_last or 0.0) + window,
                    (self._debounce_first or 0.0) + _debounce_max(window),
                )
                if self._hold_deadline is not None:
                    deadline = max(deadline, self._hold_deadline)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                fired = False
                with anyio.move_on_after(remaining):
                    await self._debounce_event.wait()
                    self._debounce_event = anyio.Event()
                    fired = True
                if not fired:  # quiet/max window elapsed with no new arrival
                    break
            await self._flush_debounce()

    # ---- delivery at resume: hold, claim, decline ------------------------ #

    @property
    def holding(self) -> bool:
        return (
            self._hold_deadline is not None and time.monotonic() < self._hold_deadline
        )

    def _start_hold(self) -> None:
        self._hold_deadline = time.monotonic() + HOLD_SECONDS
        self._debounce_event.set()  # the flush deadline moved

    def _release_hold(self) -> None:
        self._hold_deadline = None
        self._debounce_event.set()  # flush what the hold kept back

    def _discard_session_buffers(self) -> None:
        """Forget the earlier session's local items. They are unacked, so the
        server still has them under that session."""
        self._buffer = {}
        self._held = []
        self._pending_debounce = []
        self._debounce_first = None
        self._debounce_last = None
        self._claimed = []
        self._confirmed = {}
        self._ack_pending = set()

    def _take_claimable(self) -> list[dict]:
        """Remove and return the held items that pass the session's threshold.

        Items with no level (PR events, callbacks) are not gated and stay for
        channel delivery; sub-threshold messages stay held, as for catch_up."""

        def passes(item: dict) -> bool:
            meta = item.get("meta") or {}
            level = meta.get("level")
            if level is None:
                return False
            try:
                return messaging.should_surface(level, meta.get("threshold", "direct"))
            except KeyError:
                return False

        taken: list[dict] = []
        kept: list[dict] = []
        for item in self._pending_debounce:
            (taken if passes(item) else kept).append(item)
        self._pending_debounce = kept
        if not kept:
            self._debounce_first = None
            self._debounce_last = None
        kept = []
        for item in self._held:
            (taken if passes(item) else kept).append(item)
        self._held = kept
        for notification_id, payload in list(self._buffer.items()):
            item = {"id": notification_id, **payload}
            if passes(item):
                taken.append(item)
                self._buffer.pop(notification_id, None)
        return taken

    async def claim(self, session_id: str) -> list[dict]:
        """Take the messages that arrived while `session_id` was stopped, for the
        hook to put into the first turn. They stay unacked until confirm_claim.

        Registers `session_id` if it is not the registered one (after /resume in
        a running session the relay's environment still has the earlier id).
        Then asks the server with whoami, which replies after the pending items."""
        self._start_hold()  # nothing may reach the channel while the hook waits
        if not await self.wait_connected(CLAIM_CONNECT_SECONDS):
            return []
        ws = self._ws
        try:
            if session_id and session_id != self._registered_session:
                await self._register(ws, session_id)
            await self.request({"type": wsproto.WHOAMI}, timeout=WHOAMI_TIMEOUT_SECONDS)
        except (ConnectionError, TimeoutError, ConnectionClosed, WebSocketException):
            pass  # take what arrived; a miss goes to the channel later
        # confirm_claim acks only if the registered id is still this one.
        self._claimed_session = self._registered_session or ""
        self._claimed = self._take_claimable()
        return list(self._claimed)

    def _is_claimed(self, notification_id) -> bool:
        """True for an item the hook has, or wrote in this session."""
        if notification_id in self._confirmed:
            return True
        return any(item["id"] == notification_id for item in self._claimed)

    async def confirm_claim(self) -> None:
        """The hook wrote the claimed items: ack them and end the hold. Copies
        that a reconnect replayed into the buffers are dropped, not delivered.
        An ack that cannot be sent now waits for the next registration. A claim
        of another session than the registered one is discarded, not acked."""
        claimed, self._claimed = self._claimed, []
        if self._claimed_session != (self._registered_session or ""):
            claimed = []
        ids = {item["id"] for item in claimed}
        self._pending_debounce = [
            i for i in self._pending_debounce if i["id"] not in ids
        ]
        self._held = [i for i in self._held if i["id"] not in ids]
        for notification_id in ids:
            self._buffer.pop(notification_id, None)
        for item in claimed:
            self._confirmed[item["id"]] = self._claimed_session
            if not await self._ack(item["id"]):
                self._ack_pending.add(item["id"])
        self._release_hold()

    async def _send_pending_acks(self) -> None:
        """After a registration: ack what confirm_claim could not. Ids of another
        session are dropped; their items were never acked, so they stay pending."""
        pending, self._ack_pending = self._ack_pending, set()
        for notification_id in pending:
            if self._confirmed.get(notification_id) != self._registered_session:
                self._confirmed.pop(notification_id, None)
                continue
            if not await self._ack(notification_id):
                self._ack_pending.add(notification_id)

    async def decline(self, session_id: str) -> None:
        """A start that is not a resume: end the hold. A new session id (after
        /clear) is registered first, so the watcher does not see a change later
        and start a hold that no hook would end."""
        ws = self._ws
        if ws is not None and session_id and session_id != self._registered_session:
            try:
                await self._register(ws, session_id)
            except (ConnectionClosed, WebSocketException):
                pass  # the reconnect registers the new id
        self._release_hold()

    async def abandon_claim(self) -> None:
        """The hook went away before it confirmed: the claimed items go back to
        channel delivery, unacked, so nothing is lost."""
        claimed, self._claimed = self._claimed, []
        self._release_hold()
        for item in claimed:
            await self._dispatch_notify(item["id"], item["content"], item["meta"])

    async def serve_ipc(self) -> None:
        """Listen for the SessionStart hook on the per-process local socket."""
        path = _ipc_path()
        if path is None:
            return  # no hook can find this relay, so nothing to wait for
        if session_state.secure_state_dir() is None:
            # Another local user could reach the socket; without it no hook can
            # claim, so there is nothing to hold for.
            print(
                f"subspace relay: {path.parent} is not private to this user; "
                "the hook socket stays closed",
                file=sys.stderr,
            )
            return
        if _hook_expected():
            self._start_hold()
        path.unlink(missing_ok=True)
        try:
            listener = await anyio.create_unix_listener(str(path))
        except OSError as exc:
            print(f"subspace relay: hook socket unavailable ({exc})", file=sys.stderr)
            return
        try:
            async with listener:
                await listener.serve(self._handle_ipc)
        finally:
            path.unlink(missing_ok=True)

    async def _handle_ipc(self, stream) -> None:
        """One hook connection: {claim|decline, session_id, source} in, {claimed,
        count, content} out, then {done} in. A connection that ends before "done"
        abandons the claim."""
        try:
            async with stream:
                await self._serve_hook(stream)
        except Exception as exc:  # noqa: BLE001
            # A hook failure must not take the relay down.
            print(f"subspace relay: hook request failed ({exc!r})", file=sys.stderr)
        if self._claimed:  # the hook went away before "done"
            await self.abandon_claim()

    async def _serve_hook(self, stream) -> None:
        lines = BufferedByteReceiveStream(stream)
        request = await _read_ipc_line(lines, IPC_REQUEST_SECONDS)
        if request is None:
            return
        kind = request.get("type")
        if kind == "decline":
            await self.decline(str(request.get("session_id") or ""))
            return
        if kind != "claim":
            return
        items = await self.claim(str(request.get("session_id") or ""))
        reply = {
            "type": "claimed",
            "count": len(items),
            "content": "\n\n".join(item["content"] for item in items),
        }
        await stream.send(json.dumps(reply).encode() + b"\n")
        if not items:
            self._release_hold()
            return
        done = await _read_ipc_line(lines, IPC_DONE_SECONDS)
        if done is not None and done.get("type") == "done":
            await self.confirm_claim()
        else:
            await self.abandon_claim()

    async def _dispatch_notify(self, notification_id, content: str, meta: dict) -> None:
        """Route one delivered item by kind and mode."""
        if notification_id and self._is_claimed(notification_id):
            return  # a replay after a reconnect: the hook has it, the ack follows
        if meta.get("kind") == "message":  # Phase B: gated agent message
            await self._handle_message_notify(notification_id, content, meta)
        elif self._mode == "push":
            await self._enqueue_push(notification_id, content, meta)
        elif notification_id:  # detecting or pull: hold without acking
            self._buffer[notification_id] = {"content": content, "meta": meta}

    async def apply_mode(self, detected: str) -> None:
        """Switch out of buffering once we know whether we're a channel.

        In push mode the PR/scheduled buffer is flushed straight to the debounce path,
        and any agent messages held during detection are re-run through the gate (so
        sub-threshold ones stay held rather than surfacing on mode resolution). In pull
        mode nothing is pushed — catch_up (always registered) is the only drain."""
        if detected == channel_detect.REGISTERED:
            self._mode = "push"
            for notification_id, payload in list(
                self._buffer.items()
            ):  # flush what we held, coalesced through the debounce path
                self._buffer.pop(notification_id, None)
                await self._enqueue_push(
                    notification_id, payload["content"], payload["meta"]
                )
            held, self._held = self._held, []
            for item in held:  # re-evaluate the gate now that we can push
                await self._handle_message_notify(
                    item["id"], item["content"], item["meta"]
                )
        else:  # skipped or unknown: err toward no silent loss
            self._mode = "pull"

    def probe_mode(self) -> tuple[str, str | None]:
        """One look at the MCP logs for this run's channel marker: (verdict, reason).

        Primary: match the channel marker by this session's ids. Claude Code keys each
        MCP log dir by the *session* cwd, which for a worktree session — or any bg
        agent whose process cwd differs from the session cwd — is NOT the relay's
        os.getcwd(), so a cwd-scoped probe reads the wrong dir and never sees the
        marker (persistent pull-mode misdetection). The marker line carries the
        session id, so matching on it survives that divergence. A resumed session
        keeps its id, so only markers written since this process started count.

        Fallback: probe the process cwd and CLAUDE_PROJECT_DIR directly (freshness-
        gated), for marker lines that have no session id. When the ids are known,
        tagged lines in those dirs are ignored: the primary probe did not match them,
        so they belong to a different session in the same cwd."""
        session_ids = session_state.known_session_ids()
        detected, reason = channel_detect.detect_channel_mode_by_session_ex(
            SERVER_NAME, session_ids, newer_than=PROCESS_STARTED_AT
        )
        if detected != channel_detect.UNKNOWN:
            return detected, reason
        candidates = [os.getcwd()]
        env_dir = os.environ.get("CLAUDE_PROJECT_DIR")
        if env_dir and env_dir not in candidates:
            candidates.append(env_dir)
        for candidate in candidates:
            detected, reason = channel_detect.detect_channel_mode_ex(
                SERVER_NAME,
                candidate,
                newer_than=PROCESS_STARTED_AT - 5.0,
                ignore_session_tagged=bool(session_ids),
            )
            if detected != channel_detect.UNKNOWN:
                return detected, reason
        return channel_detect.UNKNOWN, None

    async def detect_and_apply(self) -> None:
        start = time.time()
        detected, reason = channel_detect.UNKNOWN, None
        while time.time() < start + CHANNEL_DETECT_TIMEOUT_SECONDS:
            detected, reason = self.probe_mode()
            if detected != channel_detect.UNKNOWN:
                break
            await anyio.sleep(CHANNEL_DETECT_POLL_SECONDS)
        if detected == channel_detect.SKIPPED:
            self._mode_detail = reason or "harness reported the channel as skipped"
        elif detected == channel_detect.UNKNOWN:
            self._mode_detail = (
                "no channel marker was found for this session (detection timed out)"
            )
        await self.apply_mode(detected)
        # A marker that comes after the timeout is still this run's verdict.
        while (
            detected == channel_detect.UNKNOWN
            and self._mode == "pull"
            and time.time() < start + CHANNEL_LATE_DETECT_SECONDS
        ):
            await anyio.sleep(CHANNEL_LATE_DETECT_POLL_SECONDS)
            detected, reason = self.probe_mode()
            if detected == channel_detect.REGISTERED:
                await self.upgrade_to_push()
            elif detected == channel_detect.SKIPPED:
                self._mode_detail = reason or "harness reported the channel as skipped"

    async def upgrade_to_push(self) -> bool:
        """Switch from pull to push mode. Returns False if not in pull mode."""
        if self._mode != "pull":
            return False
        self._mode_detail = None
        await self.apply_mode(channel_detect.REGISTERED)
        return True

    async def recheck_mode(self) -> bool:
        """In pull mode, look again for this run's marker and switch to push mode if
        it shows the channel as registered. Returns True if the mode changed."""
        if self._mode != "pull":
            return False
        detected, _ = self.probe_mode()
        if detected != channel_detect.REGISTERED:
            return False
        return await self.upgrade_to_push()

    async def drain_buffer(self) -> str:
        """Drain pending notifications for this session and ack what's drained: the
        pull-mode buffer (PR/scheduled notifications never pushed) and the wake-gating
        held buffer (agent messages below the threshold, or that arrived while not a
        channel).

        Bounded: one call drains at most _catchup_max_messages() items /
        _catchup_max_chars() characters (whichever first; oldest-first, buffer then
        held), leaving the rest UNACKED to re-drain on the next call. This keeps a large
        backlog from coming back as one unbounded tool result that could jump the turn's
        context past the window (and re-wedge a freshly-recovered session). Lossless:
        only the drained chunk is acked, exactly per the ack-on-surface invariant."""
        upgraded = await self.recheck_mode()
        max_messages = _catchup_max_messages()
        max_chars = _catchup_max_chars()
        drained = 0
        chars = 0

        def _room_for(content: str) -> bool:
            # Always allow the first item so a single oversized message can't wedge the
            # drain forever; after that, stop at either budget.
            if drained == 0:
                return True
            return drained < max_messages and chars + len(content) <= max_chars

        sections: list[str] = []
        if self._buffer:
            parts: list[str] = []
            for notification_id, payload in list(self._buffer.items()):
                content = payload.get("content", "")
                if not _room_for(content):
                    break  # remaining stay in _buffer, unacked, for the next call
                parts.append(content)
                await self._ack(notification_id)
                self._buffer.pop(notification_id, None)
                drained += 1
                chars += len(content)
            if parts:
                sections.append(
                    "Pending notifications (this session is not a channel, so they "
                    "weren't pushed automatically):\n\n" + "\n\n".join(parts)
                )
        if self._held:
            parts = []
            remaining: list[dict] = []
            stop = False
            for item in self._held:
                content = item.get("content", "")
                if stop or not _room_for(content):
                    stop = (
                        True  # once one won't fit, keep it and everything after (FIFO)
                    )
                    remaining.append(item)
                    continue
                parts.append(content)
                await self._ack(item["id"])
                drained += 1
                chars += len(content)
            self._held = remaining
            if parts:
                sections.append(
                    "Held agent messages (below your wake threshold):\n\n"
                    + "\n\n".join(parts)
                )
        body = "\n\n".join(sections) if sections else "No pending notifications."
        remaining_count = len(self._buffer) + len(self._held)
        if remaining_count and sections:
            body += (
                f"\n\n⚠️ Showing a capped batch — {remaining_count} more pending. "
                "Call catch_up again to continue."
            )
        if self._mode == "pull":
            # Self-announce silent-pull: a session that was MEANT to be a channel
            # (e.g. a respawn that lost --channels from its argv) otherwise looks
            # perfectly healthy from inside — tools work, catch_up works — and the
            # disabled push is only discovered when someone messages it and gets
            # silence. Say so, with the harness's own reason, on every drain.
            note = "⚠️ Live push is disabled — this session is not loaded as a channel"
            if self._mode_detail:
                note += f" ({self._mode_detail})"
            note += (
                ". Nothing will interrupt this session on its own: messages arrive "
                "only when catch_up is called. If this session was supposed to be a "
                "channel, a fresh session with the channel loaded restores push."
            )
            return f"{note}\n\n{body}"
        if upgraded:
            note = (
                "Live push is now active: the harness registered this session as a "
                "channel after startup detection ended. Updates that were waiting "
                "will arrive as <channel> events."
            )
            return f"{note}\n\n{body}"
        return body

    async def request(
        self, payload: dict, *, timeout: float = REQUEST_TIMEOUT_SECONDS
    ) -> dict:
        """Send a request to the server and await its correlated reply."""
        ws = self._ws
        if ws is None:
            raise ConnectionError("server not connected")
        self._req_id += 1
        req_id = self._req_id
        send_stream, receive_stream = anyio.create_memory_object_stream(1)
        self._pending[req_id] = send_stream
        try:
            await ws.send(json.dumps({**payload, "req_id": req_id}))
            with anyio.fail_after(timeout):
                return await receive_stream.receive()
        finally:
            self._pending.pop(req_id, None)

    async def run(self) -> None:
        # A hook's claim ends the grace early: the hold keeps the channel quiet.
        with anyio.move_on_after(STARTUP_GRACE_SECONDS):
            await self._reconnect_now.wait()
        self._reconnect_now = anyio.Event()
        failures = 0
        while True:
            started = time.monotonic()
            self._connected_once = False
            try:
                await self._connect_once()
            except (
                OSError,
                ConnectionClosed,
                InvalidURI,
                WebSocketException,
                ConnectionError,
            ):
                pass  # expected transient network failure — back off and retry
            except Exception as exc:  # noqa: BLE001
                # The reconnect loop is the resilience boundary and must never die.
                # Any other failure is a bad attempt, not a crash. This notably
                # catches the anyio task-group ExceptionGroup raised when a
                # connection task ends on an abnormal close — a group is an
                # Exception so it is caught here, while a shutdown CancelledError /
                # BaseExceptionGroup is not, so cancellation still propagates.
                # Without this, such a group escaped run() and took the whole MCP
                # server down on a server restart (side-door relays vanished until a
                # manual /mcp reconnect).
                print(
                    f"subspace relay: reconnect attempt failed ({exc!r}); retrying",
                    file=sys.stderr,
                )
            finally:
                self._ws = None
                # The registered id stays: the reconnect compares the id it
                # registers with it, and treats a different one as a session
                # change (discard, hold), as the watcher would have.
                # Drop un-acked pending items: the server resends unacked on
                # reconnect and they'll re-buffer, so don't deliver without acking.
                self._pending_debounce = []
                self._debounce_first = None
                self._debounce_last = None
                # Held messages are unacked, so the server re-ships them on reconnect
                # and they'll re-hold; clear to avoid duplicating the re-shipped copies.
                self._held = []
            stable = (
                self._connected_once
                and (time.monotonic() - started) >= RECONNECT_STABLE_SECONDS
            )
            failures = 0 if stable else min(failures + 1, _RECONNECT_MAX_FAILURES)
            # Interruptible backoff: wait the delay, but wake at once if a foreground
            # call nudges us — the long backoff stays for the idle/asleep case while an
            # actively-waiting agent reconnects immediately. anyio Events don't clear, so
            # swap in a fresh one after each wait.
            with anyio.move_on_after(_reconnect_delay(failures)):
                await self._reconnect_now.wait()
            self._reconnect_now = anyio.Event()

    async def _connect_once(self) -> None:
        headers = {"Authorization": f"Bearer {wsproto.token()}"}
        async with connect(
            wsproto.uri(), additional_headers=headers, open_timeout=5
        ) as ws:
            self._connected_once = True
            # Register before `connected` turns true, so a waiting claim sees
            # the registered id and does not register a second time.
            await self._register(ws)
            self._ws = ws
            await self._send_pending_acks()
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._recv_loop, ws, tg)
                tg.start_soon(self._session_watch, ws, tg)

    async def _register(self, ws, session_id: str | None = None) -> None:
        """Register `session_id` (default: the best-known id). A change from an
        earlier session starts a resume hold and discards that session's items;
        an item of that session still on the wire is dropped on receipt by its
        session tag."""
        if session_id is None:
            session_id, _ = session_state.effective_session_id()
        previous = self._registered_session
        if previous is not None and session_id != previous:
            self._discard_session_buffers()
            self._start_hold()
        self._registered_session = session_id
        await ws.send(json.dumps({"type": wsproto.REGISTER, "session_id": session_id}))

    async def _recv_loop(self, ws, tg) -> None:
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    continue
                if msg.get("type") == wsproto.NOTIFY:
                    frame_session = msg.get("session_id")
                    if (
                        frame_session is not None
                        and frame_session != self._registered_session
                    ):
                        continue  # an earlier session's item; unacked under it
                    await self._dispatch_notify(
                        msg.get("id"), msg.get("content", ""), msg.get("meta") or {}
                    )
                else:
                    stream = self._pending.get(msg.get("req_id"))
                    if stream is not None:
                        await stream.send(msg)
        except (ConnectionClosed, WebSocketException, OSError):
            # A dropped connection is the expected end of a session, not an error.
            # Swallow it so it does not escape this task-group child as an anyio
            # ExceptionGroup (which run()'s except tuple would miss, killing the
            # server); the finally still tears down and run() reconnects on the
            # clean return.
            pass
        finally:
            tg.cancel_scope.cancel()  # connection ended; tear down and reconnect

    async def _session_watch(self, ws, tg) -> None:
        """Re-register if the session id changes (e.g. after a resume)."""
        while True:
            await anyio.sleep(SESSION_POLL_SECONDS)
            session_id, _ = session_state.effective_session_id()
            if session_id and session_id != self._registered_session:
                try:
                    await self._register(ws, session_id)
                except ConnectionClosed:
                    tg.cancel_scope.cancel()
                    return


def _ipc_path() -> Path | None:
    """The hook socket, keyed by the Claude Code process both sides descend from."""
    claude_pid = session_state.resolve_claude_pid()
    if claude_pid is None:
        return None
    return session_state.ipc_path(claude_pid)


def _hook_expected() -> bool:
    """True if the SessionStart hook is about to claim or decline: it has not
    written this process's state file yet, or wrote it just now."""
    claude_pid = session_state.resolve_claude_pid()
    if claude_pid is None:
        return False
    age = session_state.state_file_age(claude_pid)
    return age is None or age < START_HOLD_MAX_AGE_SECONDS


async def _read_ipc_line(
    lines: BufferedByteReceiveStream, timeout: float
) -> dict | None:
    """One JSON object from the hook, or None if the hook went away or sent junk."""
    try:
        with anyio.fail_after(timeout):
            raw = await lines.receive_until(b"\n", IPC_LINE_MAX_BYTES)
        message = json.loads(raw)
    except (
        TimeoutError,
        anyio.EndOfStream,
        anyio.IncompleteRead,
        anyio.DelimiterNotFound,
        anyio.ClosedResourceError,
        anyio.BrokenResourceError,
        OSError,
        ValueError,
    ):
        return None
    return message if isinstance(message, dict) else None


DAEMON = DaemonClient()


def _daemon_unreachable_message() -> str:
    return (
        f"The subspace server is not reachable at {wsproto.uri()}. Start it "
        "(for a local server: systemctl --user start subspace-server), or set "
        "NOTIFICATIONS_WS_URL to its address, and try again."
    )


@mcp.tool()
async def catch_up() -> str:
    """Retrieve and acknowledge pending notifications and quiet agent messages.

    Two things land here without interrupting you: agent messages that fell below your
    wake threshold (a plain channel post while you're at 'direct', say), and — if this
    session was NOT launched as a channel — every notification, since none can be pushed
    automatically. Call it periodically (and after long pauses) to drain both.
    """
    return await DAEMON.drain_buffer()


@mcp.tool()
def get_session_id() -> str:
    """Report the Claude Code session ID this relay is attached to.

    Prefers the id recorded by the SessionStart hook (correct across `/resume`),
    falling back to the CLAUDE_CODE_SESSION_ID environment variable.
    """
    session_id, source = session_state.effective_session_id()
    server = "connected" if DAEMON.connected else "disconnected"
    if not session_id:
        return (
            "No session ID available: neither the SessionStart hook's state file "
            f"nor {session_state.SESSION_ID_ENV_VAR} is set. Server: {server}."
        )
    return f"session_id={session_id} (source: {source}); server: {server}; channel: {DAEMON.channel_label}"


@mcp.tool()
async def schedule_test_notification(delay_seconds: int = 300) -> str:
    """Ask the server to schedule a callback notification `delay_seconds` out (default 300).

    The server persists it and delivers it as a channel event reporting this
    session's id, even across closing and reopening the session.
    """
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot schedule: this relay does not yet know its session id."
    if not await DAEMON.wait_connected():
        return _daemon_unreachable_message()
    try:
        reply = await DAEMON.request(
            {
                "type": wsproto.SCHEDULE,
                "session_id": session_id,
                "delay_seconds": max(0, delay_seconds),
                "kind": "scheduled_test",
            }
        )
    except (ConnectionError, TimeoutError):
        return _daemon_unreachable_message()
    if reply.get("type") == wsproto.ERROR:
        return f"Server rejected the schedule request: {reply.get('error')}"
    return (
        f"Scheduled callback {reply.get('id')} for session {session_id} in "
        f"{max(0, delay_seconds)}s. The server will deliver it as a <channel> "
        "event, even across a restart."
    )


@mcp.tool()
async def list_scheduled_notifications() -> str:
    """List this session's pending notifications (queried from the server)."""
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Session id unknown; cannot list scheduled notifications."
    if not await DAEMON.wait_connected():
        return _daemon_unreachable_message()
    try:
        reply = await DAEMON.request({"type": wsproto.LIST, "session_id": session_id})
    except (ConnectionError, TimeoutError):
        return _daemon_unreachable_message()
    items = reply.get("items", [])
    if not items:
        return f"No scheduled notifications for session {session_id}."
    lines = [f"Scheduled notifications for session {session_id}:"]
    for item in items:
        lines.append(
            f"  {item.get('id')}  due_at={item.get('due_at')}  kind={item.get('kind')}"
        )
    return "\n".join(lines)


_PR_REF_RE = re.compile(r"^\s*([^/\s]+)/([^/#\s]+)#(\d+)\s*$")


async def _daemon_request(
    payload: dict, *, timeout: float = REQUEST_TIMEOUT_SECONDS
) -> dict | str:
    """Common guard + request for the server-backed tools (PR and agent directory);
    returns the reply dict or a human-readable error string when unreachable."""
    if not await DAEMON.wait_connected():
        return _daemon_unreachable_message()
    try:
        return await DAEMON.request(payload, timeout=timeout)
    except (ConnectionError, TimeoutError):
        return _daemon_unreachable_message()


@mcp.tool()
async def subscribe_github_pr(pr: str) -> str:
    """Subscribe this session to notifications for a GitHub PR, given as org/repo#number.

    The server polls the PR (checks, reviews, comments, mergeability) and delivers
    updates as <channel> events with enough detail to act on. Subscriptions persist
    in the server; a merged PR auto-unsubscribes you.
    """
    match = _PR_REF_RE.match(pr or "")
    if not match:
        return "Invalid PR reference. Use org/repo#number, e.g. octocat/hello-world#42."
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot subscribe: this relay does not yet know its session id."
    owner, repo, number = match.group(1), match.group(2), int(match.group(3))
    reply = await _daemon_request(
        {
            "type": wsproto.SUBSCRIBE_PR,
            "session_id": session_id,
            "owner": owner,
            "repo": repo,
            "number": number,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not subscribe to {owner}/{repo}#{number}: {reply.get('error')}"
    if reply.get("closed"):
        return f"{reply.get('pr')} is already closed/merged ({reply.get('summary')}); not subscribing."
    return f"Subscribed to {reply.get('pr')}. Current status: {reply.get('summary')}. {DAEMON.delivery_hint()}"


@mcp.tool()
async def unsubscribe_github_pr(pr: str) -> str:
    """Unsubscribe this session from a GitHub PR, given as org/repo#number."""
    match = _PR_REF_RE.match(pr or "")
    if not match:
        return "Invalid PR reference. Use org/repo#number, e.g. octocat/hello-world#42."
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot unsubscribe: this relay does not yet know its session id."
    owner, repo, number = match.group(1), match.group(2), int(match.group(3))
    reply = await _daemon_request(
        {
            "type": wsproto.UNSUBSCRIBE_PR,
            "session_id": session_id,
            "owner": owner,
            "repo": repo,
            "number": number,
        }
    )
    if isinstance(reply, str):
        return reply
    return f"Unsubscribed from {reply.get('pr')}."


@mcp.tool()
async def list_github_pr_subscriptions() -> str:
    """List this session's active GitHub PR subscriptions (queried from the server)."""
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Session id unknown; cannot list PR subscriptions."
    reply = await _daemon_request(
        {"type": wsproto.LIST_PR_SUBSCRIPTIONS, "session_id": session_id}
    )
    if isinstance(reply, str):
        return reply
    items = reply.get("items", [])
    if not items:
        return "No active GitHub PR subscriptions for this session."
    lines = ["GitHub PR subscriptions:"]
    for item in items:
        state = " (merged)" if item.get("merged") else ""
        lines.append(f"  {item.get('pr')}{state}  pending={item.get('pending', 0)}")
    return "\n".join(lines)


_REFRESH_UNSUPPORTED_MESSAGE = (
    "This server build doesn't support on-demand PR refresh yet. The server is "
    "deployed independently of relays (server-first); if it was just updated, retry "
    "shortly, otherwise the running server predates this feature. The next scheduled "
    "poll will still pick up any activity."
)


async def _daemon_request_or_unsupported(payload: dict, unsupported: str) -> dict | str:
    """_daemon_request, but a request timeout OR an 'unknown message type' ERROR becomes
    `unsupported`: an older server either drops a verb it doesn't know (so the request
    times out) or replies ERROR (servers with the terminal-else dispatch branch)."""
    if not await DAEMON.wait_connected():
        return _daemon_unreachable_message()
    try:
        reply = await DAEMON.request(payload)
    except TimeoutError:
        return unsupported
    except ConnectionError:
        return _daemon_unreachable_message()
    if reply.get("type") == wsproto.ERROR and "unknown message type" in str(
        reply.get("error") or ""
    ):
        return unsupported
    return reply


def _refresh_outcome(reply: dict, display: str) -> str:
    """Render a PR_REFRESHED reply for the agent."""
    if reply.get("type") == wsproto.ERROR:
        return (
            f"Could not refresh {display}: {reply.get('error')}. Only a PR some session "
            "is subscribed to can be refreshed."
        )
    count = reply.get("subscribers", 0)
    if not reply.get("polled"):
        return (
            f"Did not poll {display} ({reply.get('reason')}); it has {count} "
            "subscriber(s). Its scheduled polling is unchanged."
        )
    return (
        f"Polling {display} now. New activity reaches its {count} subscriber(s) as "
        "normal PR notifications; the backoff schedule is unchanged unless the poll "
        "finds activity."
    )


@mcp.tool()
async def refresh_github_pr(pr: str) -> str:
    """Poll a subscribed GitHub PR right now instead of waiting for its scheduled poll.

    Use this when you learn out-of-band that something happened on the PR (a review
    landed, CI finished, a comment was posted) and want the update without waiting out
    the backoff. Any session may refresh any tracked PR, so an agent can nudge another
    agent's PR watcher; the results are delivered to that PR's subscribers as the usual
    PR notifications, not returned here. A refresh that finds nothing new does not
    deepen the backoff; one that finds activity resets it, as a scheduled poll would.
    """
    match = _PR_REF_RE.match(pr or "")
    if not match:
        return "Invalid PR reference. Use org/repo#number, e.g. octocat/hello-world#42."
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot refresh: this relay does not yet know its session id."
    owner, repo, number = match.group(1), match.group(2), int(match.group(3))
    reply = await _daemon_request_or_unsupported(
        {
            "type": wsproto.REFRESH_PR,
            "session_id": session_id,
            "owner": owner,
            "repo": repo,
            "number": number,
        },
        _REFRESH_UNSUPPORTED_MESSAGE,
    )
    if isinstance(reply, str):
        return reply
    return _refresh_outcome(reply, f"{owner}/{repo}#{number}")


# --------------------------------------------------------------------------- #
# Forgejo/Gitea PR subscription tools (a parallel REST poller in the server)
# --------------------------------------------------------------------------- #

_FORGEJO_UNSUPPORTED_MESSAGE = (
    "This server build doesn't support Forgejo PR subscriptions yet. The server is "
    "deployed independently of relays and must ship the Forgejo poller first "
    "(server-first). If it was just updated, retry shortly; otherwise the running "
    "server predates this feature."
)

# A Forgejo ref may carry a leading "<alias>:" instance prefix — external:owner/repo#N.
# The alias is lowercase kebab-case (2-64 chars, same rule as the server and agent names),
# which by construction REJECTS a full-host ref: a scheme (https://…), a port (host:8080),
# or a dotted hostname all contain characters the alias pattern forbids, so a host never
# parses as an alias. A bare owner/repo#N has no prefix -> the default instance.
_FORGEJO_ALIAS_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_FORGEJO_ALIAS_MIN = 2
_FORGEJO_ALIAS_MAX = 64

# This-session record of (instance, ref) pairs THIS relay just created a subscription for,
# used only by the version-skew belt: if the tool sent a non-default instance but an old
# server echoed no instance, a match here means we caused a stray default-instance sub and
# should clean it up; a miss means it may be a legitimate pre-existing default sub (a
# mirrored repo) we must NOT destroy. Best-effort, in-memory: a relay restart forgets it,
# which only downgrades a would-be auto-cleanup to the warn path (never a wrong destroy).
_SESSION_CREATED_SUBS: set[tuple[str, str]] = set()


def _parse_forgejo_ref(pr: str) -> tuple[str, str, str, int] | str:
    """Parse a (possibly alias-prefixed) Forgejo ref into (instance, owner, repo, number),
    or return a human-readable error string. instance "" is the default. A "<alias>:"
    prefix is split off first (only a lowercase-kebab alias — never a full host); the rest
    must be a bare owner/repo#N."""
    raw = (pr or "").strip()
    instance = ""
    prefix, sep, rest = raw.partition(":")
    if sep:  # there is a leading "<prefix>:" — it must be a valid alias, not a host
        # A valid alias is lowercase kebab-case AND the remainder is a bare owner/repo#N
        # with no further colon (owner/repo/number never contain ':'). The second check is
        # what rejects a full host:port:… — the first colon would otherwise peel a
        # host-like alias and leave a colon-bearing owner.
        if (
            _FORGEJO_ALIAS_MIN <= len(prefix) <= _FORGEJO_ALIAS_MAX
            and _FORGEJO_ALIAS_RE.match(prefix) is not None
            and ":" not in rest
        ):
            instance, raw = prefix, rest
        else:
            return (
                f"Invalid Forgejo instance prefix {prefix!r}. Use a configured "
                "instance alias (lowercase kebab-case), e.g. external:owner/repo#N. "
                "Full-host references (a URL or hostname) are not accepted — the alias "
                "keeps infrastructure names out of the addressing layer."
            )
    match = _PR_REF_RE.match(raw)
    if not match:
        return (
            "Invalid PR reference. Use owner/repo#number, e.g. octocat/hello-world#42."
        )
    return instance, match.group(1), match.group(2), int(match.group(3))


def _forgejo_ref_display(
    instance: str, owner: str, repo: str, number: int | str
) -> str:
    """Render a Forgejo ref the way tool output tags it: "<alias>:owner/repo#N" for a
    named instance, bare "owner/repo#N" for the default (v1-identical)."""
    bare = f"{owner}/{repo}#{number}"
    return f"{instance}:{bare}" if instance else bare


async def _forgejo_daemon_request(payload: dict) -> dict | str:
    """_daemon_request_or_unsupported with the 'server doesn't support Forgejo yet'
    message, so a pre-Forgejo server fails loudly instead of hanging or reading as
    'unreachable'."""
    return await _daemon_request_or_unsupported(payload, _FORGEJO_UNSUPPORTED_MESSAGE)


def _forgejo_reply_pr(
    reply: dict, instance: str, owner: str, repo: str, number: int
) -> str:
    """The ref to show in a reply, tagged with the instance alias. Prefer the server's
    echoed instance (authoritative) but fall back to what we sent, and use the server's
    ``pr`` field for the bare ref when present."""
    bare = reply.get("pr") or f"{owner}/{repo}#{number}"
    alias = reply.get("instance")
    if alias is None:  # old server: no echo — fall back to the alias we sent
        alias = instance
    return f"{alias}:{bare}" if alias else bare


async def _forgejo_skew_cleanup(
    instance: str, owner: str, repo: str, number: int
) -> str:
    """The version-skew belt (spec): the tool SENT a non-default instance but the server's
    reply carried NO instance echo — an old server that ignored the field and subscribed on
    its DEFAULT instance. Treat it as FAILURE, not success.

    - If THIS session's record says we just created this (instance, ref), the stray sub is
      ours to undo: unsubscribe the default-instance sub the old server actually created.
    - Otherwise warn WITHOUT destroying: a bare default-instance sub to the same
      owner/repo#N may be a legitimate pre-existing subscription (mirrored repos), so blind
      cleanup is forbidden."""
    bare = f"{owner}/{repo}#{number}"
    aliased = f"{instance}:{bare}"
    if (instance, bare) in _SESSION_CREATED_SUBS:
        _SESSION_CREATED_SUBS.discard((instance, bare))
        session_id, _ = session_state.effective_session_id()
        # Undo the stray DEFAULT-instance sub (no instance field) the old server created.
        await _forgejo_daemon_request(
            {
                "type": wsproto.UNSUBSCRIBE_FORGEJO_PR,
                "session_id": session_id,
                "owner": owner,
                "repo": repo,
                "number": number,
            }
        )
        return (
            f"Could not subscribe to {aliased}: this server predates multi-instance "
            "Forgejo support and ignored the instance, subscribing on its DEFAULT "
            "instance instead. That stray default-instance subscription has been undone. "
            "Upgrade the server to subscribe to named instances."
        )
    return (
        f"Could not subscribe to {aliased}: this server predates multi-instance Forgejo "
        f"support and ignored the instance. A default-instance subscription to {bare} may "
        "have been created — check list_forgejo_pr_subscriptions and unsubscribe it if it "
        "is unwanted. Upgrade the server to subscribe to named instances."
    )


@mcp.tool()
async def subscribe_forgejo_pr(pr: str) -> str:
    """Subscribe this session to notifications for a Forgejo/Gitea PR, as owner/repo#number.

    The Forgejo counterpart of subscribe_github_pr, for a self-hosted Forgejo/Gitea
    instance the server is configured to poll (instance URL + token). The server polls
    the PR (reviews, inline + conversation comments, commit status, mergeability, new
    commits) and delivers updates as <channel> events. Subscriptions persist in the
    server; a merged PR auto-unsubscribes you.

    A second (or third) Forgejo instance the server is configured for is addressed by an
    alias prefix: `external:owner/repo#N`. A bare `owner/repo#N` targets the default
    instance. Full-host references are not accepted — use the configured alias.
    """
    parsed = _parse_forgejo_ref(pr)
    if isinstance(parsed, str):
        return parsed
    instance, owner, repo, number = parsed
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot subscribe: this relay does not yet know its session id."
    payload: dict = {
        "type": wsproto.SUBSCRIBE_FORGEJO_PR,
        "session_id": session_id,
        "owner": owner,
        "repo": repo,
        "number": number,
    }
    if instance:
        payload["instance"] = instance
    # Record BEFORE the request so the skew-cleanup can tell "a sub I just caused" from a
    # pre-existing default sub, even if the reply races. Discarded on any non-success path.
    if instance:
        _SESSION_CREATED_SUBS.add((instance, f"{owner}/{repo}#{number}"))
    reply = await _forgejo_daemon_request(payload)
    if isinstance(reply, str):
        _SESSION_CREATED_SUBS.discard((instance, f"{owner}/{repo}#{number}"))
        return reply
    if reply.get("type") == wsproto.ERROR:
        _SESSION_CREATED_SUBS.discard((instance, f"{owner}/{repo}#{number}"))
        display = _forgejo_ref_display(instance, owner, repo, number)
        return f"Could not subscribe to {display}: {reply.get('error')}"
    # Version-skew belt: a non-default instance that came back with NO instance echo is an
    # old server that subscribed on its default. That is a FAILURE, not a success.
    if instance and reply.get("instance") is None:
        return await _forgejo_skew_cleanup(instance, owner, repo, number)
    if reply.get("closed"):
        _SESSION_CREATED_SUBS.discard((instance, f"{owner}/{repo}#{number}"))
        display = _forgejo_reply_pr(reply, instance, owner, repo, number)
        return f"{display} is already closed/merged ({reply.get('summary')}); not subscribing."
    display = _forgejo_reply_pr(reply, instance, owner, repo, number)
    tag = f" (Forgejo: {instance})" if instance else " (Forgejo)"
    # A clean success has no stray to undo — clear the in-flight marker. Otherwise a stale
    # marker could survive to a later re-subscribe of the same named ref (only possible
    # against a downgraded server, which server-first forbids) and make the skew path
    # unsubscribe a legit default-instance sub — the one destroy the mirrored-repo guard prevents.
    _SESSION_CREATED_SUBS.discard((instance, f"{owner}/{repo}#{number}"))
    return f"Subscribed to {display}{tag}. Current status: {reply.get('summary')}. {DAEMON.delivery_hint()}"


@mcp.tool()
async def unsubscribe_forgejo_pr(pr: str) -> str:
    """Unsubscribe this session from a Forgejo/Gitea PR, given as owner/repo#number.

    Address a named instance with an alias prefix (`external:owner/repo#N`); a bare ref
    targets the default instance.
    """
    parsed = _parse_forgejo_ref(pr)
    if isinstance(parsed, str):
        return parsed
    instance, owner, repo, number = parsed
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot unsubscribe: this relay does not yet know its session id."
    payload: dict = {
        "type": wsproto.UNSUBSCRIBE_FORGEJO_PR,
        "session_id": session_id,
        "owner": owner,
        "repo": repo,
        "number": number,
    }
    if instance:
        payload["instance"] = instance
    reply = await _forgejo_daemon_request(payload)
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        display = _forgejo_ref_display(instance, owner, repo, number)
        return f"Could not unsubscribe from {display}: {reply.get('error')}"
    # Drop any this-session created-marker for this ref on an explicit unsubscribe.
    _SESSION_CREATED_SUBS.discard((instance, f"{owner}/{repo}#{number}"))
    display = _forgejo_reply_pr(reply, instance, owner, repo, number)
    return f"Unsubscribed from {display}."


@mcp.tool()
async def list_forgejo_pr_subscriptions() -> str:
    """List this session's active Forgejo/Gitea PR subscriptions (queried from the server)."""
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Session id unknown; cannot list PR subscriptions."
    reply = await _forgejo_daemon_request(
        {"type": wsproto.LIST_FORGEJO_PR_SUBSCRIPTIONS, "session_id": session_id}
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not list Forgejo PR subscriptions: {reply.get('error')}"
    items = reply.get("items", [])
    if not items:
        return "No active Forgejo PR subscriptions for this session."
    lines = ["Forgejo PR subscriptions:"]
    for item in items:
        state = " (merged)" if item.get("merged") else ""
        # A named instance renders "<alias>:owner/repo#N"; the default renders the bare
        # ref exactly as v1 (item["instance"] is "" / absent for the default and github).
        alias = item.get("instance") or ""
        ref = f"{alias}:{item.get('pr')}" if alias else item.get("pr")
        lines.append(f"  {ref}{state}  pending={item.get('pending', 0)}")
    return "\n".join(lines)


@mcp.tool()
async def refresh_forgejo_pr(pr: str) -> str:
    """Poll a subscribed Forgejo/Gitea PR right now instead of waiting for its scheduled poll.

    The Forgejo counterpart of refresh_github_pr: same semantics (any session may
    refresh any tracked PR, results go to subscribers as normal PR notifications, the
    backoff is untouched unless activity is found). Address a named instance with an
    alias prefix (`external:owner/repo#N`); a bare ref targets the default instance.
    """
    parsed = _parse_forgejo_ref(pr)
    if isinstance(parsed, str):
        return parsed
    instance, owner, repo, number = parsed
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot refresh: this relay does not yet know its session id."
    payload: dict = {
        "type": wsproto.REFRESH_FORGEJO_PR,
        "session_id": session_id,
        "owner": owner,
        "repo": repo,
        "number": number,
    }
    if instance:
        payload["instance"] = instance
    reply = await _daemon_request_or_unsupported(payload, _REFRESH_UNSUPPORTED_MESSAGE)
    if isinstance(reply, str):
        return reply
    return _refresh_outcome(reply, _forgejo_ref_display(instance, owner, repo, number))


# --------------------------------------------------------------------------- #
# agent directory tools (Phase A)
# --------------------------------------------------------------------------- #


def _format_last_seen(last_seen: float) -> str:
    """A coarse, human 'how long ago' for an offline agent's last_seen epoch."""
    delta = max(0.0, time.time() - (last_seen or 0.0))
    if delta < 90:
        return f"~{int(delta)}s ago"
    minutes = delta / 60.0
    if minutes < 90:
        return f"~{round(minutes)}m ago"
    hours = minutes / 60.0
    if hours < 48:
        return f"~{round(hours)}h ago"
    return f"~{round(hours / 24.0)}d ago"


def _wedge_annotation(
    name: str, info: dict, threshold: float, now: float
) -> str | None:
    """The 'possibly context-wedged' hint for a CONNECTED recipient that has a message
    pending and whose relay has not acked anything in ``threshold`` seconds — the wedge
    fingerprint (connected + registered, but not surfacing into a runnable context).
    Returns None when it does not apply. Requires last_acked truthy: a never-acked fresh
    session is new/quiet, not wedged."""
    last_acked = info.get("last_acked")
    if not last_acked or now - float(last_acked) < threshold:
        return None
    return (
        f"{name} (connected but no delivery acked in "
        f"{_format_last_seen(float(last_acked))} despite a pending message — possibly "
        "context-wedged; a restart may be needed)"
    )


def _identity_path() -> Path:
    """Where this host persists its last-assumed agent identity (name, session id).
    It survives server restarts, so a session can tell 'another session took my
    name' (SUPERSEDED) from 'the server forgot me'."""
    override = os.environ.get("NOTIFICATIONS_IDENTITY_FILE")
    if override:
        return Path(override)
    return Path.home() / ".claude" / "agent-identity.json"


def _load_identity() -> dict | None:
    try:
        data = json.loads(_identity_path().read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _store_identity(name: str, session_id: str) -> None:
    path = _identity_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(
            json.dumps(
                {
                    "name": name,
                    "session_id": session_id,
                    "updated_at": time.time(),
                }
            )
        )
        tmp.replace(path)
    except OSError as exc:  # bookkeeping must never fail a registration
        print(
            f"subspace relay: could not persist agent identity: {exc}", file=sys.stderr
        )


@mcp.tool()
async def register_agent(
    name: str,
    description: str = "",
    capabilities: str = "",
    default_threshold: str | None = None,
) -> str:
    """Register this session in the shared agent directory under a self-chosen name.

    Binds a short, memorable name to this Claude Code session so other agents can
    find you and message you. Names are lowercase kebab-case, 2-64 chars (letters,
    digits, hyphens; no leading/trailing hyphen) — e.g. 'frontend', 'pr-bot',
    'reviewer-2'.

    Re-registering with the same name updates your profile in place. Registering a
    different name renames you, releasing the old one. A name held by another
    session is rejected, unless that session has been offline for long enough.

    Args:
        name: your directory name (lowercase kebab-case, 2-64 chars).
        description: one line on who you are or what you're working on.
        capabilities: free-form note of what you can help with.
        default_threshold: optional wake threshold ('all', 'direct', or 'urgent').
            Leave unset to keep your current setting (brand-new agents default to
            'direct'). See set_availability for what each level means.
    """
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot register: this relay does not yet know its session id."
    payload: dict = {
        "type": wsproto.REGISTER_AGENT,
        "session_id": session_id,
        "name": name,
        "description": description,
        "capabilities": capabilities,
    }
    # Only send default_threshold when explicitly provided: the registry treats a
    # missing value as "leave unchanged", so we never clobber a threshold the agent
    # set earlier via set_availability just by re-registering.
    if default_threshold is not None:
        payload["default_threshold"] = default_threshold
    reply = await _daemon_request(payload)
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        error = str(reply.get("error") or "")
        stored = _load_identity()
        if (
            "already taken" in error
            and stored is not None
            and stored.get("name") == name
            and stored.get("session_id") == session_id
        ):
            # Our own bookkeeping says this session held the name, but another
            # session holds it now. This file survives server restarts. Tell the
            # agent to stand down rather than retry-register.
            print(
                f"subspace relay: SUPERSEDED — '{name}' is held by another session",
                file=sys.stderr,
            )
            return (
                f"SUPERSEDED: this session previously held '{name}', but the name is "
                "now held by another live session. Do not retry. To hand off "
                "in-flight work, register under a different name and dm "
                f"'{name}'."
            )
        return f"Could not register as '{name}': {error}"
    agent = reply.get("agent") or {}
    threshold = agent.get("default_threshold", "direct")
    reg_name = str(agent.get("name") or name)
    stored = _load_identity()
    if stored is None or (stored.get("name"), stored.get("session_id")) != (
        reg_name,
        session_id,
    ):
        _store_identity(reg_name, session_id)
    return (
        f"Registered as '{reg_name}' (wake threshold: {threshold}). "
        "Other agents can find you with list_agents."
    )


@mcp.tool()
async def unregister_agent() -> str:
    """Leave the shared agent directory, releasing your name for others to claim.

    Removes this session's directory entry. Safe to call even if you were never
    registered. Your scheduled notifications and PR subscriptions are unaffected.
    """
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot unregister: this relay does not yet know its session id."
    reply = await _daemon_request(
        {"type": wsproto.UNREGISTER_AGENT, "session_id": session_id}
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not unregister: {reply.get('error')}"
    agent = reply.get("agent")
    if not agent:
        return "You weren't registered as an agent."
    return (
        f"Unregistered '{agent.get('name')}'. The name is now free for other "
        "agents to claim."
    )


@mcp.tool()
async def list_agents() -> str:
    """List every agent in the shared directory, with live presence.

    Shows each agent's name, whether it is currently connected (or roughly how long
    ago it was last seen), its description, capabilities, and wake threshold. Use this to discover who else is around before coordinating.
    """
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {"type": wsproto.LIST_AGENTS, "session_id": session_id}
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not list agents: {reply.get('error')}"
    agents = reply.get("agents", [])
    if not agents:
        return "Registered agents: (none registered)"
    lines = ["Registered agents:"]
    for agent in agents:
        if agent.get("connected"):
            presence = "connected"
        else:
            presence = (
                f"offline (last seen {_format_last_seen(agent.get('last_seen', 0.0))})"
            )
        lines.append(f"- {agent.get('name', '?')} — {presence}")
        if agent.get("description"):
            lines.append(f"    description: {agent['description']}")
        if agent.get("capabilities"):
            lines.append(f"    capabilities: {agent['capabilities']}")
        lines.append(f"    wake threshold: {agent.get('default_threshold', 'direct')}")
    return "\n".join(lines)


@mcp.tool()
async def set_availability(default_threshold: str) -> str:
    """Set your wake threshold — how insistent a message must be to interrupt you.

    This is the 'do not disturb' knob for (Phase B) agent messaging. It is stored on
    your directory entry now; the message-gating that consumes it lands with
    messaging. You must be registered (see register_agent) first. Levels, from least
    to most strict:

        all     wake on every message to any channel you're in.
        direct  (default) wake only on messages that mention you, direct requests to
                you, or an @here addressed to a channel you're in.
        urgent  wake only on an @here.

    Args:
        default_threshold: one of 'all', 'direct', or 'urgent'.
    """
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return "Cannot set availability: this relay does not yet know its session id."
    reply = await _daemon_request(
        {
            "type": wsproto.SET_AVAILABILITY,
            "session_id": session_id,
            "default_threshold": default_threshold,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not set availability: {reply.get('error')}"
    agent = reply.get("agent") or {}
    return (
        f"Wake threshold set to '{agent.get('default_threshold', default_threshold)}'. "
        "Other agents see this in list_agents."
    )


# --------------------------------------------------------------------------- #
# agent messaging tools (Phase B): channels + DMs
# --------------------------------------------------------------------------- #
#
# Attention model (shared vocabulary across these tools):
#   intent    fyi (default, terminal — no reply expected) | question | request | reply.
#             Intent is display/reply semantics only; it does NOT change how loud a
#             message is.
#   severity  low | normal (default) | high. severity="high" is an @here: it wakes
#             every member regardless of their threshold.
#   mentions  a list of agent names — an @someone. A mentioned recipient is "addressed",
#             so the message wakes them at the 'direct' threshold even without @here.
#   threshold each recipient's bar (set via set_availability or per-context set_threshold):
#             all (every message) < direct (mentions / DMs / @here) < urgent (@here only).
# A message a recipient doesn't clear is held silently for their catch_up.


def _require_session(action: str) -> str | None:
    session_id, _ = session_state.effective_session_id()
    if not session_id:
        return f"Cannot {action}: this relay does not yet know its session id."
    return None


def _render_history(history: list[dict]) -> list[str]:
    lines: list[str] = []
    for msg in history:
        sender = msg.get("sender", "?")
        body = msg.get("body", "")
        marker = " [@here]" if msg.get("severity") == "high" else ""
        ordinal = msg.get("ordinal")
        handle = f"#{ordinal} " if ordinal else ""  # match the live #N rendering
        lines.append(f"    {handle}{sender}: {body}{marker}")
    return lines


@mcp.tool()
async def join_channel(
    channel: str, threshold: str | None = None, topic: str | None = None
) -> str:
    """Join (creating if needed) a shared channel so its messages reach you.

    Channels are communal: there is no owner, joining or posting creates one, and the
    member set is just whoever joined. Names are lowercase kebab-case (e.g. 'backend',
    'release-2'). Joining replays no history as notifications, but the reply shows the
    recent scrollback so you can catch up.

    Args:
        channel: the channel name (lowercase kebab-case).
        threshold: optional per-channel wake threshold ('all', 'direct', 'urgent') that
            overrides your global default just for this channel.
        topic: optional one-line topic to set if the channel is new/empty.
    """
    if err := _require_session("join channel"):
        return err
    try:
        messaging.validate_channel_name(channel or "")
        if threshold is not None:
            messaging.validate_threshold(threshold)
    except messaging.MessagingError as exc:
        return f"Could not join '{channel}': {exc}"
    session_id, _ = session_state.effective_session_id()
    payload: dict = {
        "type": wsproto.JOIN_CHANNEL,
        "session_id": session_id,
        "channel": channel,
    }
    if threshold is not None:
        payload["threshold"] = threshold
    if topic is not None:
        payload["topic"] = topic
    reply = await _daemon_request(payload)
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not join '{channel}': {reply.get('error')}"
    members = reply.get("members", [])
    lines = [
        (
            f"Joined #{reply.get('channel', channel)} ({len(members)} member(s): "
            f"{', '.join(members) if members else 'just you'})."
        )
    ]
    if reply.get("topic"):
        lines.append(f"Topic: {reply['topic']}")
    history = reply.get("history") or []
    if history:
        lines.append("Recent messages:")
        lines.extend(_render_history(history))
    return "\n".join(lines)


@mcp.tool()
async def leave_channel(channel: str) -> str:
    """Leave a channel you previously joined; its messages stop reaching you."""
    if err := _require_session("leave channel"):
        return err
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {"type": wsproto.LEAVE_CHANNEL, "session_id": session_id, "channel": channel}
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not leave '{channel}': {reply.get('error')}"
    return f"Left #{channel}."


@mcp.tool()
async def post(
    channel: str,
    body: str,
    intent: str = "fyi",
    severity: str = "normal",
    mentions: list[str] | None = None,
) -> str:
    """Post a message to a channel (auto-joining it if you haven't already).

    Args:
        channel: the channel name (lowercase kebab-case). A name nobody else has joined
            means only you will see the post — the reply's member count flags that.
        body: the message text.
        intent: 'fyi' (default, terminal — no reply expected), 'question', 'request',
            or 'reply'. Intent conveys reply/display semantics, not loudness.
        severity: 'low', 'normal' (default), or 'high'. 'high' is an @here — it wakes
            every member no matter their threshold.
        mentions: a list of agent names to @-mention; each mentioned member is woken at
            the 'direct' threshold even without an @here.
    """
    if err := _require_session("post"):
        return err
    try:
        messaging.validate_channel_name(channel or "")
        messaging.validate_intent(intent)
        messaging.validate_severity(severity)
    except messaging.MessagingError as exc:
        return f"Could not post to '{channel}': {exc}"
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {
            "type": wsproto.POST,
            "session_id": session_id,
            "channel": channel,
            "body": body,
            "intent": intent,
            "severity": severity,
            "mentions": list(mentions or []),
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not post to '{channel}': {reply.get('error')}"
    ordinal = reply.get("ordinal")
    handle = f" (#{ordinal})" if ordinal else ""
    members = reply.get("members", 1)
    if members <= 1:
        return (
            f"Posted to #{channel}{handle}, but you're the only member — no one else "
            "will see it. Did you mean a different channel name?"
        )
    return f"Posted to #{channel}{handle} — {members} members will see it."


@mcp.tool()
async def dm(
    to: list[str], body: str, intent: str = "request", severity: str = "normal"
) -> str:
    """Send a direct message to one or more agents by name.

    A DM is a private thread keyed by its participant set, so messaging the same people
    again reuses one thread. Every recipient is 'addressed', so a DM wakes them at the
    'direct' threshold (and an @here — severity='high' — overrides even 'urgent').

    Args:
        to: a list of agent names to message (see list_agents for who's around).
        body: the message text.
        intent: 'request' (default), 'question', 'reply', or 'fyi' (terminal — no reply
            expected).
        severity: 'low', 'normal' (default), or 'high' (@here).
    """
    if err := _require_session("send DM"):
        return err
    try:
        messaging.validate_intent(intent)
        messaging.validate_severity(severity)
    except messaging.MessagingError as exc:
        return f"Could not send DM: {exc}"
    recipients = [to] if isinstance(to, str) else list(to or [])
    if not recipients:
        return "Could not send DM: name at least one recipient."
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {
            "type": wsproto.DM,
            "session_id": session_id,
            "to": recipients,
            "body": body,
            "intent": intent,
            "severity": severity,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not send DM: {reply.get('error')}"
    ordinal = reply.get("ordinal")
    handle = f" (#{ordinal})" if ordinal else ""
    return (
        f"Sent DM to {', '.join(recipients)}{handle} — "
        f"{reply.get('members', 0)} in the thread."
    )


@mcp.tool()
async def set_threshold(context: str, threshold: str) -> str:
    """Set a per-channel/DM wake threshold, overriding your global default there.

    Args:
        context: the topic key from list_subscriptions (e.g. 'chan:backend').
        threshold: 'all' (every message), 'direct' (mentions / DMs / @here), or
            'urgent' (@here only).
    """
    if err := _require_session("set threshold"):
        return err
    try:
        messaging.validate_threshold(threshold or "")
    except messaging.MessagingError as exc:
        return f"Could not set threshold: {exc}"
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {
            "type": wsproto.SET_THRESHOLD,
            "session_id": session_id,
            "context": context,
            "threshold": threshold,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not set threshold: {reply.get('error')}"
    return f"Wake threshold for {context} set to '{threshold}'."


@mcp.tool()
async def set_channel_topic(channel: str, topic: str) -> str:
    """Set a channel's one-line topic (the 'what this channel is for' description)."""
    if err := _require_session("set channel topic"):
        return err
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {
            "type": wsproto.SET_CHANNEL_TOPIC,
            "session_id": session_id,
            "channel": channel,
            "topic": topic,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not set topic for '{channel}': {reply.get('error')}"
    return f"Topic for #{channel} set to: {topic}"


@mcp.tool()
async def list_channels() -> str:
    """List every channel the server knows about, with topic and member count."""
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {"type": wsproto.LIST_CHANNELS, "session_id": session_id}
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not list channels: {reply.get('error')}"
    channels = reply.get("channels", [])
    if not channels:
        return "No channels exist yet. Use join_channel to create one."
    lines = ["Channels:"]
    for ch in sorted(channels, key=lambda c: c.get("name", "")):
        topic = f" — {ch['topic']}" if ch.get("topic") else ""
        lines.append(
            f"- #{ch.get('name', '?')} ({ch.get('members', 0)} member(s)){topic}"
        )
    return "\n".join(lines)


@mcp.tool()
async def list_subscriptions() -> str:
    """List the channels and DMs you're in, with your effective wake threshold for each."""
    if err := _require_session("list subscriptions"):
        return err
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {"type": wsproto.LIST_SUBSCRIPTIONS, "session_id": session_id}
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not list subscriptions: {reply.get('error')}"
    subscriptions = reply.get("subscriptions", [])
    if not subscriptions:
        return "You're not in any channels or DMs yet."
    lines = ["Your channels and DMs:"]
    for sub in sorted(subscriptions, key=lambda s: s.get("context", "")):
        lines.append(
            f"- {sub.get('context', '?')} ({sub.get('kind', '?')}) — "
            f"wake threshold: {sub.get('threshold', 'direct')}"
        )
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# receipts + reactions (Phase C)
# --------------------------------------------------------------------------- #


@mcp.tool()
async def react(message_id: str, reaction: str) -> str:
    """React to a message with a short, TERMINAL acknowledgment.

    A reaction acknowledges a message without asking for a reply, and NEVER wakes the
    recipient — it rides at 'ambient', so the author only sees it on their next turn,
    via catch_up, or in message_status. Prefer it over a courtesy "got it" post: it is
    the sanctioned way to say "received" without triggering a reply or an interruption.

    The reaction body is free-form but short — a single emoji or a terse token, no
    newlines (e.g. '👍', 'ack', 'done').

    Args:
        message_id: which message to react to. Pass the short '#N' handle shown on any
            message (e.g. '#147' — or just '147'), or the full id ('msg:chan:room:3').
        reaction: a short, single-line reaction body.
    """
    if err := _require_session("react"):
        return err
    try:
        messaging.validate_reaction(reaction or "")
    except messaging.MessagingError as exc:
        return f"Could not react: {exc}"
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {
            "type": wsproto.REACT,
            "session_id": session_id,
            "target": message_id,
            "reaction": reaction,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not react to {message_id}: {reply.get('error')}"
    return f'Reacted "{reaction}" to {message_id} (terminal — no reply expected).'


@mcp.tool()
async def message_status(message_id: str) -> str:
    """Show who has received a message, who is still pending, and any reactions to it.

    'Delivered' means the message reached that agent's context — it was surfaced, or
    drained via catch_up — NOT that they read, understood, or acted on it. A recipient
    holding the message below their wake threshold reads as 'pending' until they
    catch_up. The message's own author is never counted (it pre-acks its own post).

    Args:
        message_id: which message to inspect. Pass the short '#N' handle shown on any
            message (e.g. '#147' — or just '147'), or the full id ('msg:chan:room:3').
    """
    if err := _require_session("check message status"):
        return err
    session_id, _ = session_state.effective_session_id()
    reply = await _daemon_request(
        {
            "type": wsproto.MESSAGE_STATUS,
            "session_id": session_id,
            "target": message_id,
        }
    )
    if isinstance(reply, str):
        return reply
    if reply.get("type") == wsproto.ERROR:
        return f"Could not get status for {message_id}: {reply.get('error')}"
    delivered = reply.get("delivered", [])
    pending = reply.get("pending", [])
    reactions = reply.get("reactions", [])
    recipients = reply.get("recipients", {})

    def _annotate(name: str, pending: bool = False) -> str:
        """Append liveness so 'delivered' can't be misread as 'processed': a
        disconnected or long-idle recipient may have the message sitting in a
        context nobody is running, and a *connected* recipient that has stopped acking
        while a message is pending may be context-wedged."""
        info = recipients.get(name)
        if not isinstance(info, dict):
            return name  # older server: no annotation available
        if info.get("connected"):
            # A connected recipient with THIS message pending, whose relay hasn't acked
            # anything in a while, is the context-wedge fingerprint (connected +
            # registered, but not surfacing into a runnable context).
            if pending:
                hint = _wedge_annotation(name, info, _wedge_hint_seconds(), time.time())
                if hint:
                    return hint
            return name
        last_seen = info.get("last_seen")
        if last_seen:
            return f"{name} (offline, last seen {_format_last_seen(float(last_seen))})"
        return f"{name} (offline)"

    total = len(delivered) + len(pending)
    parts = [f"Delivered to {len(delivered)} of {total}"]
    if delivered:
        parts[0] += ": " + ", ".join(_annotate(n) for n in delivered)
    if pending:
        parts.append(
            "pending: " + ", ".join(_annotate(n, pending=True) for n in pending)
        )
    if reactions:
        parts.append(
            "reactions: "
            + ", ".join(f"{r.get('reaction')} {r.get('by')}" for r in reactions)
        )
    return (
        f"{message_id} — {'; '.join(parts)}. (Delivered means the message reached the "
        "agent's context, not that it was read or acted on.)"
    )


async def _serve() -> None:
    server = mcp._mcp_server
    init_options = server.create_initialization_options(
        notification_options=NotificationOptions(tools_changed=True),
        experimental_capabilities={"claude/channel": {}},
    )
    async with stdio_server() as (read_stream, write_stream):
        DAEMON.attach_write_stream(write_stream)
        async with anyio.create_task_group() as tg:
            tg.start_soon(DAEMON.serve_ipc)  # the SessionStart hook's claim
            tg.start_soon(DAEMON.run)
            tg.start_soon(DAEMON.detect_and_apply)  # decide push vs catch_up
            tg.start_soon(DAEMON.debounce_loop)  # coalesce push-mode bursts
            await server.run(read_stream, write_stream, init_options)
            tg.cancel_scope.cancel()  # transport closed: stop the server client


if __name__ == "__main__":
    anyio.run(_serve)
