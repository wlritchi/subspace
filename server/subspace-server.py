#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""
subspace server

A persistent, single-instance WebSocket server that owns notification state for
Claude sessions. Clients (the per-session stdio MCP relay, or the remote-agent
gateway) authenticate with a credential, which selects an account, attach to a
session in that account, and exchange notifications. See
../docs/specs/2026-10-10-identity.md.

It provides two capabilities:
  - scheduled one-shot callbacks (proof-of-concept; ../lib/scheduler.py)
  - GitHub PR monitoring (../lib/pr_monitor.py): polls subscribed PRs for checks,
    reviews, comments and mergeability, and pushes rich notifications. Polling
    cadence backs off but is capped during business hours (../lib/pr_schedule.py).
    Updates are cached (content-addressed event ids) and each subscriber tracks
    the ids it has acked; new subscribers join polling without replay. Polling
    suspends while no
    subscribed session is connected and resumes on reconnect. A merged PR
    auto-unsubscribes everyone and stops polling.

Run manually or via systemd --user (see ./README.md). The relay never spawns it.

Config (env):  SUBSPACE_WS_HOST (default 127.0.0.1)
               SUBSPACE_WS_PORT (default 8137)
               SUBSPACE_DATA_DIR (default $XDG_DATA_HOME/subspace)
               SUBSPACE_TOKEN (default: auto-created <DATA_DIR>/token)
               SUBSPACE_DEFAULT_ACCOUNT (default "default")
               SUBSPACE_ACCOUNTS_FILE (default <DATA_DIR>/accounts.toml)
               SUBSPACE_PR_WARM_TTL_SECONDS (default 1800)
               SUBSPACE_PR_CHECK_SUMMARY_THRESHOLD (default 5; 0 disables)
               SUBSPACE_HANDLE_IDLE_SECONDS (default 900)
               SUBSPACE_SESSION_TTL_DAYS (default 30)
               SUBSPACE_CHANNEL_TTL_SECONDS (default 86400)
               SUBSPACE_CHANNEL_HISTORY (default 20)
               GITHUB_TOKEN, GITHUB_API_URL (default https://api.github.com)
"""

# /// script
# requires-python = ">=3.12"
# dependencies = ["websockets", "httpx", "tzdata", "subspace-protocol"]
#
# [tool.uv.sources]
# subspace-protocol = { path = "../protocol", editable = true }
# ///

import asyncio
import itertools
import json
import logging
import os
import random
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import subspace_protocol as proto
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import accounts  # noqa: E402
import forgejo_client  # noqa: E402
import github_client  # noqa: E402
import messaging  # noqa: E402
import message_topic  # noqa: E402
import migrate_identity  # noqa: E402
import pr_errors  # noqa: E402
import pr_monitor  # noqa: E402
import pr_schedule  # noqa: E402
import scheduler  # noqa: E402
import sessions  # noqa: E402
import settings  # noqa: E402
import storage  # noqa: E402

# A scheduled callback this far past due was held while nothing was connected.
RECOVERED_THRESHOLD_SECONDS = 30
# consecutive-no-update level that forces ~max (8h) backoff after an auth failure.
AUTH_BACKOFF_LEVEL = 14
# How long an unsubscribed (subscriber-less, non-terminal) tracker is kept warm
# before the reaper deletes it, so a quick re-subscribe reuses its cached state.
WARM_TTL_DEFAULT_SECONDS = 1800.0
# How long a handle stays with a session that has no connection before another
# session in the account can register it without takeover.
HANDLE_IDLE_DEFAULT_SECONDS = 900.0
# How long a session with no connection is kept before the reaper removes it.
SESSION_TTL_DEFAULT_DAYS = 30.0
# The largest message body that post and dm accept, in UTF-8 bytes.
MAX_BODY_BYTES = 16 * 1024
# How often to check the accounts file for changes and credentials for expiry.
ACCOUNTS_CHECK_SECONDS = 5.0
# How long a memberless+silent message topic (channel or DM) is kept before the
# reaper deletes it, and how many recent messages a join reply renders as scrollback.
CHANNEL_TTL_DEFAULT_SECONDS = 86400.0
CHANNEL_HISTORY_DEFAULT = 20
# How much of a reacted-to message's body to echo back when rendering a reaction,
# so `agent-b reacted "👍" to agent-a's "<snippet>"` stays a single readable line.
REACTION_SNIPPET_MAX = 40


def _warm_ttl_seconds() -> float:
    raw = os.environ.get("SUBSPACE_PR_WARM_TTL_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return WARM_TTL_DEFAULT_SECONDS


def _handle_idle_seconds() -> float:
    raw = os.environ.get("SUBSPACE_HANDLE_IDLE_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return HANDLE_IDLE_DEFAULT_SECONDS


def _session_ttl_seconds() -> float:
    raw = os.environ.get("SUBSPACE_SESSION_TTL_DAYS")
    if raw:
        try:
            return float(raw) * 86400.0
        except ValueError:
            pass
    return SESSION_TTL_DEFAULT_DAYS * 86400.0


def _accounts_check_seconds() -> float:
    raw = os.environ.get("SUBSPACE_ACCOUNTS_CHECK_SECONDS")
    if raw:
        try:
            return max(0.1, float(raw))
        except ValueError:
            pass
    return ACCOUNTS_CHECK_SECONDS


def _channel_ttl_seconds() -> float:
    raw = os.environ.get("SUBSPACE_CHANNEL_TTL_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return CHANNEL_TTL_DEFAULT_SECONDS


def _channel_history_n() -> int:
    raw = os.environ.get("SUBSPACE_CHANNEL_HISTORY")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return CHANNEL_HISTORY_DEFAULT


# Session key -> the connections attached to it, oldest first.
CONNECTIONS: dict[str, list["Connection"]] = {}
# Every open connection, attached or not, for the credential checks.
ALL_CONNECTIONS: set["Connection"] = set()
# "owner/repo#number" -> PRTracker
TRACKERS: dict[str, pr_monitor.PRTracker] = {}
# The rest of the state loads in main(), after the identity migration.
ACCOUNTS: accounts.AccountStore
SESSIONS: sessions.SessionRegistry
# Message topics: topic key ("chan:<name>" / "dm:<...>") -> MessageTopic,
# persisted under <data_dir>/msg/<safe_key>/.
TOPICS: dict[str, message_topic.MessageTopic] = {}
# Session key -> (tokens, last refill time) for the post rate limit.
POST_BUCKETS: dict[str, tuple[float, float]] = {}


def _next_ordinal_path() -> Path:
    """Where the global message-handle counter is persisted: a single int file under
    the message root, so it survives topic reaps (which delete per-topic dirs)."""
    return message_topic._msg_root(settings.data_dir()) / ".next_ordinal"


def _load_next_ordinal() -> int:
    """Read the persisted next-ordinal counter, tolerantly (default 0 when absent)."""
    try:
        return int(_next_ordinal_path().read_text().strip())
    except (OSError, ValueError):
        return 0


def _init_next_ordinal() -> int:
    """Starting value for the global handle counter: strictly past both the persisted
    counter and the highest ordinal across loaded messages, so handles stay unique and
    strictly monotonic across restarts and topic reaps. Ordinals start at 1."""
    max_seen = max(
        (m.ordinal for topic in TOPICS.values() for m in topic.messages),
        default=0,
    )
    return max(_load_next_ordinal(), 1 + max_seen)


# Server-global, monotonic message-handle counter. The lib stays counter-agnostic;
# the server allocates an ordinal per post and passes it into MessageTopic.post, so
# every channel/DM/reaction message gets a stable, globally unique `#N` handle
# agents can pass back to react / message_status. Set in main().
_NEXT_ORDINAL: int = 0


def _alloc_ordinal() -> int:
    """Return the current global ordinal, then advance and persist it. Persisting the
    advanced value keeps handles strictly monotonic across restarts and reaps."""
    global _NEXT_ORDINAL
    value = _NEXT_ORDINAL
    _NEXT_ORDINAL += 1
    storage.atomic_write(_next_ordinal_path(), str(_NEXT_ORDINAL))
    return value


GH: github_client.GitHubClient | None = None
# The DEFAULT Forgejo/Gitea client, built in main() only when FORGEJO_API_URL is
# configured; None means this server does not serve default-instance Forgejo PR
# subscriptions.
FJ: forgejo_client.ForgejoClient | None = None
# NAMED Forgejo instances: alias (lowercase-kebab) -> ForgejoClient, built in main() from
# FORGEJO_<ALIAS>_API_URL / FORGEJO_<ALIAS>_TOKEN env pairs. Empty when only the default
# instance is configured (the v1 shape), so the default path stays byte-identical.
FJ_INSTANCES: dict[str, forgejo_client.ForgejoClient] = {}


def _forgejo_client(instance: str):
    """The Forgejo client for an alias, or None if that alias isn't configured here.
    instance "" (or None) is the DEFAULT instance (FJ)."""
    if not instance:
        return FJ if (FJ is not None and FJ.configured) else None
    return FJ_INSTANCES.get(instance)


def _forgejo_aliases() -> list[str]:
    """Configured named-instance aliases, sorted (names only — never URLs), for the
    'unknown Forgejo instance' error and diagnostics."""
    return sorted(FJ_INSTANCES)


def _pr_client(provider: str, instance: str = ""):
    """The PR client for a (provider, instance), or None if it isn't wired here. For
    github the instance is ignored (no multi-instance dimension in v1); for forgejo,
    instance "" is the default client and a named alias resolves to its registered
    client (or None when the alias isn't configured)."""
    if provider == "forgejo":
        return _forgejo_client(instance)
    return GH


def _pr_clients() -> dict:
    """The clients map for load_trackers, covering the default + every configured named
    instance. The DEFAULT instance keys on the bare provider string (byte-identical to
    the v1 {provider: client} shape); a NAMED forgejo instance keys on a
    ("forgejo", alias) tuple. Only configured (provider, instance)s are included, so a
    tracker whose instance is unconfigured is skipped rather than loaded with no client."""
    clients: dict = {"github": GH}
    if FJ is not None and FJ.configured:
        clients["forgejo"] = FJ
    for alias, client in FJ_INSTANCES.items():
        clients[("forgejo", alias)] = client
    return clients


# A named Forgejo instance is one env pair: FORGEJO_<ALIAS>_API_URL + FORGEJO_<ALIAS>_TOKEN.
# The alias is the env segment between the FORGEJO_ prefix and the _API_URL suffix,
# lowercased with '_' -> '-' to recover the ref/tool alias (e.g. FORGEJO_EXTERNAL_API_URL
# -> "external", FORGEJO_SELF_HOSTED_API_URL -> "self-hosted").
_FORGEJO_ALIAS_URL_RE = re.compile(r"^FORGEJO_(?P<alias>[A-Z0-9][A-Z0-9_]*)_API_URL$")
# The alias must round-trip to lowercase kebab-case (same rule as agent/channel names and
# the relay's ref parser), so infrastructure names stay clean and a full host can never be
# mistaken for an alias.
_FORGEJO_ALIAS_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")


def _forgejo_token_env(instance: str) -> str:
    """The env var holding a Forgejo instance's token: FORGEJO_TOKEN for the default, and
    FORGEJO_<ALIAS>_TOKEN (alias uppercased, '-' -> '_') for a named instance. Used to
    name the exact var in the auth-error message so an operator knows which secret to fix."""
    if not instance:
        return "FORGEJO_TOKEN"
    return f"FORGEJO_{instance.upper().replace('-', '_')}_TOKEN"


def _build_forgejo_instances(env: dict[str, str], default_url: str) -> dict:
    """Parse FORGEJO_<ALIAS>_API_URL / FORGEJO_<ALIAS>_TOKEN env pairs into named
    ForgejoClients. ``default_url`` is the default instance's NORMALIZED base_url ("" when
    the default instance is unconfigured), used for the canonicalization check.

    Rules (per spec):
      - The alias env segment must recover a lowercase kebab-case alias; a segment that
        doesn't (e.g. a stray FORGEJO_API_URL — no alias segment — or an all-numeric one)
        is ignored, not turned into an instance.
      - CANONICALIZATION: an alias whose normalized URL equals the default instance's URL
        would grow a SECOND tracker for one PR. That is a configuration error — it is
        logged loudly and the alias is SKIPPED (never created).
      - An alias missing its URL is skipped; a token is optional (public repos), matching
        the default client's contract.
    """
    instances: dict = {}
    # Every accepted instance's normalized URL -> a label, for the collision message.
    # Seeded with the default so an alias == default is caught; grows as aliases are
    # accepted so an alias == an EARLIER alias is caught too (two aliases must not
    # double-poll one PR — same duplicate-tracker class canonicalization exists to prevent).
    seen_urls: dict[str, str] = (
        {default_url: "the default instance (FORGEJO_API_URL)"} if default_url else {}
    )
    for name, value in sorted(env.items()):
        m = _FORGEJO_ALIAS_URL_RE.match(name)
        if not m or not (value or "").strip():
            continue
        alias = m.group("alias").lower().replace("_", "-")
        if _FORGEJO_ALIAS_RE.match(alias) is None:
            print(
                f"subspace: ignoring {name} — alias {alias!r} is not valid "
                "lowercase kebab-case",
                file=sys.stderr,
            )
            continue
        token = env.get(_forgejo_token_env(alias))
        client = forgejo_client.ForgejoClient(base_url=value, token=token)
        if not client.configured:  # normalized to empty; nothing to poll
            continue
        # Canonicalization: an alias whose normalized URL is already served — by the
        # default instance OR an earlier-accepted alias — must NOT create a duplicate:
        # one PR would resolve to two trackers double-polling it. Refuse and tell the
        # operator which pair to drop.
        if client.base_url in seen_urls:
            print(
                f"subspace: CONFIG ERROR — Forgejo instance {alias!r} "
                f"({name}) resolves to the same URL as {seen_urls[client.base_url]}; "
                f"refusing to create a duplicate instance for one PR. Drop the {name} / "
                f"{_forgejo_token_env(alias)} pair.",
                file=sys.stderr,
            )
            continue
        seen_urls[client.base_url] = f"instance {alias!r} ({name})"
        instances[alias] = client
    return instances


# The master token. It always gives access to the default account. Set in main().
TOKEN = ""


def _live(key: str) -> bool:
    """True if at least one connection is attached to the session."""
    return bool(CONNECTIONS.get(key))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# scheduled one-shot callbacks (proof-of-concept feature)
# --------------------------------------------------------------------------- #


def _build_callback_notification(
    entry: dict, key: str, now: float
) -> tuple[str, dict[str, str]]:
    due = float(entry.get("due_at", now))
    created_for = sessions.split_key(str(entry.get("session_id", "")))[1] or "?"
    late = int(now - due)
    recovered = (
        " (recovered after restart)" if late >= RECOVERED_THRESHOLD_SECONDS else ""
    )
    content = (
        f"Scheduled notification fired{recovered}. callback_id={entry.get('id')} "
        f"session_id={sessions.split_key(key)[1]} (scheduled for {created_for}) "
        f"due_at={_iso(due)} now={_iso(now)} late_by={late}s"
    )
    meta = {
        "severity": "info",
        "kind": str(entry.get("kind", "scheduled")),
        "callback_id": str(entry.get("id", "")),
    }
    return content, meta


# --------------------------------------------------------------------------- #
# connection handling + delivery
# --------------------------------------------------------------------------- #


class Connection:
    def __init__(self, websocket, account_id: str, credential_id: str) -> None:
        self.ws = websocket
        self.account_id: str = account_id
        self.credential_id: str = credential_id
        # The session key this connection is attached to, or None.
        self.session_key: str | None = None
        self.inflight: set[str] = set()  # notification ids sent, awaiting ack
        self.wake = asyncio.Event()  # set to nudge the dispatch loop to deliver now
        self.dispatch_task: asyncio.Task | None = None

    @property
    def account(self) -> accounts.Account | None:
        return ACCOUNTS.get(self.account_id)


async def _safe_send(conn: Connection, payload: dict) -> bool:
    try:
        await conn.ws.send(json.dumps(payload))
        return True
    except ConnectionClosed:
        return False


async def _send_notify(conn: Connection, payload: dict) -> bool:
    """Send a NOTIFY frame. While more than one connection is attached to the
    session, the meta carries the count, so that the client can warn that
    delivery is split."""
    count = len(CONNECTIONS.get(conn.session_key or "", ()))
    if count > 1:
        payload = {**payload, "meta": {**payload["meta"], "connections": count}}
    return await _safe_send(conn, payload)


def _wake(key: str | None) -> None:
    """Nudge the dispatch loops of a session's connections to deliver newly
    available notifications. Setting an asyncio.Event is sync and safe anywhere."""
    if not key:
        return
    for conn in CONNECTIONS.get(key, ()):
        conn.wake.set()


def _wake_subscribers(tracker: pr_monitor.PRTracker) -> None:
    """Wake every connected subscriber of a tracker after new events are appended."""
    for key in tracker.subscribers:
        _wake(key)


def _session_key_for(conn: Connection, msg: dict) -> str | None:
    """The session a request acts for: the attached session, else the frame's
    ``session_id`` in the connection's account. None if there is neither."""
    if conn.session_key:
        return conn.session_key
    session_id = msg.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return None
    try:
        sessions.validate_session_id(session_id)
    except sessions.SessionError:
        return None
    return sessions.session_key(conn.account_id, session_id)


_NOTICE_SEQ = itertools.count(1)


async def _announce_connections(key: str) -> None:
    """Tell every connection of a session how many connections it has, after the
    count goes above one or back to one."""
    conns = list(CONNECTIONS.get(key, ()))
    count = len(conns)
    if count > 1:
        content = (
            f"⚠️ {count} connections are attached to this session. Delivery can be "
            "split or duplicated between them: a message that one connection acks "
            "does not reach the others. This is usually a mistake, for example two "
            "runs that use the same session id or name."
        )
    else:
        content = (
            "Only one connection is attached to this session now. Delivery is no "
            "longer split."
        )
    payload = {
        "type": proto.NOTIFY,
        "id": f"connections:{next(_NOTICE_SEQ)}",
        "content": content,
        "meta": {
            "kind": "multiple_connections",
            "connections": count,
            "level": "direct",
            "threshold": "direct",
        },
    }
    for conn in conns:
        await _safe_send(conn, payload)


async def _attach(conn: Connection, key: str) -> None:
    """Attach a connection to a session, and detach it from its previous one."""
    if conn.session_key == key:
        return
    await _detach(conn)
    now = time.time()
    conns = CONNECTIONS.setdefault(key, [])
    conns.append(conn)
    conn.session_key = key
    conn.inflight.clear()
    SESSIONS.ensure(key, now)
    SESSIONS.touch(key, now, persist=True)
    for tracker in TRACKERS.values():
        if key in tracker.subscribers:
            tracker.wake.set()  # resume polling for this session's PRs
    if conn.dispatch_task is None:
        conn.dispatch_task = asyncio.create_task(_dispatch_loop(conn))
    else:
        conn.wake.set()
    if len(conns) > 1:
        await _announce_connections(key)


async def _detach(conn: Connection) -> None:
    key = conn.session_key
    if key is None:
        return
    conn.session_key = None
    conns = CONNECTIONS.get(key, [])
    if conn in conns:
        conns.remove(conn)
    if not conns:
        CONNECTIONS.pop(key, None)
    # The handle idle rule and session expiry count from here.
    SESSIONS.touch(key, time.time(), persist=True)
    if len(conns) == 1:
        await _announce_connections(key)


def _next_callback_timeout(key: str, now: float) -> float | None:
    """Seconds until this session's soonest not-yet-due scheduled callback, so the
    loop can sleep until it comes due; None if none are pending (wait for a wake)."""
    upcoming = [float(e.get("due_at", 0.0)) - now for e in scheduler.pending(key)]
    future = [d for d in upcoming if d > 0]
    return min(future) if future else None


async def _dispatch_loop(conn: Connection) -> None:
    """Deliver undelivered notifications (callbacks, PR events, messages) to the
    session that this connection is attached to.

    Event-driven: each pass delivers everything currently deliverable, then blocks
    on the wake event until either new content arrives (_wake) or the soonest
    not-yet-due scheduled callback comes due. Idle with nothing pending costs zero CPU."""
    while True:
        key = conn.session_key
        timeout: float | None = None
        if key:
            now = time.time()
            for entry in scheduler.due_callbacks(key, now):
                callback_id = str(entry.get("id", ""))
                if callback_id and callback_id not in conn.inflight:
                    content, meta = _build_callback_notification(entry, key, now)
                    if not await _send_notify(
                        conn,
                        {
                            "type": proto.NOTIFY,
                            "id": callback_id,
                            "content": content,
                            "meta": meta,
                        },
                    ):
                        return
                    conn.inflight.add(callback_id)
            for tracker in list(TRACKERS.values()):
                if key not in tracker.subscribers:
                    continue
                # If events were dropped from the cache while this subscriber was
                # away, surface a one-time "history truncated" notice ahead of the
                # surviving events, so they know to check the PR for what was lost.
                missed = tracker.missed.get(key, 0)
                trunc_id = f"trunc:{tracker.storage_key}"
                if missed > 0 and trunc_id not in conn.inflight:
                    content = (
                        f"⚠️ {tracker.key}: {missed} earlier update(s) were dropped "
                        "from the cache before they reached you (the PR was very "
                        "active while this session was away). Check the PR directly "
                        "for anything important."
                    )
                    meta = {
                        "severity": "high",
                        "kind": "pr_truncated",
                        "pr": tracker.key,
                    }
                    if not await _send_notify(
                        conn,
                        {
                            "type": proto.NOTIFY,
                            "id": trunc_id,
                            "content": content,
                            "meta": meta,
                        },
                    ):
                        return
                    conn.inflight.add(trunc_id)
                acked = tracker.acked.get(key, set())
                for event in tracker.events:
                    event_id = event["id"]
                    if event_id in acked:
                        continue
                    nid = f"pr:{tracker.storage_key}:{event_id}"
                    if nid in conn.inflight:
                        continue
                    payload = {
                        "type": proto.NOTIFY,
                        "id": nid,
                        "content": event["content"],
                        "meta": event["meta"],
                    }
                    if not await _send_notify(conn, payload):
                        return
                    conn.inflight.add(nid)
            if not await _deliver_messages(conn, key):
                return
            timeout = _next_callback_timeout(key, now)
        try:
            await asyncio.wait_for(conn.wake.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        conn.wake.clear()


async def _handle_schedule(websocket, conn: Connection, msg: dict) -> None:
    key = _session_key_for(conn, msg)
    if key is None:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    if (err := _limit_error(conn, key, "callbacks")) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    delay = max(0, int(msg.get("delay_seconds", 300)))
    callback_id = scheduler.schedule(
        key, time.time() + delay, kind=str(msg.get("kind", "scheduled"))
    )
    _wake(key)  # let the dispatch loop schedule its wake for the new due time
    await _send(
        websocket, proto.SCHEDULED, msg, id=callback_id, due_at=time.time() + delay
    )


# Frame type -> the account permission that it needs. Other frames need none.
_FRAME_PERMISSIONS: dict[str, str] = {
    proto.SCHEDULE: "schedule",
    proto.LIST: "schedule",
    proto.SUBSCRIBE_PR: "pr",
    proto.UNSUBSCRIBE_PR: "pr",
    proto.REFRESH_PR: "pr",
    proto.LIST_PR_SUBSCRIPTIONS: "pr",
    proto.SUBSCRIBE_FORGEJO_PR: "pr",
    proto.UNSUBSCRIBE_FORGEJO_PR: "pr",
    proto.REFRESH_FORGEJO_PR: "pr",
    proto.LIST_FORGEJO_PR_SUBSCRIPTIONS: "pr",
    proto.LIST_AGENTS: "messaging",
    proto.SET_AVAILABILITY: "messaging",
    proto.JOIN_CHANNEL: "messaging",
    proto.LEAVE_CHANNEL: "messaging",
    proto.POST: "messaging",
    proto.DM: "messaging",
    proto.SET_THRESHOLD: "messaging",
    proto.SET_CHANNEL_TOPIC: "messaging",
    proto.LIST_CHANNELS: "messaging",
    proto.LIST_SUBSCRIPTIONS: "messaging",
    proto.REACT: "messaging",
    proto.MESSAGE_STATUS: "messaging",
}


async def _handle(websocket) -> None:
    # Authenticate once, at connect. The account and the credential stay with the
    # connection. _credential_check_loop closes it if the credential goes away.
    ACCOUNTS.reload_if_changed()
    provided = websocket.request.headers.get(proto.AUTH_HEADER) or ""
    auth = ACCOUNTS.authenticate(provided, time.time())
    if auth is None:
        await websocket.close(code=proto.UNAUTHORIZED_CLOSE_CODE, reason="unauthorized")
        return
    conn = Connection(websocket, *auth)
    ALL_CONNECTIONS.add(conn)
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(msg, dict):
                continue
            if conn.session_key:
                SESSIONS.touch(conn.session_key, time.time(), persist=False)
            await _handle_frame(conn, msg)
    except ConnectionClosed:
        pass
    finally:
        ALL_CONNECTIONS.discard(conn)
        if conn.dispatch_task is not None:
            conn.dispatch_task.cancel()
        await _detach(conn)


async def _handle_frame(conn: Connection, msg: dict) -> None:
    websocket = conn.ws
    kind = msg.get("type")
    account = conn.account
    if account is None:  # removed from the accounts file since connect
        await websocket.close(
            code=proto.UNAUTHORIZED_CLOSE_CODE, reason="account removed"
        )
        return
    permission = _FRAME_PERMISSIONS.get(kind) if isinstance(kind, str) else None
    if permission is not None and permission not in account.permissions:
        await _send(websocket, proto.ERROR, msg, error=f"forbidden: {permission}")
        return

    if kind == proto.REGISTER:
        await _handle_register(conn, msg)

    elif kind == proto.WHOAMI:
        await _handle_whoami(conn, msg)

    elif kind == proto.SCHEDULE:
        await _handle_schedule(websocket, conn, msg)

    elif kind == proto.ACK:
        _handle_ack(conn, msg)

    elif kind == proto.LIST:
        key = _session_key_for(conn, msg)
        items = (
            [
                {
                    "id": e.get("id"),
                    "due_at": e.get("due_at"),
                    "kind": e.get("kind"),
                }
                for e in scheduler.pending(key)
            ]
            if key
            else []
        )
        await _send(websocket, proto.LIST_RESULT, msg, items=items)

    elif kind == proto.SUBSCRIBE_PR:
        await _handle_subscribe(websocket, conn, msg)

    elif kind == proto.UNSUBSCRIBE_PR:
        _handle_unsubscribe(conn, msg)
        await _send(websocket, proto.UNSUBSCRIBED, msg, pr=_msg_key(msg))

    elif kind == proto.LIST_PR_SUBSCRIPTIONS:
        await _handle_list_pr_subscriptions(websocket, conn, msg)

    elif kind == proto.SUBSCRIBE_FORGEJO_PR:
        await _handle_subscribe(websocket, conn, msg, provider="forgejo")

    elif kind == proto.UNSUBSCRIBE_FORGEJO_PR:
        _handle_unsubscribe(conn, msg, provider="forgejo")
        _uinst = _msg_instance(msg)  # named-only echo; default stays v1-shaped
        await _send(
            websocket,
            proto.UNSUBSCRIBED,
            msg,
            pr=_msg_key(msg),
            **({"instance": _uinst} if _uinst else {}),
        )

    elif kind == proto.LIST_FORGEJO_PR_SUBSCRIPTIONS:
        await _handle_list_pr_subscriptions(websocket, conn, msg, provider="forgejo")

    elif kind == proto.REFRESH_PR:
        await _handle_refresh(websocket, conn, msg)

    elif kind == proto.REFRESH_FORGEJO_PR:
        await _handle_refresh(websocket, conn, msg, provider="forgejo")

    elif kind == proto.REGISTER_AGENT:
        await _handle_register_agent(websocket, conn, msg)

    elif kind == proto.UNREGISTER_AGENT:
        await _handle_unregister_agent(websocket, conn, msg)

    elif kind == proto.LIST_AGENTS:
        await _handle_list_agents(websocket, conn, msg)

    elif kind == proto.SET_AVAILABILITY:
        await _handle_set_availability(websocket, conn, msg)

    elif kind == proto.JOIN_CHANNEL:
        await _handle_join_channel(websocket, conn, msg)

    elif kind == proto.LEAVE_CHANNEL:
        await _handle_leave_channel(websocket, conn, msg)

    elif kind == proto.POST:
        await _handle_post(websocket, conn, msg)

    elif kind == proto.DM:
        await _handle_dm(websocket, conn, msg)

    elif kind == proto.SET_THRESHOLD:
        await _handle_set_threshold(websocket, conn, msg)

    elif kind == proto.SET_CHANNEL_TOPIC:
        await _handle_set_channel_topic(websocket, conn, msg)

    elif kind == proto.LIST_CHANNELS:
        await _handle_list_channels(websocket, conn, msg)

    elif kind == proto.LIST_SUBSCRIPTIONS:
        await _handle_list_subscriptions(websocket, conn, msg)

    elif kind == proto.REACT:
        await _handle_react(websocket, conn, msg)

    elif kind == proto.MESSAGE_STATUS:
        await _handle_message_status(websocket, conn, msg)

    else:
        # No silent drops: an unknown type means a newer client is speaking a verb
        # this server predates. Reply ERROR so the skew fails loudly on the client
        # (which otherwise blocks on a reply that never comes) instead of hanging.
        await _send(
            websocket,
            proto.ERROR,
            msg,
            error=f"unknown message type: {kind}",
        )


async def _handle_register(conn: Connection, msg: dict) -> None:
    """Attach the connection to a session. A session id ``@<handle>`` attaches to
    the session that holds the handle in this account, or else to a new session
    with that id, which then takes the handle."""
    session_id = msg.get("session_id") or None
    if session_id is None:
        await _detach(conn)
        return
    try:
        if not isinstance(session_id, str):
            raise sessions.InvalidSessionId(f"invalid session id {session_id!r}")
        sessions.validate_session_id(session_id)
        handle = ""
        if session_id.startswith(sessions.HANDLE_SESSION_PREFIX):
            handle = session_id[len(sessions.HANDLE_SESSION_PREFIX) :]
            sessions.validate_name(handle)
    except sessions.SessionError as exc:
        await _send(conn.ws, proto.ERROR, msg, error=str(exc))
        return
    if handle:
        holder = SESSIONS.by_handle(conn.account_id, handle)
        if holder is not None:
            await _attach(conn, holder.key)
            return
    key = sessions.session_key(conn.account_id, session_id)
    await _attach(conn, key)
    if handle and SESSIONS.by_handle(conn.account_id, handle) is None:
        SESSIONS.register_handle(
            key,
            handle,
            now=time.time(),
            is_live=_live,
            idle=_handle_idle_seconds(),
        )
        _wake(key)


async def _handle_whoami(conn: Connection, msg: dict) -> None:
    account = conn.account
    assert account is not None  # _handle_frame checked it
    key = conn.session_key
    record = SESSIONS.get(key) if key else None
    limits = {
        name: getattr(account.limits, name)
        for name in accounts.Limits.__dataclass_fields__
    }
    await _send(
        conn.ws,
        proto.WHOAMI_RESULT,
        msg,
        account=account.id,
        label=account.label,
        default_account=ACCOUNTS.default_id,
        permissions=sorted(account.permissions),
        channels=list(account.channels) if account.channels is not None else None,
        max_severity=account.max_severity,
        limits=limits,
        session_id=sessions.split_key(key)[1] if key else None,
        handle=record.handle if record is not None else "",
        connections=len(CONNECTIONS.get(key, ())) if key else 0,
    )


def _handle_ack(conn: Connection, msg: dict) -> None:
    nid = msg.get("id")
    key = conn.session_key
    if not isinstance(nid, str) or not nid or key is None:
        return
    # Any ack means this session's client just surfaced a delivery into a runnable
    # context — stamp last_acked for the context-wedge liveness signal (a connected
    # session that stops acking while messages pend is the wedge fingerprint).
    SESSIONS.mark_acked(key, time.time())
    if nid.startswith("trunc:"):
        # trunc:{storage_key}. Acking the notice clears the missed counter until
        # the next truncation.
        tracker = TRACKERS.get(nid[len("trunc:") :])
        if tracker is not None and key in tracker.subscribers:
            tracker.missed[key] = 0
            pr_monitor.save_subscriber(tracker, key)
        conn.inflight.discard(nid)
    elif nid.startswith("pr:"):
        storage_key, _, event_id = nid[3:].rpartition(":")
        tracker = TRACKERS.get(storage_key)
        if tracker is not None and key in tracker.acked:
            tracker.acked[key].add(event_id)
            conn.inflight.discard(nid)
            pr_monitor.save_subscriber(tracker, key)
            _finalize_terminal(tracker, key)
    elif nid.startswith("msg:"):
        # nid is the message id "msg:<topic_key>:<seq>". The topic key itself carries
        # a ':' ("chan:"/"dm:") but the trailing seq never does, so rpartition(':')
        # peels the seq off and the remainder (after the "msg:" prefix) is the key.
        topic_key, _, _seq = nid[len("msg:") :].rpartition(":")
        topic = TOPICS.get(topic_key)
        if topic is not None and key in topic.members:
            topic.acked.setdefault(key, set()).add(nid)
            conn.inflight.discard(nid)
            message_topic.save_subscriber(settings.data_dir(), topic, key)
    else:
        scheduler.delete(key, nid)
        conn.inflight.discard(nid)


# --------------------------------------------------------------------------- #
# PR subscription handling
# --------------------------------------------------------------------------- #


def _msg_key(msg: dict) -> str:
    return pr_monitor.pr_key(msg.get("owner"), msg.get("repo"), msg.get("number"))


def _msg_instance(msg: dict) -> str:
    """The Forgejo instance alias carried on a PR message payload, "" for the default
    instance (absent, null, or empty). github never carries one."""
    return msg.get("instance") or ""


def _forgejo_config_error(provider: str, instance: str) -> str | None:
    """Validate that a forgejo (provider, instance) is served here, returning a clear
    error string when it isn't (else None). github is always served.

    - Default instance ("") unconfigured -> the v1 "set FORGEJO_API_URL / FORGEJO_TOKEN"
      message (byte-identical, so the default path is unchanged).
    - A named alias that isn't configured -> "unknown Forgejo instance '<alias>' —
      configured: <aliases>" (names only, never URLs, per spec). When no named instances
      exist at all, say so explicitly rather than list an empty set."""
    if provider != "forgejo":
        return None
    if not instance:
        if _forgejo_client("") is None:
            return (
                "Forgejo PR monitoring is not configured on this server "
                "(set FORGEJO_API_URL / FORGEJO_TOKEN)."
            )
        return None
    if _forgejo_client(instance) is not None:
        return None
    aliases = _forgejo_aliases()
    configured = ", ".join(aliases) if aliases else "(none configured)"
    return (
        f"unknown Forgejo instance {instance!r} — configured: {configured}. Name a "
        "configured instance's alias, or omit the prefix for the default instance."
    )


def _msg_storage_key(msg: dict, provider: str) -> str:
    return pr_monitor.storage_key(
        provider,
        msg.get("owner"),
        msg.get("repo"),
        msg.get("number"),
        _msg_instance(msg) if provider == "forgejo" else "",
    )


async def _handle_subscribe(
    websocket, conn: Connection, msg: dict, provider: str = "github"
) -> None:
    key = _session_key_for(conn, msg)
    owner, repo, number = msg.get("owner"), msg.get("repo"), msg.get("number")
    if not key or not owner or not repo or number is None:
        await _send(
            websocket, proto.ERROR, msg, error="missing session id or PR reference"
        )
        return
    instance = _msg_instance(msg) if provider == "forgejo" else ""
    if err := _forgejo_config_error(provider, instance):
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    client = _pr_client(provider, instance)
    pr_ref = pr_monitor.pr_key(owner, repo, number)  # display ref for replies/errors
    # TRACKERS/dir key — alias-tagged for a named forgejo instance, unprefixed default.
    skey = pr_monitor.storage_key(provider, owner, repo, number, instance)
    tracker = TRACKERS.get(skey)
    if tracker is None or key not in tracker.subscribers:
        if (err := _limit_error(conn, key, "pr_subscriptions")) is not None:
            await _send(websocket, proto.ERROR, msg, error=err)
            return

    if tracker is None:
        tracker = pr_monitor.PRTracker(
            owner,
            repo,
            int(number),
            client,
            provider=provider,
            base_url=client.base_url if provider == "forgejo" else None,
            instance=instance,
        )
        try:
            summary = await tracker.initial_poll()
        except Exception as exc:  # noqa: BLE001 - report any fetch failure to the agent
            await _send(
                websocket, proto.ERROR, msg, error=f"could not fetch {pr_ref}: {exc}"
            )
            return
        TRACKERS[skey] = tracker
        tracker.next_poll_at = time.time() + _poll_delay(
            tracker
        )  # baseline done; schedule first real poll
        pr_monitor.save_state(tracker)
        tracker.task = asyncio.create_task(_tracker_loop(tracker))
    else:
        summary = pr_monitor.summarize(tracker.snapshot)

    if tracker.terminal:
        # Echo `instance` ONLY for a named instance — the skew belt only needs it there,
        # and omitting it keeps the default (and github) reply byte-identical to v1.
        inst = {"instance": instance} if instance else {}
        await _send(
            websocket,
            proto.SUBSCRIBED,
            msg,
            pr=pr_ref,
            summary=summary,
            merged=tracker.merged,
            closed=True,
            **inst,
        )
        return

    tracker.subscribers.add(key)
    tracker.idle_since = None  # reactivated; clear any warm marker
    pr_monitor.save_state(
        tracker
    )  # persist so a restart in the gap can't re-idle/reap it
    tracker.acked[key] = set(tracker.event_ids)  # join without replaying old events
    tracker.missed[key] = 0  # caught up by definition; no truncation to report
    pr_monitor.save_subscriber(tracker, key)
    tracker.wake.set()  # resume polling for this PR
    _wake(key)  # deliver any catch-up / future events immediately
    inst = {"instance": instance} if instance else {}  # named-only echo (see above)
    await _send(
        websocket,
        proto.SUBSCRIBED,
        msg,
        pr=pr_ref,
        summary=summary,
        merged=tracker.merged,
        closed=False,
        **inst,
    )


def _handle_unsubscribe(conn: Connection, msg: dict, provider: str = "github") -> None:
    key = _session_key_for(conn, msg)
    tracker = TRACKERS.get(_msg_storage_key(msg, provider))
    if tracker is not None and key is not None:
        _unsubscribe(tracker, key)


def _unsubscribe(tracker: pr_monitor.PRTracker, key: str) -> None:
    if key in tracker.subscribers:
        skey = tracker.storage_key
        tracker.subscribers.discard(key)
        tracker.acked.pop(key, None)
        tracker.missed.pop(key, None)
        pr_monitor.delete_subscriber(tracker, key)
        if not tracker.subscribers:
            # Keep a non-terminal tracker warm (polling auto-suspends with no
            # subscribers) so a quick re-subscribe reuses its cached snapshot; the
            # reaper deletes it after the TTL. A terminal tracker, or warm retention
            # disabled, is removed immediately as before.
            if _warm_ttl_seconds() > 0 and not tracker.terminal:
                tracker.idle_since = time.time()
                pr_monitor.save_state(tracker)
            else:
                _remove_tracker(skey)


async def _handle_list_pr_subscriptions(
    websocket, conn: Connection, msg: dict, provider: str = "github"
) -> None:
    key = _session_key_for(conn, msg)
    items = [
        {
            "pr": t.key,  # human-facing ref (owner/repo#number), not the storage key
            "merged": t.merged,
            "pending": len(t.unacked_for(key)),
            # Named-instance alias so the relay can render "<alias>:owner/repo#n";
            # omitted for the default instance (and github) so those items stay v1-shaped.
            **(
                {"instance": t.instance}
                if (t.provider == "forgejo" and t.instance)
                else {}
            ),
        }
        for t in TRACKERS.values()
        if key in t.subscribers and t.provider == provider
    ]
    await _send(websocket, proto.SUBSCRIPTIONS_RESULT, msg, items=items)


async def _handle_refresh(
    websocket, conn: Connection, msg: dict, provider: str = "github"
) -> None:
    """Poll a tracked PR now instead of at its scheduled time. Any session may ask,
    not only a subscriber: the use case is one agent telling another's PR watcher
    that something just happened. Events still reach subscribers the normal way."""
    skey = _msg_storage_key(msg, provider)
    tracker = TRACKERS.get(skey)
    key = _msg_key(msg)
    if tracker is None:
        await _send(websocket, proto.ERROR, msg, error=f"{key} is not tracked")
        return
    instance = _msg_instance(msg) if provider == "forgejo" else ""
    inst = {"instance": instance} if instance else {}  # named-only echo, as elsewhere
    reply: dict[str, object] = {
        "pr": key,
        "subscribers": len(tracker.subscribers),
        **inst,
    }
    reason: str | None = None
    if tracker.terminal:
        reason = "merged" if tracker.merged else "closed or gone"
    elif not any(_live(k) for k in tracker.subscribers):
        reason = "no connected subscribers"
    else:
        throttle_until = (
            tracker.client.should_throttle() if tracker.client is not None else None
        )
        if throttle_until is not None:
            reason = f"rate limited until {_iso(throttle_until)}"
    if reason is not None:
        await _send(
            websocket, proto.PR_REFRESHED, msg, polled=False, reason=reason, **reply
        )
        return
    tracker.refresh_pending = True
    tracker.next_poll_at = time.time()
    tracker.wake.set()
    await _send(websocket, proto.PR_REFRESHED, msg, polled=True, **reply)


# --------------------------------------------------------------------------- #
# handles: the agent directory
# --------------------------------------------------------------------------- #


def _agent_view(record: sessions.SessionRecord, reader_account: str) -> dict:
    """A session record as the wire AgentRecord, for a reader in ``reader_account``.
    The session id is visible only in the same account. Deprecated fields get stub
    values."""
    return {
        "name": sessions.display_handle(record, reader_account),
        "session_id": record.session_id if record.account == reader_account else "",
        "description": record.description,
        "capabilities": record.capabilities,
        "working_dir": "",
        "default_threshold": record.default_threshold,
        "registered_at": record.registered_at,
        "last_seen": record.last_seen,
        "reclaim_key_hash": "",
        "generation": 1,
        "last_acked": record.last_acked,
        "account": record.account,
    }


async def _handle_register_agent(websocket, conn: Connection, msg: dict) -> None:
    key = _session_key_for(conn, msg)
    if not key:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    name = msg.get("name")
    if not name or not isinstance(name, str):
        await _send(websocket, proto.ERROR, msg, error="missing agent name")
        return
    try:
        record, displaced = SESSIONS.register_handle(
            key,
            name,
            now=time.time(),
            is_live=_live,
            idle=_handle_idle_seconds(),
            takeover=bool(msg.get("takeover")),
            description=msg.get("description") or "",
            capabilities=msg.get("capabilities") or "",
            default_threshold=msg.get("default_threshold"),
        )
    except sessions.SessionError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if displaced is not None:
        await _send_handle_lost(displaced, name, conn.account_id)
    await _send(
        websocket, proto.AGENT_OK, msg, agent=_agent_view(record, conn.account_id)
    )


async def _send_handle_lost(key: str, handle: str, account_id: str) -> None:
    """Tell a session whose handle another session took. Only connected sessions
    get the notice."""
    payload = {
        "type": proto.NOTIFY,
        "id": f"handle_lost:{next(_NOTICE_SEQ)}",
        "content": (
            f"⟳ Another session took over your name '{handle}'. You no longer hold "
            "it: you cannot send messages until you register a name, and DMs to "
            f"'{handle}' now reach the other session. Your channel memberships and "
            "your unread messages stay with this session."
        ),
        "meta": {
            "kind": "handle_lost",
            "handle": handle,
            "level": "direct",
            "threshold": "direct",
        },
    }
    for conn in CONNECTIONS.get(key, ()):
        await _send_notify(conn, payload)


async def _handle_unregister_agent(websocket, conn: Connection, msg: dict) -> None:
    """Release the session's handle and leave every channel and DM."""
    key = _session_key_for(conn, msg)
    if not key:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    before = SESSIONS.release_handle(key)
    if before is not None:
        _leave_all_topics(key)
    await _send(
        websocket,
        proto.AGENT_OK,
        msg,
        agent=_agent_view(before, conn.account_id) if before is not None else None,
    )


def _leave_all_topics(key: str) -> None:
    for topic in list(TOPICS.values()):
        if key in topic.members:
            topic.leave(key)
            message_topic.delete_subscriber(settings.data_dir(), topic, key)
            message_topic.save_state(settings.data_dir(), topic)


async def _handle_set_availability(websocket, conn: Connection, msg: dict) -> None:
    key = _session_key_for(conn, msg)
    if not key:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    try:
        record = SESSIONS.set_threshold(key, msg.get("default_threshold") or "")
    except sessions.SessionError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    await _send(
        websocket, proto.AGENT_OK, msg, agent=_agent_view(record, conn.account_id)
    )


async def _handle_list_agents(websocket, conn: Connection, msg: dict) -> None:
    agents = [
        {**_agent_view(record, conn.account_id), "connected": _live(record.key)}
        for record in SESSIONS.list()
        if record.handle
    ]
    await _send(websocket, proto.AGENT_LIST, msg, agents=agents)


# --------------------------------------------------------------------------- #
# agent messaging: channels + DMs over the message-topic primitive
# --------------------------------------------------------------------------- #


async def _resolve_sender(
    websocket, conn: Connection, msg: dict
) -> tuple[str | None, sessions.SessionRecord | None]:
    """Resolve (session key, record) for a messaging request, or (None, None)
    after sending an ERROR. A messaging request needs a handle: other sessions see
    it as the sender."""
    key = _session_key_for(conn, msg)
    if not key:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return None, None
    record = SESSIONS.get(key)
    if record is None or not record.handle:
        await _send(websocket, proto.ERROR, msg, error="register_agent first")
        return None, None
    return key, record


def _resolve_handle(ref: str, own_account: str) -> sessions.SessionRecord | None:
    """The session that holds ``handle`` or ``handle@account``, or None."""
    try:
        account, handle = sessions.parse_handle_ref(ref.removeprefix("@"), own_account)
    except sessions.SessionError:
        return None
    return SESSIONS.by_handle(account, handle)


def _qualified(record: sessions.SessionRecord) -> str:
    return f"{record.handle}@{record.account}"


def _member_display(key: str, reader_account: str) -> str:
    """How a topic member appears to a reader: its handle, or a placeholder for a
    session that holds none."""
    record = SESSIONS.get(key)
    if record is not None and record.handle:
        return sessions.display_handle(record, reader_account)
    account, session_id = sessions.split_key(key)
    if account == reader_account:
        return f"(session {session_id})"
    return f"(unnamed@{account})"


def _sender_display(message: message_topic.Message, reader_account: str) -> str:
    """The sender as a reader sees it: the handle it held when it posted, bare in
    the reader's account, else qualified."""
    if "/" not in message.sender:  # "system"
        return message.sender
    account = sessions.split_key(message.sender)[0]
    handle = message.sender_handle or "(unnamed)"
    return handle if account == reader_account else f"{handle}@{account}"


def _sender_account(message: message_topic.Message) -> str:
    return sessions.split_key(message.sender)[0] if "/" in message.sender else ""


def _mentions_display(message: message_topic.Message, reader_account: str) -> list[str]:
    """Mentions are session keys for handles that resolved at post time, and raw
    strings otherwise."""
    return [
        _member_display(m, reader_account) if "/" in m else m for m in message.mentions
    ]


def _message_view(message: message_topic.Message, reader_account: str) -> dict:
    view = message.to_dict()
    del view["sender_handle"]
    return {
        **view,
        "sender": _sender_display(message, reader_account),
        "mentions": _mentions_display(message, reader_account),
        "account": _sender_account(message),
    }


def _channel_name(topic: message_topic.MessageTopic) -> str:
    return topic.key[len("chan:") :] if topic.key.startswith("chan:") else topic.key


def _topic_prefix(topic: message_topic.MessageTopic) -> str:
    """The `[dm]` / `[#name]` lead-in shared by every rendered line in a topic."""
    return "[dm]" if topic.kind == "dm" else f"[#{_channel_name(topic)}]"


def _snippet(body: str) -> str:
    """A short, single-line excerpt of a message body for inline reaction rendering."""
    flat = " ".join(body.split())
    if len(flat) <= REACTION_SNIPPET_MAX:
        return flat
    return flat[: REACTION_SNIPPET_MAX - 1].rstrip() + "…"


def _msg_handle(message: message_topic.Message) -> str:
    """The bare `#N` handle for a message, or `""` when its ordinal is unset (legacy)."""
    return f"#{message.ordinal}" if message.ordinal else ""


def _handle_prefix(message: message_topic.Message) -> str:
    """The leading `#N ` a rendered line opens with, or `""` for an unset ordinal."""
    handle = _msg_handle(message)
    return f"{handle} " if handle else ""


def _render_reaction(
    topic: message_topic.MessageTopic,
    message: message_topic.Message,
    reader_account: str,
) -> str:
    """Render a reaction against the message it targets, e.g.
    `#150 [#room] agent-b reacted "👍" to #147 (agent-a's "<snippet>")`. The line leads
    with the reaction's own handle and references the target's handle; if the target has
    been compacted out of the log, fall back to a generic `… to an earlier message`."""
    prefix = _topic_prefix(topic)
    lead = _handle_prefix(message)
    sender = _sender_display(message, reader_account)
    target = next((m for m in topic.messages if m.id == message.target), None)
    if target is None:
        return f'{lead}{prefix} {sender} reacted "{message.body}" to an earlier message'
    target_handle = _msg_handle(target)
    ref = f"{target_handle} " if target_handle else ""
    return (
        f'{lead}{prefix} {sender} reacted "{message.body}" to '
        f"{ref}({_sender_display(target, reader_account)}'s "
        f'"{_snippet(target.body)}")'
    )


def _render_message(
    topic: message_topic.MessageTopic,
    message: message_topic.Message,
    addressed: bool,
    reader_account: str,
) -> str:
    """The one-line rendering a recipient sees. Every line leads with the message's
    `#N` handle (omitted only for a legacy/unset ordinal). DMs read
    `#N [dm] sender: body`; channels read `#N [#name] sender: body`; an addressed (DM /
    mentioned / @here) message gets a trailing ` (→ you)` marker so the recipient can
    tell it was aimed at them. A sender in another account shows as
    `handle@account`. A reaction renders against its target instead (see
    `_render_reaction`)."""
    if message.intent == "reaction":
        return _render_reaction(topic, message, reader_account)
    marker = " (→ you)" if addressed else ""
    return (
        f"{_handle_prefix(message)}{_topic_prefix(topic)} "
        f"{_sender_display(message, reader_account)}: {message.body}{marker}"
    )


def _wake_topic_members(topic: message_topic.MessageTopic) -> None:
    """Nudge every member's dispatch loop after a new message is appended, so each
    delivers it on the next pass (mirrors _wake_subscribers for PR trackers)."""
    for key in topic.members:
        _wake(key)


async def _deliver_messages(conn: Connection, key: str) -> bool:
    """Ship every not-yet-acked message in every topic this session belongs to.

    Sender-side loudness only: each NOTIFY is stamped with the per-recipient `level`
    (compute_level) and `threshold` (the per-topic override or the session's default)
    so the client can make the surface-vs-hold decision. The server never gates — it
    ships eagerly, exactly as the PR path does. Returns False if the send failed (so
    the dispatch loop tears down), True otherwise."""
    record = SESSIONS.get(key)
    default_threshold = (
        record.default_threshold if record is not None else sessions.DEFAULT_THRESHOLD
    )
    reader_account = sessions.split_key(key)[0]
    for topic in list(TOPICS.values()):
        if key not in topic.members:
            continue
        acked = topic.acked.get(key, set())
        threshold = messaging.effective_threshold(
            topic.thresholds.get(key), default_threshold
        )
        is_dm = topic.kind == "dm"
        for message in topic.messages:
            if message.id in acked or message.id in conn.inflight:
                continue
            addressed = is_dm or key in message.mentions
            level = messaging.compute_level(
                severity=message.severity,
                addressed=addressed,
                intent=message.intent,
            )
            meta: dict[str, object] = {
                "kind": "message",
                "context": topic.key,
                "from": _sender_display(message, reader_account),
                "intent": message.intent,
                "severity": message.severity,
                "mentions": _mentions_display(message, reader_account),
                "level": level,
                "threshold": threshold,
            }
            if sender_account := _sender_account(message):
                meta["account"] = sender_account
            payload = {
                "type": proto.NOTIFY,
                "id": message.id,
                "content": _render_message(topic, message, addressed, reader_account),
                "meta": meta,
            }
            if not await _send_notify(conn, payload):
                return False
            conn.inflight.add(message.id)
    return True


def _get_or_create_topic(key: str, kind: str) -> message_topic.MessageTopic:
    topic = TOPICS.get(key)
    if topic is None:
        topic = message_topic.MessageTopic(key, kind)
        TOPICS[key] = topic
    return topic


def _limit_error(conn: Connection, key: str, limit: str) -> str | None:
    """An error string if the session is at its account's ``limit``, else None.
    For posts_per_minute, a None result also spends one post."""
    account = conn.account
    if account is None:
        return "account removed"
    value: int | None = getattr(account.limits, limit)
    if value is None:
        return None
    if limit == "posts_per_minute":
        now = time.time()
        tokens, last = POST_BUCKETS.get(key, (float(value), now))
        tokens = min(float(value), tokens + (now - last) * value / 60.0)
        if tokens < 1.0:
            POST_BUCKETS[key] = (tokens, now)
            return f"rate limited: at most {value} posts and DMs a minute"
        POST_BUCKETS[key] = (tokens - 1.0, now)
        return None
    if limit == "channels":
        used = sum(
            1 for t in TOPICS.values() if t.kind == "channel" and key in t.members
        )
    elif limit == "pr_subscriptions":
        used = sum(1 for t in TRACKERS.values() if key in t.subscribers)
    elif limit == "callbacks":
        used = len(scheduler.pending(key))
    else:
        raise ValueError(f"unknown limit {limit!r}")
    if used >= value:
        return f"limit reached: {limit} is {value} for this account"
    return None


def _channel_error(conn: Connection, channel: str) -> str | None:
    """An error string if the account cannot use ``channel``, else None."""
    account = conn.account
    if account is None or not account.allows_channel(channel):
        return f"forbidden: channel {channel!r}"
    return None


async def _handle_join_channel(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None:
        return
    try:
        channel_key = messaging.channel_key(msg.get("channel") or "")
        threshold = msg.get("threshold")
        if threshold is not None:
            messaging.validate_threshold(threshold)
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if (err := _channel_error(conn, msg["channel"])) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    topic = TOPICS.get(channel_key)
    if topic is None or key not in topic.members:
        if (err := _limit_error(conn, key, "channels")) is not None:
            await _send(websocket, proto.ERROR, msg, error=err)
            return
    created = channel_key not in TOPICS
    topic = _get_or_create_topic(channel_key, "channel")
    requested_topic = msg.get("topic")
    if requested_topic and (created or not topic.messages):
        topic.topic = requested_topic
    topic.join(key, now=time.time(), threshold=threshold)
    message_topic.save_state(settings.data_dir(), topic)
    message_topic.save_subscriber(settings.data_dir(), topic, key)
    await _send(
        websocket,
        proto.CHANNEL_JOINED,
        msg,
        channel=_channel_name(topic),
        members=sorted(_member_display(m, conn.account_id) for m in topic.members),
        topic=topic.topic,
        history=[
            _message_view(m, conn.account_id)
            for m in topic.history_tail(_channel_history_n())
        ],
    )


async def _handle_leave_channel(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None:
        return
    try:
        channel_key = messaging.channel_key(msg.get("channel") or "")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    topic = TOPICS.get(channel_key)
    if topic is not None and key in topic.members:
        topic.leave(key)
        message_topic.delete_subscriber(settings.data_dir(), topic, key)
        message_topic.save_state(settings.data_dir(), topic)
    await _send(websocket, proto.AGENT_OK, msg)


def _validate_message_fields(
    conn: Connection, msg: dict, default_intent: str
) -> tuple[str, str]:
    """Validate a post/dm and return (intent, severity), with the severity lowered
    to the account's maximum. Raises MessagingError."""
    intent = msg.get("intent") or default_intent
    severity = msg.get("severity") or "normal"
    messaging.validate_intent(intent)
    messaging.validate_severity(severity)
    body = msg.get("body") or ""
    if not isinstance(body, str):
        raise messaging.MessagingError("body must be a string")
    if len(body.encode("utf-8")) > MAX_BODY_BYTES:
        raise messaging.MessagingError(
            f"message body too long: at most {MAX_BODY_BYTES} bytes"
        )
    account = conn.account
    if account is not None:
        severity = account.cap_severity(severity)
    return intent, severity


def _persist_post(
    topic: message_topic.MessageTopic,
    sender_key: str,
    message: message_topic.Message,
) -> None:
    """Common persistence after authoring a message: the sender pre-acks its own post
    (no self-delivery), the log is appended, and state/subscriber are flushed."""
    topic.acked.setdefault(sender_key, set()).add(message.id)
    message_topic.append_messages(settings.data_dir(), topic, [message])
    message_topic.save_state(settings.data_dir(), topic)
    message_topic.save_subscriber(settings.data_dir(), topic, sender_key)


async def _handle_post(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None or record is None:
        return
    try:
        channel_key = messaging.channel_key(msg.get("channel") or "")
        intent, severity = _validate_message_fields(conn, msg, "fyi")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if (err := _channel_error(conn, msg["channel"])) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    topic = TOPICS.get(channel_key)
    if topic is None or key not in topic.members:
        if (err := _limit_error(conn, key, "channels")) is not None:
            await _send(websocket, proto.ERROR, msg, error=err)
            return
    if (err := _limit_error(conn, key, "posts_per_minute")) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    mentions: list[str] = []
    for ref in msg.get("mentions") or ():
        if not isinstance(ref, str):
            continue
        target = _resolve_handle(ref, conn.account_id)
        mentions.append(target.key if target is not None else ref)
    now = time.time()
    topic = _get_or_create_topic(channel_key, "channel")
    if key not in topic.members:  # posting auto-joins the sender
        topic.join(key, now=now)
    message = topic.post(
        key,
        now=now,
        body=msg.get("body") or "",
        sender_handle=record.handle,
        intent=intent,
        severity=severity,
        mentions=tuple(mentions),
        ordinal=_alloc_ordinal(),
    )
    _persist_post(topic, key, message)
    _wake_topic_members(topic)
    await _send(
        websocket,
        proto.POSTED,
        msg,
        id=message.id,
        ordinal=message.ordinal,
        context=topic.key,
        members=len(topic.members),
    )


def _dm_topic(participants: list[sessions.SessionRecord]) -> message_topic.MessageTopic:
    """The DM thread for exactly these sessions. A new thread's key is made from
    the qualified handles, with a ``#N`` suffix if an older thread (for other
    sessions that held the same handles) already uses it."""
    keys = {r.key for r in participants}
    for topic in TOPICS.values():
        if topic.kind == "dm" and topic.members == keys:
            return topic
    base = messaging.dm_key([_qualified(r) for r in participants])
    topic_key, n = base, 1
    while topic_key in TOPICS:
        n += 1
        topic_key = f"{base}#{n}"
    return _get_or_create_topic(topic_key, "dm")


async def _handle_dm(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None or record is None:
        return
    to = msg.get("to")
    to = [to] if isinstance(to, str) else list(to or [])
    try:
        intent, severity = _validate_message_fields(conn, msg, "request")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if not to:
        await _send(
            websocket, proto.ERROR, msg, error="dm needs at least one recipient"
        )
        return
    participants: dict[str, sessions.SessionRecord] = {key: record}
    for ref in to:
        target = _resolve_handle(ref, conn.account_id) if isinstance(ref, str) else None
        if target is None:
            await _send(websocket, proto.ERROR, msg, error=f"unknown agent {ref!r}")
            return
        participants[target.key] = target
    if (err := _limit_error(conn, key, "posts_per_minute")) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    now = time.time()
    topic = _dm_topic(list(participants.values()))
    for member in participants:  # a DM thread's members are its participant sessions
        if member not in topic.members:
            topic.join(member, now=now)
            message_topic.save_subscriber(settings.data_dir(), topic, member)
    message = topic.post(
        key,
        now=now,
        body=msg.get("body") or "",
        sender_handle=record.handle,
        intent=intent,
        severity=severity,
        ordinal=_alloc_ordinal(),
    )
    _persist_post(topic, key, message)
    _wake_topic_members(topic)
    await _send(
        websocket,
        proto.POSTED,
        msg,
        id=message.id,
        ordinal=message.ordinal,
        context=topic.key,
        members=len(topic.members),
    )


async def _handle_set_threshold(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None:
        return
    threshold = msg.get("threshold")
    try:
        messaging.validate_threshold(threshold or "")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    context = msg.get("context") or ""
    topic = TOPICS.get(context)
    if topic is None or key not in topic.members:
        await _send(websocket, proto.ERROR, msg, error=f"not a member of {context!r}")
        return
    topic.thresholds[key] = threshold
    message_topic.save_subscriber(settings.data_dir(), topic, key)
    await _send(websocket, proto.AGENT_OK, msg)


async def _handle_set_channel_topic(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None:
        return
    try:
        channel_key = messaging.channel_key(msg.get("channel") or "")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if (err := _channel_error(conn, msg["channel"])) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    topic = TOPICS.get(channel_key)
    if topic is None:
        await _send(
            websocket,
            proto.ERROR,
            msg,
            error=f"no such channel {msg.get('channel')!r}",
        )
        return
    topic.topic = msg.get("topic") or ""
    message_topic.save_state(settings.data_dir(), topic)
    await _send(websocket, proto.AGENT_OK, msg)


async def _handle_list_channels(websocket, conn: Connection, msg: dict) -> None:
    account = conn.account
    channels = [
        {
            "name": _channel_name(topic),
            "topic": topic.topic,
            "members": len(topic.members),
            "last_activity": topic.last_activity,
        }
        for topic in TOPICS.values()
        if topic.kind == "channel"
        and account is not None
        and account.allows_channel(_channel_name(topic))
    ]
    await _send(websocket, proto.CHANNEL_LIST, msg, channels=channels)


async def _handle_list_subscriptions(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None or record is None:
        return
    subscriptions = [
        {
            "context": topic.key,
            "kind": topic.kind,
            "threshold": messaging.effective_threshold(
                topic.thresholds.get(key), record.default_threshold
            ),
        }
        for topic in TOPICS.values()
        if key in topic.members
    ]
    await _send(websocket, proto.SUBSCRIPTION_LIST, msg, subscriptions=subscriptions)


# --------------------------------------------------------------------------- #
# receipts + reactions: both ride the message-topic primitive
# --------------------------------------------------------------------------- #


def _topic_for_message(message_id: str) -> message_topic.MessageTopic | None:
    """Resolve the topic owning ``message_id`` (``msg:<topic_key>:<seq>``), or None.

    Mirrors the ``_handle_ack`` ``msg:`` parsing: the topic key carries a ``:``
    (``chan:`` / ``dm:``) but the trailing seq never does, so ``rpartition(':')``
    peels the seq and the remainder (after the ``msg:`` prefix) is the topic key."""
    if not message_id.startswith("msg:"):
        return None
    topic_key, _, _seq = message_id[len("msg:") :].rpartition(":")
    return TOPICS.get(topic_key)


def _message_by_ordinal(
    n: int,
) -> tuple[message_topic.MessageTopic, message_topic.Message] | None:
    """The (topic, message) for global ordinal ``n``, found by scanning TOPICS. Ordinals
    are globally unique, so there is at most one match; react/message_status are
    low-frequency, so a scan beats maintaining a dedicated index."""
    for topic in TOPICS.values():
        for message in topic.messages:
            if message.ordinal == n:
                return topic, message
    return None


def _resolve_target(
    s: str,
) -> tuple[message_topic.MessageTopic, message_topic.Message] | None:
    """Resolve an agent-supplied message reference to (topic, message).

    Accepts a ``#N`` / ``N`` global handle (resolved by ordinal) or a full ``msg:`` id
    (the always-available escape hatch). Returns None when nothing matches."""
    ordinal = messaging.parse_handle(s)
    if ordinal is not None:
        return _message_by_ordinal(ordinal)
    if s.startswith("msg:"):
        topic = _topic_for_message(s)
        if topic is None:
            return None
        message = next((m for m in topic.messages if m.id == s), None)
        return (topic, message) if message is not None else None
    return None


async def _handle_react(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None or record is None:
        return
    raw_target = msg.get("target") or ""
    reaction = msg.get("reaction") or ""
    resolved = _resolve_target(raw_target)
    if resolved is None:
        await _send(
            websocket, proto.ERROR, msg, error=f"unknown message {raw_target!r}"
        )
        return
    topic, target_message = resolved
    if key not in topic.members:  # reacting does not auto-join
        await _send(websocket, proto.ERROR, msg, error=f"not a member of {topic.key!r}")
        return
    try:
        messaging.validate_reaction(reaction)
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if (err := _limit_error(conn, key, "posts_per_minute")) is not None:
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    # A reaction is a real message (intent="reaction", target=<resolved id>) on the
    # normal post path: it persists, appears in scrollback/catch_up, and ships at ambient
    # (forced by compute_level), so it is seen but never wakes — not even the author. It
    # gets its own handle too (uniform). target is the resolved full id so reactions_for
    # / the render lookup key correctly even when the agent reacted via a #N handle.
    message = topic.post(
        key,
        now=time.time(),
        body=reaction,
        sender_handle=record.handle,
        intent="reaction",
        target=target_message.id,
        ordinal=_alloc_ordinal(),
    )
    _persist_post(topic, key, message)
    _wake_topic_members(topic)
    await _send(websocket, proto.AGENT_OK, msg, id=message.id)


async def _handle_message_status(websocket, conn: Connection, msg: dict) -> None:
    key, record = await _resolve_sender(websocket, conn, msg)
    if key is None:
        return
    raw_target = msg.get("target") or ""
    resolved = _resolve_target(raw_target)
    if resolved is None:
        await _send(
            websocket, proto.ERROR, msg, error=f"unknown message {raw_target!r}"
        )
        return
    topic, target_msg = resolved
    if key not in topic.members:
        await _send(websocket, proto.ERROR, msg, error=f"not a member of {topic.key!r}")
        return
    # delivery_status / reactions_for key on the resolved message's full id, regardless
    # of whether the agent referenced it by #N handle or full id. The message's own
    # author pre-acks its post, so excluding it keeps the receipt an honest tally of
    # *recipients*.
    target = target_msg.id
    delivered_keys, pending_keys = topic.delivery_status(target)
    delivered = [k for k in delivered_keys if k != target_msg.sender]
    pending = [k for k in pending_keys if k != target_msg.sender]

    def _recipient_info(keys: list[str]) -> dict:
        """Per-recipient liveness so the sender can tell 'delivered into a running
        session' from 'delivered to an agent idle since X'. `last_acked` lets the
        client flag a connected recipient that does not ack a pending message as
        possibly context-wedged."""
        info: dict = {}
        for k in keys:
            rec = SESSIONS.get(k)
            info[_member_display(k, conn.account_id)] = {
                "connected": _live(k),
                "last_seen": rec.last_seen if rec is not None else None,
                "last_acked": rec.last_acked if rec is not None else None,
            }
        return info

    await _send(
        websocket,
        proto.MESSAGE_STATUS_RESULT,
        msg,
        delivered=[_member_display(k, conn.account_id) for k in delivered],
        pending=[_member_display(k, conn.account_id) for k in pending],
        recipients=_recipient_info(delivered + pending),
        reactions=[
            {"by": _sender_display(m, conn.account_id), "reaction": m.body}
            for m in topic.messages
            if m.intent == "reaction" and m.target == target
        ],
    )


def _reap_idle_topics(now: float) -> list[str]:
    """Delete message topics that have been both memberless and silent past the TTL,
    dropping them from TOPICS and from disk. Returns the keys removed."""
    ttl = _channel_ttl_seconds()
    removed: list[str] = []
    for key, topic in list(TOPICS.items()):
        if topic.reapable(now=now, ttl=ttl):
            del TOPICS[key]
            message_topic.delete_topic(settings.data_dir(), key)
            removed.append(key)
    return removed


def _finalize_terminal(tracker: pr_monitor.PRTracker, session_id: str) -> None:
    """After a subscriber acks the terminal (merged/gone) event, drop them."""
    if (
        tracker.terminal
        and tracker.terminal_id
        and tracker.terminal_id in tracker.acked.get(session_id, set())
    ):
        tracker.subscribers.discard(session_id)
        tracker.acked.pop(session_id, None)
        tracker.missed.pop(session_id, None)
        pr_monitor.delete_subscriber(tracker, session_id)
        if not tracker.subscribers:
            _remove_tracker(tracker.storage_key)


def _poll_delay(tracker: pr_monitor.PRTracker) -> float:
    """Seconds until this tracker's next poll. SUBSPACE_PR_POLL_SECONDS forces
    a fixed cadence (testing / manual override); otherwise use the backoff schedule."""
    override = os.environ.get("SUBSPACE_PR_POLL_SECONDS")
    if override:
        try:
            return max(0.2, float(override))
        except ValueError:
            pass
    nxt = pr_schedule.compute_next_poll(_now_utc(), tracker.consecutive_no_update)
    return max(1.0, (nxt - _now_utc()).total_seconds())


def _remove_tracker(key: str) -> None:
    tracker = TRACKERS.pop(key, None)
    if tracker is not None and tracker.task is not None:
        tracker.task.cancel()
    pr_monitor.delete_tracker(key)


def _reap_idle_trackers(now: float) -> list[str]:
    """Remove warm (subscriber-less, non-terminal) trackers idle past the TTL.
    Self-heals: a subscriber-less non-terminal tracker with no idle marker
    (e.g. loaded from older on-disk state) gets its clock started here.
    Returns the keys removed. No-op when warm retention is disabled."""
    ttl = _warm_ttl_seconds()
    if ttl <= 0:
        return []
    removed: list[str] = []
    for key, tracker in list(TRACKERS.items()):
        if tracker.subscribers or tracker.terminal:
            continue
        if tracker.idle_since is None:
            tracker.idle_since = now
            pr_monitor.save_state(tracker)
            continue
        if now - tracker.idle_since >= ttl:
            _remove_tracker(key)
            removed.append(key)
    return removed


async def _reaper_loop() -> None:
    while True:
        # Wake often enough to honor the soonest of the warm-PR and channel TTLs (half
        # each), but never busier than every second nor idler than every 5 minutes.
        halves = [
            t / 2.0 for t in (_warm_ttl_seconds(), _channel_ttl_seconds()) if t > 0
        ]
        interval = max(1.0, min([*halves, 300.0]))
        await asyncio.sleep(interval)
        _reap_expired_sessions(time.time())
        _reap_idle_trackers(time.time())
        _reap_idle_topics(time.time())


def _reap_expired_sessions(now: float) -> list[str]:
    """Remove sessions that have had no connection for the session TTL: their
    handle, topic memberships, PR subscriptions, and callbacks. Returns the keys
    removed."""
    removed = SESSIONS.expired(now, _session_ttl_seconds(), _live)
    for key in removed:
        _leave_all_topics(key)
        for tracker in list(TRACKERS.values()):
            _unsubscribe(tracker, key)
        scheduler.delete_session(key)
        POST_BUCKETS.pop(key, None)
        SESSIONS.remove(key)
    return removed


async def _credential_check_loop() -> None:
    """Reload the accounts file when it changes, and close every connection whose
    credential or account is no longer valid."""
    while True:
        await asyncio.sleep(_accounts_check_seconds())
        ACCOUNTS.reload_if_changed()
        now = time.time()
        for conn in list(ALL_CONNECTIONS):
            if ACCOUNTS.get(conn.account_id) is None or not ACCOUNTS.credential_valid(
                conn.account_id, conn.credential_id, now
            ):
                await conn.ws.close(
                    code=proto.UNAUTHORIZED_CLOSE_CODE, reason="credential revoked"
                )


def _emit(tracker: pr_monitor.PRTracker, event: dict) -> None:
    """Record a server-originated event (e.g. gone/auth) and append it to the log."""
    pr_monitor.append_events(tracker, tracker.record([event]))
    _wake_subscribers(tracker)  # deliver the gone/auth event without waiting


async def _wait_or_wake(tracker: pr_monitor.PRTracker, seconds: float) -> None:
    try:
        await asyncio.wait_for(tracker.wake.wait(), timeout=max(0.2, seconds))
        tracker.wake.clear()
    except asyncio.TimeoutError:
        pass


async def _tracker_loop(tracker: pr_monitor.PRTracker) -> None:
    """Poll a PR while at least one subscriber is connected; suspend otherwise."""
    while True:
        if not any(_live(k) for k in tracker.subscribers):
            tracker.wake.clear()
            await tracker.wake.wait()  # resumed when a subscribed session reconnects
            continue
        # Honor the persisted next-poll time, so a server restart or flap doesn't
        # stampede GitHub: each tracker waits out the remainder of its backoff.
        due_in = (tracker.next_poll_at or 0) - time.time()
        if due_in > 0:
            await _wait_or_wake(tracker, due_in)
            continue
        delay: float | None = None
        forced = tracker.refresh_pending
        tracker.refresh_pending = False
        try:
            added = tracker.record(await tracker.poll_once())
            pr_monitor.append_events(tracker, added)
            if added:
                _wake_subscribers(tracker)  # deliver new PR events immediately
                tracker.consecutive_no_update = 0
            elif not forced:
                tracker.consecutive_no_update += 1
        except pr_errors.PRNotFound as exc:
            _emit(
                tracker,
                pr_monitor.synthetic_event(
                    "pr_gone",
                    "high",
                    f"{tracker.key} could not be fetched ({exc} — deleted, or the token lost "
                    "access). Polling stopped and you've been unsubscribed.",
                    tracker.key,
                    f"gone:{tracker.key}",
                ),
            )
            pr_monitor.save_state(tracker)
            return
        except pr_errors.PRRateLimited as exc:
            wait = max(1.0, exc.reset_at - time.time())
            print(
                f"subspace: {tracker.key} rate limited; waiting {int(wait)}s",
                file=sys.stderr,
            )
            delay = wait + random.uniform(1.0, 15.0)  # defer; not a "no update"
        except pr_errors.PRAuthError as exc:
            if not tracker.auth_notified:
                if tracker.provider == "forgejo":
                    # Name the FAILING instance's alias and its exact token env var, so an
                    # operator knows which of possibly-several Forgejo secrets to fix. The
                    # default instance stays "Forgejo" / FORGEJO_TOKEN (v1-identical).
                    label = (
                        f"Forgejo ({tracker.instance})"
                        if tracker.instance
                        else "Forgejo"
                    )
                    token_env = _forgejo_token_env(tracker.instance)
                elif tracker.provider == "github":
                    label, token_env = "GitHub", "GITHUB_TOKEN"
                else:
                    label, token_env = tracker.provider, "the provider token"
                _emit(
                    tracker,
                    pr_monitor.synthetic_event(
                        "pr_auth_error",
                        "high",
                        f"{label} access to {tracker.key} failed ({exc}). Polling is paused "
                        f"until the server's {token_env} is fixed (restart the server with a "
                        "valid token).",
                        tracker.key,
                        f"auth_error:{tracker.key}",
                    ),
                )
                tracker.auth_notified = True
            tracker.consecutive_no_update = max(
                tracker.consecutive_no_update, AUTH_BACKOFF_LEVEL
            )
            print(
                f"subspace: auth error {tracker.key}: {exc}",
                file=sys.stderr,
            )
        except Exception as exc:  # noqa: BLE001 - transient/server/network: back off and retry
            print(
                f"subspace: poll error {tracker.key}: {exc}",
                file=sys.stderr,
            )
            tracker.consecutive_no_update += 1

        if delay is None:
            # A refresh that arrived while this poll was in flight may postdate the
            # fetch, so poll again at once rather than trust the result just gathered.
            delay = 0.0 if tracker.refresh_pending else _poll_delay(tracker)
            throttle_until = (
                tracker.client.should_throttle() if tracker.client is not None else None
            )
            if throttle_until is not None:
                delay = max(
                    delay, throttle_until - time.time() + random.uniform(1.0, 15.0)
                )
        tracker.next_poll_at = time.time() + delay
        pr_monitor.save_state(tracker)
        if tracker.terminal:  # merged: deliver the final event, then stop polling
            return


async def _send(websocket, msg_type: str, request: dict, **fields: object) -> None:
    payload: dict[str, object] = {"type": msg_type, **fields}
    if "req_id" in request:
        payload["req_id"] = request["req_id"]
    try:
        await websocket.send(json.dumps(payload))
    except ConnectionClosed:
        pass


def _caused_by_eof(exc: BaseException | None) -> bool:
    """True if `exc` or anything in its cause/context chain is an EOFError."""
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, EOFError):
            return True
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__
    return False


class _ProbeHandshakeFilter(logging.Filter):
    """Silence the benign 'opening handshake failed' tracebacks that k8s tcpSocket
    liveness/readiness probes produce: a probe opens a bare TCP connection to the
    WS-only port and closes it without sending the HTTP upgrade, so websockets fails
    the handshake (InvalidMessage caused by EOFError) and logs it at ERROR every probe
    cycle. A genuine handshake failure (malformed upgrade, bad headers) carries no
    EOFError in its chain, and auth rejections happen after a successful handshake, so
    filtering only the EOF-caused case drops the probe noise without hiding real errors.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if (
            record.getMessage() == "opening handshake failed"
            and record.exc_info
            and _caused_by_eof(record.exc_info[1])
        ):
            return False  # drop
        return True


def _load_state() -> None:
    """Migrate the data directory if needed, then load accounts, sessions, and
    message topics."""
    global ACCOUNTS, SESSIONS, TOPICS, _NEXT_ORDINAL
    data_dir = settings.data_dir()
    default_id = accounts.default_account_id()
    migrate_identity.migrate(data_dir, default_id)
    ACCOUNTS = accounts.AccountStore(
        accounts.accounts_path(data_dir), TOKEN, default_id
    )
    SESSIONS = sessions.SessionRegistry(data_dir)
    TOPICS = {topic.key: topic for topic in message_topic.load_all_topics(data_dir)}
    _NEXT_ORDINAL = _init_next_ordinal()


async def main() -> None:
    global GH, FJ, FJ_INSTANCES, TOKEN
    TOKEN = settings.token()  # auto-creates <SUBSPACE_DATA_DIR>/token if needed
    _load_state()
    GH = github_client.GitHubClient()
    # Forgejo is optional: only served when an instance URL is configured. A server
    # without it rejects forgejo_* subscriptions (and skips any forgejo trackers on
    # disk) rather than failing to start. FORGEJO_API_URL/FORGEJO_TOKEN is the DEFAULT
    # instance (byte-identical to v1); FORGEJO_<ALIAS>_API_URL/TOKEN pairs are additional
    # NAMED instances built into FJ_INSTANCES.
    FJ = forgejo_client.ForgejoClient()
    if not FJ.configured:
        FJ = None
    FJ_INSTANCES = _build_forgejo_instances(
        dict(os.environ), FJ.base_url if FJ is not None else ""
    )
    if FJ_INSTANCES:
        print(
            "subspace: Forgejo named instances configured: "
            + ", ".join(_forgejo_aliases()),
            file=sys.stderr,
        )
    for tracker in pr_monitor.load_trackers(_pr_clients()):
        TRACKERS[tracker.storage_key] = tracker
        tracker.task = asyncio.create_task(_tracker_loop(tracker))
    asyncio.create_task(_reaper_loop())
    asyncio.create_task(_credential_check_loop())

    # Graceful shutdown. k8s terminates pods with SIGTERM, whose default Python
    # disposition kills the process outright — the `async with serve(...)` block never
    # unwinds, so connected relays see an abnormal close (1006) rather than a clean
    # 1001 and (on pre-fix relays) crash instead of reconnecting. Install a handler that
    # resolves `stop` so the block exits normally and serve()'s teardown closes every
    # connection with 1001. SIGINT already unwinds via KeyboardInterrupt, but route it
    # the same way so a `tini`-forwarded signal (the server runs as tini's non-PID-1
    # child in the image) shuts down cleanly regardless of which signal arrives.
    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    def _request_stop() -> None:
        if not stop.done():
            stop.set_result(None)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass  # e.g. Windows: no signal handlers — fall back to default disposition

    # Quiet the per-probe-cycle handshake-failed traceback k8s tcpSocket probes cause.
    logging.getLogger("websockets.server").addFilter(_ProbeHandshakeFilter())

    host, port = settings.host(), settings.port()
    try:
        async with serve(_handle, host, port):
            print(f"subspace listening on ws://{host}:{port}", file=sys.stderr)
            await stop  # run until a shutdown signal resolves this
        print("subspace: shutting down (connections closed)", file=sys.stderr)
    except OSError as exc:
        print(
            f"subspace: cannot bind {host}:{port} (already running?): {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
