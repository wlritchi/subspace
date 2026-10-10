#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""
subspace server

A persistent, single-instance WebSocket server that owns notification state for
all Claude sessions on this machine. Per-session stdio MCP relays connect over
localhost, register their session id, and exchange notifications.

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
               SUBSPACE_PR_WARM_TTL_SECONDS (default 1800)
               SUBSPACE_PR_CHECK_SUMMARY_THRESHOLD (default 5; 0 disables)
               SUBSPACE_AGENT_TTL_SECONDS (default 900)
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
import hmac
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

import agent_registry  # noqa: E402
import forgejo_client  # noqa: E402
import github_client  # noqa: E402
import messaging  # noqa: E402
import message_topic  # noqa: E402
import pr_errors  # noqa: E402
import pr_monitor  # noqa: E402
import pr_schedule  # noqa: E402
import scheduler  # noqa: E402
import settings  # noqa: E402
import storage  # noqa: E402

# A scheduled callback this far past due was held while nothing was connected.
RECOVERED_THRESHOLD_SECONDS = 30
# consecutive-no-update level that forces ~max (8h) backoff after an auth failure.
AUTH_BACKOFF_LEVEL = 14
# How long an unsubscribed (subscriber-less, non-terminal) tracker is kept warm
# before the reaper deletes it, so a quick re-subscribe reuses its cached state.
WARM_TTL_DEFAULT_SECONDS = 1800.0
# How long an offline agent keeps its directory name reserved before a colliding
# register_agent may reclaim it. <=0 reclaims immediately once the session is gone.
AGENT_TTL_DEFAULT_SECONDS = 900.0
# How long a name's offline holder must stay quiet before a matching reclaim_key may
# bypass the grace — long enough to outlast a roll's reconnect blip so a same-key bg
# spare can't hijack a live agent that only briefly dropped. STOPGAP; see agent_registry.
RECLAIM_SETTLE_DEFAULT_SECONDS = 60.0
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


def _agent_ttl_seconds() -> float:
    raw = os.environ.get("SUBSPACE_AGENT_TTL_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return AGENT_TTL_DEFAULT_SECONDS


def _reclaim_settle_seconds() -> float:
    raw = os.environ.get("SUBSPACE_RECLAIM_SETTLE_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return RECLAIM_SETTLE_DEFAULT_SECONDS


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


# session id -> its current connection
CONNECTIONS: dict[str, "Connection"] = {}
# superseded session id -> {name, generation}: a pending "an heir has appeared"
# notice, shipped if/when that session reconnects (docs/specs/2026-07-09 slice d).
# In-memory only: a ceremony, not a delivery contract.
SUPERSEDED: dict[str, dict] = {}
# "owner/repo#number" -> PRTracker
TRACKERS: dict[str, pr_monitor.PRTracker] = {}
# The agent directory (Phase A): names -> records, persisted under <data_dir>/agents.
REGISTRY: agent_registry.AgentRegistry = agent_registry.AgentRegistry(
    settings.data_dir()
)
# Message topics (Phase B): topic key ("chan:<name>" / "dm:<names>") -> MessageTopic,
# persisted under <data_dir>/msg/<safe_key>/ (mirrors how TRACKERS is loaded).
TOPICS: dict[str, message_topic.MessageTopic] = {
    topic.key: topic for topic in message_topic.load_all_topics(settings.data_dir())
}


def _migrate_topic_members() -> None:
    """One-time member re-keying for topics that predate the name-keyed identity
    model (docs/specs/2026-07-09): member keys written by older servers are session
    ids. A key matching a registry record's session_id is renamed to that record's
    name; a key that is already a registered name is kept; anything else is a ghost
    (a session that died without a name holder to inherit its state) and is dropped
    — its acked state is meaningless without a successor. Runs at startup, before
    any connection is served, and persists what it changes."""
    sid_to_name = {r.session_id: r.name for r in REGISTRY.list()}
    names = set(sid_to_name.values())
    for topic in TOPICS.values():
        changed = False
        for member in sorted(topic.members):
            if member in names:
                continue
            new = sid_to_name.get(member)
            if new is not None:
                topic.rekey_member(member, new)
                message_topic.delete_subscriber(settings.data_dir(), topic, member)
                message_topic.save_subscriber(settings.data_dir(), topic, new)
            else:
                topic.leave(member)
                message_topic.delete_subscriber(settings.data_dir(), topic, member)
            changed = True
        if changed:
            message_topic.save_state(settings.data_dir(), topic)


_migrate_topic_members()


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


# Server-global, monotonic message-handle counter (Phase C addendum). The lib stays
# counter-agnostic; the server allocates an ordinal per post and passes it into
# MessageTopic.post, so every channel/DM/reaction message gets a stable, globally
# unique `#N` handle agents can pass back to react / message_status.
_NEXT_ORDINAL: int = _init_next_ordinal()


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


# Shared secret each relay must present (Authorization: Bearer <token>) to connect.
# Computed once at startup; relays compute the same value from SUBSPACE_DATA_DIR.
TOKEN = ""


def _session_live(session_id: str) -> bool:
    """Liveness predicate the registry uses for name-reclaim decisions."""
    return session_id in CONNECTIONS


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# scheduled one-shot callbacks (proof-of-concept feature)
# --------------------------------------------------------------------------- #


def _build_callback_notification(
    entry: dict, session_id: str, now: float
) -> tuple[str, dict[str, str]]:
    due = float(entry.get("due_at", now))
    created_for = entry.get("session_id", "?")
    late = int(now - due)
    recovered = (
        " (recovered after restart)" if late >= RECOVERED_THRESHOLD_SECONDS else ""
    )
    content = (
        f"Scheduled notification fired{recovered}. callback_id={entry.get('id')} "
        f"session_id={session_id} (scheduled for {created_for}) "
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
    def __init__(self, websocket) -> None:
        self.ws = websocket
        self.session_id: str | None = None
        self.inflight: set[str] = set()  # notification ids sent, awaiting ack
        self.wake = asyncio.Event()  # set to nudge the dispatch loop to deliver now


async def _safe_send(conn: Connection, payload: dict) -> bool:
    try:
        await conn.ws.send(json.dumps(payload))
        return True
    except ConnectionClosed:
        return False


def _wake(session_id: str | None) -> None:
    """Nudge a session's dispatch loop to deliver newly-available notifications.
    Setting an asyncio.Event is sync and safe to call from anywhere in the loop."""
    if not session_id:
        return
    conn = CONNECTIONS.get(session_id)
    if conn is not None:
        conn.wake.set()


def _wake_subscribers(tracker: pr_monitor.PRTracker) -> None:
    """Wake every connected subscriber of a tracker after new events are appended."""
    for sid in tracker.subscribers:
        _wake(sid)


def _next_callback_timeout(session_id: str, now: float) -> float | None:
    """Seconds until this session's soonest not-yet-due scheduled callback, so the
    loop can sleep until it comes due; None if none are pending (wait for a wake)."""
    upcoming = [
        float(e.get("due_at", 0.0)) - now for e in scheduler.pending(session_id)
    ]
    future = [d for d in upcoming if d > 0]
    return min(future) if future else None


async def _dispatch_loop(conn: Connection) -> None:
    """Deliver undelivered notifications (callbacks + PR events) for this session.

    Event-driven: each pass delivers everything currently deliverable, then blocks
    on the wake event until either new content arrives (_wake) or the soonest
    not-yet-due scheduled callback comes due. Idle with nothing pending costs zero CPU."""
    while True:
        session_id = conn.session_id
        timeout: float | None = None
        if session_id:
            now = time.time()
            for entry in scheduler.due_callbacks(session_id, now):
                callback_id = str(entry.get("id", ""))
                if callback_id and callback_id not in conn.inflight:
                    content, meta = _build_callback_notification(entry, session_id, now)
                    if not await _safe_send(
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
                if session_id not in tracker.subscribers:
                    continue
                # If events were dropped from the cache while this subscriber was
                # away, surface a one-time "history truncated" notice ahead of the
                # surviving events, so they know to check the PR for what was lost.
                missed = tracker.missed.get(session_id, 0)
                trunc_id = f"trunc:{tracker.storage_key}:{session_id}"
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
                    if not await _safe_send(
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
                acked = tracker.acked.get(session_id, set())
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
                    if not await _safe_send(conn, payload):
                        return
                    conn.inflight.add(nid)
            if not await _deliver_messages(conn, session_id):
                return
            timeout = _next_callback_timeout(session_id, now)
        try:
            await asyncio.wait_for(conn.wake.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        conn.wake.clear()


async def _handle(websocket) -> None:
    # Auth gate: validate the shared token once, at connect. The connection is
    # trusted for its lifetime afterwards (we never re-check per message). Use a
    # constant-time compare so a rejected connection's timing can't leak the token.
    provided = websocket.request.headers.get(proto.AUTH_HEADER) or ""
    if not hmac.compare_digest(provided, proto.auth_header_value(TOKEN)):
        await websocket.close(code=proto.UNAUTHORIZED_CLOSE_CODE, reason="unauthorized")
        return
    conn = Connection(websocket)
    dispatch_task: asyncio.Task | None = None
    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            kind = msg.get("type")

            if kind == proto.REGISTER:
                new_sid = msg.get("session_id") or None
                if (
                    conn.session_id
                    and CONNECTIONS.get(conn.session_id) is conn
                    and conn.session_id != new_sid
                ):
                    del CONNECTIONS[conn.session_id]
                conn.session_id = new_sid
                if new_sid:
                    CONNECTIONS[new_sid] = conn
                    for tracker in TRACKERS.values():
                        if new_sid in tracker.subscribers:
                            tracker.wake.set()  # resume polling for this session's PRs
                    if dispatch_task is None:
                        dispatch_task = asyncio.create_task(_dispatch_loop(conn))
                    if new_sid in SUPERSEDED:
                        # A session whose name was taken over while it was away:
                        # tell it an heir exists before it wonders why it's deaf.
                        await _send_heir_notice(conn, new_sid)

            elif kind == proto.SCHEDULE:
                await _handle_schedule(websocket, conn, msg)

            elif kind == proto.ACK:
                _handle_ack(conn, msg)

            elif kind == proto.LIST:
                session_id = msg.get("session_id") or conn.session_id
                items = (
                    [
                        {
                            "id": e.get("id"),
                            "due_at": e.get("due_at"),
                            "kind": e.get("kind"),
                        }
                        for e in scheduler.pending(session_id)
                    ]
                    if session_id
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
                await _handle_list_pr_subscriptions(
                    websocket, conn, msg, provider="forgejo"
                )

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
                # No silent drops: an unknown type means a newer relay is speaking a verb
                # this server predates. Reply ERROR so the skew fails loudly on the relay
                # (which otherwise blocks on a reply that never comes) instead of hanging.
                await _send(
                    websocket,
                    proto.ERROR,
                    msg,
                    error=f"unknown message type: {kind}",
                )
    except ConnectionClosed:
        pass
    finally:
        if dispatch_task is not None:
            dispatch_task.cancel()
        # Stamp last_seen so the name-reclaim grace clock starts from now; presence
        # flips to offline automatically once the connection leaves CONNECTIONS.
        if conn.session_id:
            REGISTRY.touch(conn.session_id, time.time())
        if conn.session_id and CONNECTIONS.get(conn.session_id) is conn:
            del CONNECTIONS[conn.session_id]


async def _handle_schedule(websocket, conn: Connection, msg: dict) -> None:
    session_id = msg.get("session_id") or conn.session_id
    if not session_id:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    delay = max(0, int(msg.get("delay_seconds", 300)))
    callback_id = scheduler.schedule(
        session_id, time.time() + delay, kind=str(msg.get("kind", "scheduled"))
    )
    _wake(session_id)  # let the dispatch loop schedule its wake for the new due time
    await _send(
        websocket, proto.SCHEDULED, msg, id=callback_id, due_at=time.time() + delay
    )


def _handle_ack(conn: Connection, msg: dict) -> None:
    nid = msg.get("id")
    if not nid:
        return
    # Any ack means this session's relay just surfaced a delivery into a runnable
    # context — stamp last_acked for the context-wedge liveness signal (a connected
    # session that stops acking while messages pend is the wedge fingerprint).
    if conn.session_id:
        REGISTRY.mark_acked(conn.session_id, time.time())
    if isinstance(nid, str) and nid.startswith("trunc:"):
        # trunc:{storage_key}:{sid} — the trailing session id never contains ':', so
        # rpartition(':') peels it off cleanly, leaving the storage key (which may carry
        # a "<provider>:" prefix for non-github PRs, plus '/' and '#') intact for lookup.
        # Acking the notice clears the missed counter until the next truncation.
        key, _, session_id = nid[len("trunc:") :].rpartition(":")
        tracker = TRACKERS.get(key)
        if tracker is not None and session_id in tracker.subscribers:
            tracker.missed[session_id] = 0
            pr_monitor.save_subscriber(tracker, session_id)
        conn.inflight.discard(nid)
    elif isinstance(nid, str) and nid.startswith("pr:"):
        key, _, event_id = nid[3:].rpartition(":")
        tracker = TRACKERS.get(key)
        if tracker is not None and conn.session_id and conn.session_id in tracker.acked:
            tracker.acked[conn.session_id].add(event_id)
            conn.inflight.discard(nid)
            pr_monitor.save_subscriber(tracker, conn.session_id)
            _finalize_terminal(tracker, conn.session_id)
    elif isinstance(nid, str) and nid.startswith("msg:"):
        # nid is the message id "msg:<topic_key>:<seq>". The topic key itself carries
        # a ':' ("chan:"/"dm:") but the trailing seq never does, so rpartition(':')
        # peels the seq off and the remainder (after the "msg:" prefix) is the key.
        topic_key, _, _seq = nid[len("msg:") :].rpartition(":")
        topic = TOPICS.get(topic_key)
        # Membership is keyed by agent NAME (name = service, session = process), so
        # the acking session is resolved to its registered name first. A session that
        # has been displaced from its name resolves to no record and cannot ack —
        # that is the generation fencing falling out of delivery-time resolution.
        record = REGISTRY.get_by_session(conn.session_id) if conn.session_id else None
        if topic is not None and record is not None and record.name in topic.members:
            topic.acked.setdefault(record.name, set()).add(nid)
            conn.inflight.discard(nid)
            message_topic.save_subscriber(settings.data_dir(), topic, record.name)
    else:
        if conn.session_id:
            scheduler.delete(conn.session_id, nid)
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
    session_id = conn.session_id or msg.get("session_id")
    owner, repo, number = msg.get("owner"), msg.get("repo"), msg.get("number")
    if not session_id or not owner or not repo or number is None:
        await _send(
            websocket, proto.ERROR, msg, error="missing session id or PR reference"
        )
        return
    instance = _msg_instance(msg) if provider == "forgejo" else ""
    if err := _forgejo_config_error(provider, instance):
        await _send(websocket, proto.ERROR, msg, error=err)
        return
    client = _pr_client(provider, instance)
    key = pr_monitor.pr_key(owner, repo, number)  # display ref for replies/errors
    # TRACKERS/dir key — alias-tagged for a named forgejo instance, unprefixed default.
    skey = pr_monitor.storage_key(provider, owner, repo, number, instance)
    tracker = TRACKERS.get(skey)

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
                websocket, proto.ERROR, msg, error=f"could not fetch {key}: {exc}"
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
            pr=key,
            summary=summary,
            merged=tracker.merged,
            closed=True,
            **inst,
        )
        return

    tracker.subscribers.add(session_id)
    tracker.idle_since = None  # reactivated; clear any warm marker
    pr_monitor.save_state(
        tracker
    )  # persist so a restart in the gap can't re-idle/reap it
    tracker.acked[session_id] = set(
        tracker.event_ids
    )  # join without replaying old events
    tracker.missed[session_id] = 0  # caught up by definition; no truncation to report
    pr_monitor.save_subscriber(tracker, session_id)
    tracker.wake.set()  # resume polling for this PR
    _wake(session_id)  # deliver any catch-up / future events immediately
    inst = {"instance": instance} if instance else {}  # named-only echo (see above)
    await _send(
        websocket,
        proto.SUBSCRIBED,
        msg,
        pr=key,
        summary=summary,
        merged=tracker.merged,
        closed=False,
        **inst,
    )


def _handle_unsubscribe(conn: Connection, msg: dict, provider: str = "github") -> None:
    session_id = conn.session_id or msg.get("session_id")
    skey = _msg_storage_key(msg, provider)
    tracker = TRACKERS.get(skey)
    if tracker is not None and session_id in tracker.subscribers:
        tracker.subscribers.discard(session_id)
        tracker.acked.pop(session_id, None)
        tracker.missed.pop(session_id, None)
        pr_monitor.delete_subscriber(tracker, session_id)
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
    session_id = conn.session_id or msg.get("session_id")
    items = [
        {
            "pr": t.key,  # human-facing ref (owner/repo#number), not the storage key
            "merged": t.merged,
            "pending": len(t.unacked_for(session_id)),
            # Named-instance alias so the relay can render "<alias>:owner/repo#n";
            # omitted for the default instance (and github) so those items stay v1-shaped.
            **(
                {"instance": t.instance}
                if (t.provider == "forgejo" and t.instance)
                else {}
            ),
        }
        for t in TRACKERS.values()
        if session_id in t.subscribers and t.provider == provider
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
    elif not any(s in CONNECTIONS for s in tracker.subscribers):
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
# agent directory handling (Phase A)
# --------------------------------------------------------------------------- #


async def _handle_register_agent(websocket, conn: Connection, msg: dict) -> None:
    session_id = conn.session_id or msg.get("session_id")
    if not session_id:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    name = msg.get("name")
    if not name:
        await _send(websocket, proto.ERROR, msg, error="missing agent name")
        return
    prior = _registry_by_name(name)  # pre-register holder, for succession detection
    try:
        record = REGISTRY.register(
            session_id,
            name,
            now=time.time(),
            is_session_live=_session_live,
            ttl=_agent_ttl_seconds(),
            settle=_reclaim_settle_seconds(),
            description=msg.get("description") or "",
            capabilities=msg.get("capabilities") or "",
            working_dir=msg.get("working_dir") or "",
            default_threshold=msg.get("default_threshold"),
            reclaim_key=msg.get("reclaim_key"),
        )
    except agent_registry.AgentRegistryError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    # Taking a name grants its topic memberships (membership is name-keyed), so any
    # messages the name accrued while unheld — or that a displaced predecessor never
    # surfaced — are deliverable NOW. Nudge this session's dispatch loop; without
    # this, a successor would wait for the next unrelated wake.
    _wake(record.session_id)
    if prior is not None and prior.session_id != record.session_id:
        await _announce_succession(record, prior.session_id)
    await _send(websocket, proto.AGENT_OK, msg, agent=record.to_dict())


async def _announce_succession(
    record: agent_registry.AgentRecord, prior_sid: str
) -> None:
    """Surface a name transfer (docs/specs/2026-07-09 slice d): a fleet-visible,
    ambient post in the well-known #system channel — knowing a handoff happened
    explains behavior the way knowing a pod rolled explains a cold cache — plus a
    direct heir notice to the superseded session if it is still connected, enabling
    an explicit predecessor->heir handoff (the predecessor can DM the name it used
    to hold; delivery-time resolution routes that to the heir)."""
    gen = record.generation
    topic = _get_or_create_topic("chan:system", "channel")
    message = topic.post(
        "system",
        now=time.time(),
        body=f"⟳ succession: {record.name} gen {gen - 1}→{gen}",
        intent="fyi",
        severity="low",
        ordinal=_alloc_ordinal(),
    )
    # Not _persist_post: "system" is an author, not a member — pre-acking it would
    # write a sub file that load_topic would resurrect as a phantom member.
    message_topic.append_messages(settings.data_dir(), topic, [message])
    message_topic.save_state(settings.data_dir(), topic)
    _wake_topic_members(topic)
    # Queue the heir notice for the superseded session. A live holder is never
    # displaced, so the predecessor is by definition NOT connected at succession
    # time — the notice is held and shipped if/when that session reconnects (a
    # briefly-dropped predecessor coming back is exactly the case that needs it).
    SUPERSEDED[prior_sid] = {"name": record.name, "generation": gen}
    conn = CONNECTIONS.get(prior_sid)
    if conn is not None:  # belt: ship now if a connection somehow still exists
        await _send_heir_notice(conn, prior_sid)


async def _send_heir_notice(conn: Connection, session_id: str) -> None:
    """Tell a superseded session that an heir holds its old name, enabling an
    explicit predecessor->heir handoff. One-shot: dropped from SUPERSEDED once
    sent (a server restart also clears it — this is a ceremony, not a contract)."""
    entry = SUPERSEDED.get(session_id)
    if entry is None:
        return
    name, gen = entry["name"], entry["generation"]
    payload = {
        "type": proto.NOTIFY,
        "id": f"succession:{name}:{gen}",
        "content": (
            f"⟳ an heir has appeared: '{name}' is now gen {gen}, held by another "
            "session. You no longer hold this name — you cannot send or ack as it, "
            "and a DM to it reaches your successor. To hand off in-flight work, "
            "re-register under a different name and message your old one."
        ),
        "meta": {"kind": "succession", "level": "direct", "threshold": "direct"},
    }
    if await _safe_send(conn, payload):
        conn.inflight.add(payload["id"])
        SUPERSEDED.pop(session_id, None)


async def _handle_unregister_agent(websocket, conn: Connection, msg: dict) -> None:
    session_id = conn.session_id or msg.get("session_id")
    if not session_id:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    record = REGISTRY.unregister(session_id)
    await _send(
        websocket,
        proto.AGENT_OK,
        msg,
        agent=record.to_dict() if record is not None else None,
    )


async def _handle_set_availability(websocket, conn: Connection, msg: dict) -> None:
    session_id = conn.session_id or msg.get("session_id")
    if not session_id:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return
    try:
        record = REGISTRY.set_availability(session_id, msg.get("default_threshold"))
    except agent_registry.AgentRegistryError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    await _send(websocket, proto.AGENT_OK, msg, agent=record.to_dict())


async def _handle_list_agents(websocket, conn: Connection, msg: dict) -> None:
    agents = [
        {**record.to_dict(), "connected": record.session_id in CONNECTIONS}
        for record in REGISTRY.list()
    ]
    await _send(websocket, proto.AGENT_LIST, msg, agents=agents)


# --------------------------------------------------------------------------- #
# agent messaging (Phase B): channels + DMs over the message-topic primitive
# --------------------------------------------------------------------------- #


async def _resolve_sender(
    websocket, conn: Connection, msg: dict
) -> tuple[str | None, agent_registry.AgentRecord | None]:
    """Resolve (session_id, agent record) for a messaging request, or (None, None)
    after sending an ERROR. Every messaging op requires the caller to be a registered
    agent: that is what guarantees a display name (for `from`/mentions/DM addressing)
    and a global wake-threshold default."""
    session_id = conn.session_id or msg.get("session_id")
    if not session_id:
        await _send(websocket, proto.ERROR, msg, error="no session id")
        return None, None
    record = REGISTRY.get_by_session(session_id)
    if record is None:
        await _send(websocket, proto.ERROR, msg, error="register_agent first")
        return None, None
    return session_id, record


def _registry_by_name(name: str) -> agent_registry.AgentRecord | None:
    for record in REGISTRY.list():
        if record.name == name:
            return record
    return None


def _member_names(topic: message_topic.MessageTopic) -> list[str]:
    """A topic's member names, sorted. Membership is keyed by agent name (a pre-
    migration session-id key would render raw, but startup migration converts or
    drops those)."""
    return sorted(topic.members)


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
    topic: message_topic.MessageTopic, message: message_topic.Message
) -> str:
    """Render a reaction against the message it targets, e.g.
    `#150 [#room] agent-b reacted "👍" to #147 (agent-a's "<snippet>")`. The line leads
    with the reaction's own handle and references the target's handle; if the target has
    been compacted out of the log, fall back to a generic `… to an earlier message`."""
    prefix = _topic_prefix(topic)
    lead = _handle_prefix(message)
    target = next((m for m in topic.messages if m.id == message.target), None)
    if target is None:
        return (
            f'{lead}{prefix} {message.sender} reacted "{message.body}" '
            "to an earlier message"
        )
    target_handle = _msg_handle(target)
    ref = f"{target_handle} " if target_handle else ""
    return (
        f'{lead}{prefix} {message.sender} reacted "{message.body}" to '
        f'{ref}({target.sender}\'s "{_snippet(target.body)}")'
    )


def _render_message(
    topic: message_topic.MessageTopic, message: message_topic.Message, addressed: bool
) -> str:
    """The one-line rendering a recipient sees. Every line leads with the message's
    `#N` handle (omitted only for a legacy/unset ordinal). DMs read
    `#N [dm] sender: body`; channels read `#N [#name] sender: body`; an addressed (DM /
    mentioned / @here) message gets a trailing ` (→ you)` marker so the recipient can
    tell it was aimed at them. A reaction renders against its target instead (see
    `_render_reaction`)."""
    if message.intent == "reaction":
        return _render_reaction(topic, message)
    marker = " (→ you)" if addressed else ""
    return (
        f"{_handle_prefix(message)}{_topic_prefix(topic)} "
        f"{message.sender}: {message.body}{marker}"
    )


def _wake_topic_members(topic: message_topic.MessageTopic) -> None:
    """Nudge every member's dispatch loop after a new message is appended, so each
    delivers it on the next pass (mirrors _wake_subscribers for PR trackers).
    Members are names; each is resolved to whichever session currently holds it —
    delivery-time resolution is what routes a thread to a name's successor."""
    for name in topic.members:
        record = _registry_by_name(name)
        if record is not None:
            _wake(record.session_id)


async def _deliver_messages(conn: Connection, session_id: str) -> bool:
    """Ship every not-yet-acked message in every topic this session belongs to.

    Sender-side loudness only: each NOTIFY is stamped with the per-recipient `level`
    (compute_level) and `threshold` (the per-topic override or the agent's default) so
    the shim can make the surface-vs-hold decision. The server never gates — it ships
    eagerly, exactly as the PR path does. Returns False if the send failed (so the
    dispatch loop tears down), True otherwise."""
    record = REGISTRY.get_by_session(session_id)
    if record is None:  # unregistered session: no name, so no memberships
        return True
    for topic in list(TOPICS.values()):
        if record.name not in topic.members:
            continue
        acked = topic.acked.get(record.name, set())
        threshold = messaging.effective_threshold(
            topic.thresholds.get(record.name), record.default_threshold
        )
        is_dm = topic.kind == "dm"
        for message in topic.messages:
            if message.id in acked or message.id in conn.inflight:
                continue
            addressed = is_dm or record.name in message.mentions
            level = messaging.compute_level(
                severity=message.severity,
                addressed=addressed,
                intent=message.intent,
            )
            payload = {
                "type": proto.NOTIFY,
                "id": message.id,
                "content": _render_message(topic, message, addressed),
                "meta": {
                    "kind": "message",
                    "context": topic.key,
                    "from": message.sender,
                    "intent": message.intent,
                    "severity": message.severity,
                    "mentions": list(message.mentions),
                    "level": level,
                    "threshold": threshold,
                },
            }
            if not await _safe_send(conn, payload):
                return False
            conn.inflight.add(message.id)
    return True


def _get_or_create_topic(key: str, kind: str) -> message_topic.MessageTopic:
    topic = TOPICS.get(key)
    if topic is None:
        topic = message_topic.MessageTopic(key, kind)
        TOPICS[key] = topic
    return topic


async def _handle_join_channel(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    try:
        key = messaging.channel_key(msg.get("channel") or "")
        threshold = msg.get("threshold")
        if threshold is not None:
            messaging.validate_threshold(threshold)
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    created = key not in TOPICS
    topic = _get_or_create_topic(key, "channel")
    requested_topic = msg.get("topic")
    if requested_topic and (created or not topic.messages):
        topic.topic = requested_topic
    topic.join(record.name, now=time.time(), threshold=threshold)
    message_topic.save_state(settings.data_dir(), topic)
    message_topic.save_subscriber(settings.data_dir(), topic, record.name)
    await _send(
        websocket,
        proto.CHANNEL_JOINED,
        msg,
        channel=_channel_name(topic),
        members=_member_names(topic),
        topic=topic.topic,
        history=[m.to_dict() for m in topic.history_tail(_channel_history_n())],
    )


async def _handle_leave_channel(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    try:
        key = messaging.channel_key(msg.get("channel") or "")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    topic = TOPICS.get(key)
    if topic is not None and record.name in topic.members:
        topic.leave(record.name)
        message_topic.delete_subscriber(settings.data_dir(), topic, record.name)
        message_topic.save_state(settings.data_dir(), topic)
    await _send(websocket, proto.AGENT_OK, msg)


def _validate_message_fields(msg: dict, default_intent: str) -> tuple[str, str]:
    """Validate and return (intent, severity) for a post/dm, raising MessagingError."""
    intent = msg.get("intent") or default_intent
    severity = msg.get("severity") or "normal"
    messaging.validate_intent(intent)
    messaging.validate_severity(severity)
    return intent, severity


def _persist_post(
    topic: message_topic.MessageTopic,
    sender_name: str,
    message: message_topic.Message,
) -> None:
    """Common persistence after authoring a message: the sender pre-acks its own post
    (no self-delivery), the log is appended, and state/subscriber are flushed."""
    topic.acked.setdefault(sender_name, set()).add(message.id)
    message_topic.append_messages(settings.data_dir(), topic, [message])
    message_topic.save_state(settings.data_dir(), topic)
    message_topic.save_subscriber(settings.data_dir(), topic, sender_name)


async def _handle_post(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    try:
        key = messaging.channel_key(msg.get("channel") or "")
        intent, severity = _validate_message_fields(msg, "fyi")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    now = time.time()
    topic = _get_or_create_topic(key, "channel")
    if record.name not in topic.members:  # posting auto-joins the sender
        topic.join(record.name, now=now)
    message = topic.post(
        record.name,
        now=now,
        body=msg.get("body") or "",
        intent=intent,
        severity=severity,
        mentions=tuple(msg.get("mentions") or ()),
        ordinal=_alloc_ordinal(),
    )
    _persist_post(topic, record.name, message)
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


async def _handle_dm(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    to = msg.get("to")
    to = [to] if isinstance(to, str) else list(to or [])
    try:
        intent, severity = _validate_message_fields(msg, "request")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    if not to:
        await _send(
            websocket, proto.ERROR, msg, error="dm needs at least one recipient"
        )
        return
    recipients: list[agent_registry.AgentRecord] = []
    for name in to:
        target = _registry_by_name(name)
        if target is None:
            await _send(websocket, proto.ERROR, msg, error=f"unknown agent {name!r}")
            return
        recipients.append(target)
    participants = sorted({record.name, *(r.name for r in recipients)})
    key = messaging.dm_key(participants)
    now = time.time()
    topic = _get_or_create_topic(key, "dm")
    for name in participants:  # a DM thread's members are its participant NAMES
        if name not in topic.members:
            topic.join(name, now=now)
            message_topic.save_subscriber(settings.data_dir(), topic, name)
    message = topic.post(
        record.name,
        now=now,
        body=msg.get("body") or "",
        intent=intent,
        severity=severity,
        ordinal=_alloc_ordinal(),
    )
    _persist_post(topic, record.name, message)
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
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    threshold = msg.get("threshold")
    try:
        messaging.validate_threshold(threshold or "")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    context = msg.get("context") or ""
    topic = TOPICS.get(context)
    if topic is None or record.name not in topic.members:
        await _send(websocket, proto.ERROR, msg, error=f"not a member of {context!r}")
        return
    topic.thresholds[record.name] = threshold
    message_topic.save_subscriber(settings.data_dir(), topic, record.name)
    await _send(websocket, proto.AGENT_OK, msg)


async def _handle_set_channel_topic(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    try:
        key = messaging.channel_key(msg.get("channel") or "")
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    topic = TOPICS.get(key)
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
    channels = [
        {
            "name": _channel_name(topic),
            "topic": topic.topic,
            "members": len(topic.members),
            "last_activity": topic.last_activity,
        }
        for topic in TOPICS.values()
        if topic.kind == "channel"
    ]
    await _send(websocket, proto.CHANNEL_LIST, msg, channels=channels)


async def _handle_list_subscriptions(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    subscriptions = [
        {
            "context": topic.key,
            "kind": topic.kind,
            "threshold": messaging.effective_threshold(
                topic.thresholds.get(record.name), record.default_threshold
            ),
        }
        for topic in TOPICS.values()
        if record.name in topic.members
    ]
    await _send(websocket, proto.SUBSCRIPTION_LIST, msg, subscriptions=subscriptions)


# --------------------------------------------------------------------------- #
# receipts + reactions (Phase C): both ride the Phase B message-topic primitive
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
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
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
    if record.name not in topic.members:  # reacting does not auto-join
        await _send(websocket, proto.ERROR, msg, error=f"not a member of {topic.key!r}")
        return
    try:
        messaging.validate_reaction(reaction)
    except messaging.MessagingError as exc:
        await _send(websocket, proto.ERROR, msg, error=str(exc))
        return
    # A reaction is a real message (intent="reaction", target=<resolved id>) on the
    # normal post path: it persists, appears in scrollback/catch_up, and ships at ambient
    # (forced by compute_level), so it is seen but never wakes — not even the author. It
    # gets its own handle too (uniform). target is the resolved full id so reactions_for
    # / the render lookup key correctly even when the agent reacted via a #N handle.
    message = topic.post(
        record.name,
        now=time.time(),
        body=reaction,
        intent="reaction",
        target=target_message.id,
        ordinal=_alloc_ordinal(),
    )
    _persist_post(topic, record.name, message)
    _wake_topic_members(topic)
    await _send(websocket, proto.AGENT_OK, msg, id=message.id)


async def _handle_message_status(websocket, conn: Connection, msg: dict) -> None:
    session_id, record = await _resolve_sender(websocket, conn, msg)
    if record is None:
        return
    raw_target = msg.get("target") or ""
    resolved = _resolve_target(raw_target)
    if resolved is None:
        await _send(
            websocket, proto.ERROR, msg, error=f"unknown message {raw_target!r}"
        )
        return
    topic, target_msg = resolved
    if record.name not in topic.members:
        await _send(websocket, proto.ERROR, msg, error=f"not a member of {topic.key!r}")
        return
    # delivery_status / reactions_for key on the resolved message's full id, regardless
    # of whether the agent referenced it by #N handle or full id.
    target = target_msg.id
    # Members are names, so delivery_status already yields names. The message's own
    # author pre-acks its post, so excluding it keeps the receipt an honest tally of
    # *recipients*.
    delivered_names, pending_names = topic.delivery_status(target)
    delivered = [n for n in delivered_names if n != target_msg.sender]
    pending = [n for n in pending_names if n != target_msg.sender]

    def _recipient_info(names: list[str]) -> dict:
        """Per-recipient liveness so the sender can tell 'delivered into a running
        session' from 'delivered to an agent idle since X' (docs/specs/2026-07-09
        slice c). Additive field: old relays render the plain name lists."""
        info: dict = {}
        for n in names:
            rec = _registry_by_name(n)
            info[n] = {
                "connected": rec is not None and rec.session_id in CONNECTIONS,
                "last_seen": rec.last_seen if rec is not None else None,
                # Last surfaced-a-push time; lets the relay flag a connected-but-not-
                # acking recipient with a pending message as possibly context-wedged
                # (docs/specs/2026-07-14). Additive; older relays ignore it.
                "last_acked": rec.last_acked if rec is not None else None,
            }
        return info

    await _send(
        websocket,
        proto.MESSAGE_STATUS_RESULT,
        msg,
        delivered=delivered,
        pending=pending,
        recipients=_recipient_info(delivered + pending),
        reactions=[{"by": s, "reaction": b} for s, b in topic.reactions_for(target)],
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
        _reap_idle_trackers(time.time())
        _reap_idle_topics(time.time())


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
        if not any(s in CONNECTIONS for s in tracker.subscribers):
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


async def main() -> None:
    global GH, FJ, FJ_INSTANCES, TOKEN
    TOKEN = settings.token()  # auto-creates <SUBSPACE_DATA_DIR>/token if needed
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
