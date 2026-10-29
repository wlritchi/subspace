# vim: filetype=python
"""Frame names and connection settings for the relay's WebSocket to a subspace server.

Messages are JSON text frames over a WebSocket. Request/response messages carry a
`req_id` the server echoes back so the relay can correlate replies; `notify`
messages are server-initiated and carry no `req_id`.

The specification is the subspace_protocol package (protocol/ in the subspace
repo). The relay keeps its own copy of the names it uses, so that it has no
dependency on that package at run time. tests/test_unit_relay_protocol.py checks
that the two agree.

stdlib only.
"""

import os
import secrets
import time
from pathlib import Path

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8137

# relay -> server
REGISTER = "register"  # {session_id}            attach this connection to a session
SCHEDULE = "schedule"  # {req_id, session_id, delay_seconds, kind}
ACK = "ack"  # {id}                              a delivered notification was handled
LIST = "list"  # {req_id, session_id}
SUBSCRIBE_PR = "subscribe_pr"  # {req_id, session_id, owner, repo, number}
UNSUBSCRIBE_PR = "unsubscribe_pr"  # {req_id, session_id, owner, repo, number}
LIST_PR_SUBSCRIPTIONS = "list_pr_subscriptions"  # {req_id, session_id}
# Forgejo/Gitea PR subscriptions (additive; parallel a separate REST poller). Reuse the
# SUBSCRIBED / UNSUBSCRIBED / SUBSCRIPTIONS_RESULT reply frames — same shapes as GitHub.
# Multi-instance (additive): an optional `instance` alias field selects a named Forgejo
# instance; absent/"" is the default instance, byte-identical to v1. The server echoes
# `instance` back in its replies so the relay can detect an old server that ignored it.
SUBSCRIBE_FORGEJO_PR = (
    "subscribe_forgejo_pr"  # {req_id, session_id, owner, repo, number, instance?}
)
UNSUBSCRIBE_FORGEJO_PR = (
    "unsubscribe_forgejo_pr"  # {req_id, session_id, owner, repo, number, instance?}
)
LIST_FORGEJO_PR_SUBSCRIPTIONS = "list_forgejo_pr_subscriptions"  # {req_id, session_id}
# On-demand poll of a tracked PR (any session may ask; results reach subscribers as
# normal PR events). The backoff level is untouched unless the poll finds activity.
REFRESH_PR = "refresh_pr"  # {req_id, session_id, owner, repo, number}
REFRESH_FORGEJO_PR = (
    "refresh_forgejo_pr"  # {req_id, session_id, owner, repo, number, instance?}
)
# agent directory (Phase A)
REGISTER_AGENT = "register_agent"  # {req_id, session_id, name, description?, capabilities?, default_threshold?}
UNREGISTER_AGENT = "unregister_agent"  # {req_id, session_id}
LIST_AGENTS = "list_agents"  # {req_id, session_id}
SET_AVAILABILITY = "set_availability"  # {req_id, session_id, default_threshold}
# agent messaging (Phase B) — relay -> server
JOIN_CHANNEL = "join_channel"  # {req_id, session_id, channel, threshold?, topic?}
LEAVE_CHANNEL = "leave_channel"  # {req_id, session_id, channel}
POST = "post"  # {req_id, session_id, channel, body, intent?, severity?, mentions?}
DM = "dm"  # {req_id, session_id, to: [name...], body, intent?, severity?}
SET_THRESHOLD = "set_threshold"  # {req_id, session_id, context, threshold}
SET_CHANNEL_TOPIC = "set_channel_topic"  # {req_id, session_id, channel, topic}
LIST_CHANNELS = "list_channels"  # {req_id, session_id}
LIST_SUBSCRIPTIONS = "list_subscriptions"  # {req_id, session_id}
# receipts + reactions (Phase C) — relay -> server
REACT = "react"  # {req_id, session_id, target, reaction}
MESSAGE_STATUS = "message_status"  # {req_id, session_id, target}
# The reply comes after the pending items of the registered session were sent
# (or after a short wait), so a client knows that it has seen everything pending.
WHOAMI = "whoami"  # {req_id}

# server -> relay
# NOTIFY meta optionally carries message-gating fields when kind=="message":
#   {kind:"message", context, level, threshold, from, intent, severity, mentions}.
# session_id is the session the connection was attached to when the frame was
# sent, so that a relay that re-registered can drop an earlier session's item.
NOTIFY = (
    "notify"  # {id, content, meta, session_id?}  deliver this to the agent, then ack
)
SCHEDULED = "scheduled"  # {req_id, id, due_at}
LIST_RESULT = "list_result"  # {req_id, items}
SUBSCRIBED = "subscribed"  # {req_id, pr, summary, merged, closed, instance?}
UNSUBSCRIBED = "unsubscribed"  # {req_id, pr, instance?}
SUBSCRIPTIONS_RESULT = (
    "subscriptions_result"  # {req_id, items:[{pr, instance?, merged, pending}]}
)
# polled=false carries `reason` (no connected subscribers / closed / rate limited);
# ERROR reports an unknown (never subscribed or already reaped) PR.
PR_REFRESHED = "pr_refreshed"  # {req_id, pr, instance?, polled, reason?, subscribers}
AGENT_OK = "agent_ok"  # {req_id, agent}          resulting record dict (or null/{name} for unregister)
AGENT_LIST = "agent_list"  # {req_id, agents}      [record-dict + "connected": bool]
# agent messaging (Phase B) — server -> relay (AGENT_OK acks leave/set_threshold/
# set_channel_topic; ERROR reports not-registered/unknown-name/invalid args).
CHANNEL_JOINED = (
    "channel_joined"  # {req_id, channel, members, topic, history: [msg...]}
)
POSTED = "posted"  # {req_id, id, ordinal, context, members}
CHANNEL_LIST = (
    "channel_list"  # {req_id, channels: [{name, topic, members, last_activity}]}
)
SUBSCRIPTION_LIST = (
    "subscription_list"  # {req_id, subscriptions: [{context, kind, threshold}]}
)
# receipts + reactions (Phase C) — server -> relay (AGENT_OK acks a react, carrying
# the reaction's own id; ERROR reports not-a-member / unknown message / invalid reaction).
MESSAGE_STATUS_RESULT = "message_status_result"  # {req_id, delivered: [name...], pending: [name...], reactions: [{by, reaction}]}
WHOAMI_RESULT = "whoami_result"  # {req_id, session_id}
ERROR = "error"  # {req_id, error}


def host() -> str:
    return os.environ.get("NOTIFICATIONS_WS_HOST", DEFAULT_HOST)


def port() -> int:
    raw = os.environ.get("NOTIFICATIONS_WS_PORT")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return DEFAULT_PORT


def uri() -> str:
    """The URL the relay connects to. A full ``NOTIFICATIONS_WS_URL`` override wins
    when set — e.g. ``wss://subspace.example.com`` for a remote server behind a
    TLS-terminating proxy (scheme/host/port/path all honored). Otherwise it is built
    from NOTIFICATIONS_WS_HOST/PORT."""
    return os.environ.get("NOTIFICATIONS_WS_URL") or f"ws://{host()}:{port()}"


def _data_dir() -> Path:
    base = os.environ.get("NOTIFICATIONS_DATA_DIR")
    return Path(base) if base else Path.home() / ".claude" / "notifications"


def token() -> str:
    """Shared secret authenticating relay->server WebSocket connections.

    Returns NOTIFICATIONS_TOKEN if set; otherwise reads (or atomically creates) a
    token file at <NOTIFICATIONS_DATA_DIR>/token (default ~/.claude/notifications),
    mode 0600. For a local server, scripts/migrate-from-notifications.py links that
    file to the server's token. Creation is racy-safe: whoever wins the O_EXCL create
    writes the secret, the other reads it (retrying briefly while the file is still
    empty).
    """
    env = os.environ.get("NOTIFICATIONS_TOKEN")
    if env:
        return env
    path = _data_dir() / "token"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return _read_token(path)
    try:
        value = secrets.token_urlsafe(32)
        os.write(fd, value.encode())
    finally:
        os.close(fd)
    return value


def _read_token(path: Path) -> str:
    # The O_EXCL create winner may not have written the secret yet; the file exists
    # but is momentarily empty. Retry briefly rather than return a blank token.
    for _ in range(100):
        try:
            value = path.read_text().strip()
        except OSError:
            value = ""
        if value:
            return value
        time.sleep(0.01)
    return path.read_text().strip()
