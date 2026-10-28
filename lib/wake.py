# vim: filetype=python
"""Webhook wake-up: when the core may wake an offline session, and what it
sends. See "Webhook wake-up" in docs/specs/2026-10-10-remote-agent-mcp.md and
docs/specs/2026-10-28-launcher-wake.md.

The clock and the session state are arguments, so the rules are testable
without a server. stdlib only.
"""

import json

# Delays before the second, third, and fourth attempt.
RETRY_DELAYS = (5.0, 30.0, 120.0)
# The body lists at most this many messages.
MAX_ITEMS = 10


def due_at(
    *,
    live: bool,
    has_new: bool,
    last_seen: float,
    sent_at: float,
    grace: float,
    cooldown: float,
    started_at: float = 0.0,
) -> float | None:
    """The earliest time at which the core may send a wake, or None if it must
    not send one.

    ``has_new`` is True if the session has a message that the wake rules
    count and that no earlier wake covered. ``last_seen`` is when the session
    last had a connection; ``sent_at`` is when the last wake was sent, or 0.
    The grace time counts from ``started_at`` (when the core started) at the
    earliest: after a restart, clients need time to connect again.
    """
    if live or not has_new:
        return None
    due = max(last_seen, started_at) + grace
    if sent_at > 0 and sent_at >= last_seen:
        # The session did not connect after the last wake.
        due = max(due, sent_at + cooldown)
    return due


def retryable(status: int | None) -> bool:
    """True if a wake that got ``status`` (None: no response) is tried again."""
    return status is None or status == 429 or status >= 500


# The log keeps at most this many characters of a response field.
MAX_NOTE = 200


def body(account: str, session: str, handle: str, items: list[dict]) -> dict:
    """The JSON body of a wake. ``session`` is the session id, ``handle`` the
    session's current handle. ``items`` describe the messages, oldest first."""
    return {
        "event": "wake",
        "account": account,
        "session": session,
        "handle": handle,
        "pending": len(items),
        "items": items[:MAX_ITEMS],
    }


def response_note(content: bytes) -> str:
    """The ``verdict``, ``detail`` or ``message`` of a JSON response body, for
    the log, or "" if the body has none. The receiver is not trusted: the note
    is quoted, so it cannot add lines to the log."""
    try:
        data = json.loads(content)
    except (ValueError, RecursionError):
        return ""
    if not isinstance(data, dict):
        return ""
    for name in ("verdict", "detail", "message"):
        value = data.get(name)
        if isinstance(value, str) and value:
            return f"{name} {value[:MAX_NOTE]!r}"
    return ""
