# vim: filetype=python
"""Persistent store of scheduled callback notifications, keyed by session id.

Proof-of-concept persistence for scheduled callbacks. Each scheduled
callback is a small JSON file under a per-session directory, so the schedule
survives across restarts. The store is owned by the persistent subspace
server (see ../server/subspace-server.py); it schedules callbacks on behalf
of relays, delivers due ones over the WebSocket, and deletes them on ack.

The store location is fixed (not derived from CLAUDE_PLUGIN_DATA) so the server
finds the same files whether launched from a plugin context or from a bare
systemd --user unit. Override with SUBSPACE_DATA_DIR.

stdlib only.
"""

import json
import os
import re
import time
import uuid
from pathlib import Path

import settings

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def store_dir() -> Path:
    """Root directory for persisted callbacks (survives across sessions)."""
    return settings.data_dir() / "scheduled"


def _session_dir(session_id: str) -> Path:
    return store_dir() / _UNSAFE.sub("_", session_id)


def schedule(
    session_id: str,
    due_at: float,
    *,
    kind: str = "scheduled",
    content: str | None = None,
) -> str:
    """Persist a callback for `session_id` due at epoch `due_at`; return its id."""
    callback_id = uuid.uuid4().hex
    directory = _session_dir(session_id)
    directory.mkdir(parents=True, exist_ok=True)

    entry: dict[str, object] = {
        "id": callback_id,
        "session_id": session_id,
        "created_at": time.time(),
        "due_at": due_at,
        "kind": kind,
    }
    if content:
        entry["content"] = content

    path = directory / f"{callback_id}.json"
    tmp = path.with_name(f".{callback_id}.json.tmp")
    tmp.write_text(json.dumps(entry))
    os.replace(tmp, path)
    return callback_id


def pending(session_id: str) -> list[dict]:
    """All persisted callbacks for `session_id`, oldest file first."""
    directory = _session_dir(session_id)
    if not directory.is_dir():
        return []
    entries: list[dict] = []
    for path in sorted(directory.glob("*.json")):
        try:
            entries.append(json.loads(path.read_text()))
        except (OSError, ValueError):
            continue
    return entries


def due_callbacks(session_id: str, now: float) -> list[dict]:
    """Callbacks for `session_id` whose due time has passed."""
    return [e for e in pending(session_id) if float(e.get("due_at", 0)) <= now]


def delete(session_id: str, callback_id: str) -> None:
    """Remove a callback once acknowledged (callbacks are one-shot)."""
    if not session_id or not callback_id:
        return
    path = _session_dir(session_id) / f"{callback_id}.json"
    try:
        path.unlink()
    except OSError:
        pass


def mark_dispatched(entry: dict) -> None:
    """Remove a callback by entry dict."""
    delete(str(entry.get("session_id", "")), str(entry.get("id", "")))
