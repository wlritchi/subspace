#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""SessionStart hook for the subspace plugin.

Claude Code passes the real session id (as JSON on stdin) here on every
SessionStart, including `resume`. We record it in a per-instance state file so
the long-lived MCP server can pick up the correct id even when its env var holds
the stale temporary id from before a resume. Correlation is by the Claude Code
process both the hook and the server descend from.

The hook also tells the relay how the session starts. On a resume, the relay
keeps holding the messages that arrived while the session was stopped, and the
hook leaves a resume mark for the UserPromptSubmit hook (prompt-submit.py),
which claims them for the first turn. The hook does not claim them itself:
SessionStart context reaches the model only with a turn, and a resume with no
prompt starts no turn. The relay acks a claimed message, so a claim here would
lose it. For any other start, the hook declines and the relay ends its hold.
The hook talks to the relay, never to the server: a second server connection
would get every unacked message again.

Entirely best-effort: any failure is swallowed so a session never fails to start
because of us, and every wait is bounded.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import hook_ipc
import session_state

# The relay's socket appears when its MCP server starts, which can lag a slow
# `uv run`. A relay that starts later holds at start anyway (it finds a fresh
# state file), so the hook does not wait long for it.
NOTIFY_WAIT_SECONDS = 3.0


async def handoff(path: Path, session_id: str, source: str) -> bool:
    """Tell the relay that this start is a resume, or decline for any other
    start. Returns True if the relay got the message."""
    kind = "resume" if source == "resume" else "decline"
    deadline = time.monotonic() + NOTIFY_WAIT_SECONDS
    connection = await hook_ipc.connect(path, deadline)
    if connection is None:
        return False
    _reader, writer = connection
    try:
        await hook_ipc.send(
            writer, {"type": kind, "session_id": session_id, "source": source}
        )
        return True
    except OSError:
        return False
    finally:
        await hook_ipc.close(writer)


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError, OSError):
        return

    session_id = payload.get("session_id")
    if not session_id:
        return

    claude_pid = session_state.resolve_claude_pid()
    if claude_pid is None:
        return  # can't correlate to a server; nothing useful to do
    if session_state.secure_state_dir() is None:
        return  # another local user could serve a fake socket there

    source = payload.get("source")
    session_state.write_session(claude_pid, session_id, source)
    session_state.reap_stale()
    if source == "resume":
        session_state.mark_resume(claude_pid, session_id)
    else:
        session_state.clear_resume(claude_pid)
    asyncio.run(handoff(session_state.ipc_path(claude_pid), session_id, str(source)))


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001, S110
        # Never let a hook failure break session startup.
        pass
