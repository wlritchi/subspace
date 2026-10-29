#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""SessionStart hook for the subspace plugin.

Claude Code passes the real session id (as JSON on stdin) here on every
SessionStart, including `resume`. We record it in a per-instance state file so
the long-lived MCP server can pick up the correct id even when its env var holds
the stale temporary id from before a resume. Correlation is by the Claude Code
process both the hook and the server descend from.

On a resume, the hook also claims from the relay the messages that arrived while
the session was stopped, and returns them as `additionalContext`, so the agent
has them in its first turn. The relay holds them back from channel delivery
until the hook claims or declines; the relay acks them only after the hook has
written its output. The hook talks to the relay, never to the server: a second
server connection would get every unacked message again.

Entirely best-effort: any failure is swallowed so a session never fails to start
because of us, and every wait is bounded.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import asyncio
import contextlib
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import session_state

# The relay's socket appears when its MCP server starts, which can lag a slow
# `uv run`. A claim waits through the relay's startup grace and the server's
# whoami; a decline only needs a relay that is already up.
CLAIM_WAIT_SECONDS = 10.0
DECLINE_WAIT_SECONDS = 3.0
CONNECT_POLL_SECONDS = 0.1
CONTEXT_HEADER = (
    "Agent messages arrived while this session was stopped. They are delivered "
    "here once, at resume, in place of channel notifications:\n\n"
)


def build_output(content: str) -> dict:
    """The SessionStart hook JSON that puts `content` into the first turn."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "SessionStart",
            "additionalContext": CONTEXT_HEADER + content,
        }
    }


async def _connect(
    path: Path, deadline: float
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
    """Connect to the relay's socket, trying again until `deadline`."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            return await asyncio.wait_for(
                asyncio.open_unix_connection(str(path)), timeout=remaining
            )
        except (OSError, TimeoutError):
            await asyncio.sleep(CONNECT_POLL_SECONDS)


async def _send(writer: asyncio.StreamWriter, message: dict) -> None:
    writer.write(json.dumps(message).encode() + b"\n")
    await writer.drain()


async def handoff(
    path: Path, session_id: str, source: str, emit: Callable[[str], None]
) -> bool:
    """Claim the held messages for a resume (or decline for any other start).

    `emit` writes the hook output; it is called at most once, before the relay
    is told "done" and acks the messages. Returns True if messages were emitted.
    """
    claim = source == "resume"
    wait = CLAIM_WAIT_SECONDS if claim else DECLINE_WAIT_SECONDS
    deadline = time.monotonic() + wait
    connection = await _connect(path, deadline)
    if connection is None:
        return False
    reader, writer = connection
    try:
        await _send(
            writer,
            {
                "type": "claim" if claim else "decline",
                "session_id": session_id,
                "source": source,
            },
        )
        if not claim:
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        raw = await asyncio.wait_for(reader.readline(), timeout=remaining)
        reply = json.loads(raw)
        if not isinstance(reply, dict) or reply.get("type") != "claimed":
            return False
        content = reply.get("content")
        if not isinstance(content, str) or not content:
            return False
        emit(json.dumps(build_output(content)))
        await _send(writer, {"type": "done"})
        return True
    except (OSError, TimeoutError, ValueError):
        return False
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()


def _emit(text: str) -> None:
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


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
    asyncio.run(
        handoff(session_state.ipc_path(claude_pid), session_id, str(source), _emit)
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001, S110
        # Never let a hook failure break session startup.
        pass
