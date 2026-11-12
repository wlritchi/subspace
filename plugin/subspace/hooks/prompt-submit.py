#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""UserPromptSubmit hook for the subspace plugin.

On the first prompt after a resume, the hook claims from the relay the messages
that arrived while the session was stopped, and returns them as
`additionalContext`, so the agent has them in this turn. The relay holds them
back from channel delivery until the hook claims them or the hold ends; the
relay acks them only after the hook has written its output. A resume with no
prompt claims nothing: when the hold ends, the messages go out as channel
notifications, which start a turn.

The hook runs on every prompt. It contacts the relay only if the SessionStart
hook left a resume mark, and it takes the mark away, so the other prompts cost
only a file check.

Entirely best-effort: any failure is swallowed so a prompt is never refused
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
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import hook_ipc
import session_state

# The claim can wait for the relay to connect to the server and for whoami.
# This blocks the first prompt after a resume, and no other prompt.
CLAIM_WAIT_SECONDS = 10.0
# The relay's socket appears when its MCP server starts, which can lag a slow
# `uv run`. Later than this after the resume, the relay is not coming, so the
# hook tries to connect only once.
RELAY_START_SECONDS = 10.0
CONTEXT_HEADER = (
    "Agent messages arrived while this session was stopped. They are delivered "
    "here once, with the first prompt after the resume, in place of channel "
    "notifications:\n\n"
)


def build_output(content: str) -> dict:
    """The UserPromptSubmit hook JSON that puts `content` into this turn."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": CONTEXT_HEADER + content,
        }
    }


async def claim(
    path: Path,
    session_id: str,
    emit: Callable[[str], None],
    connect_seconds: float = RELAY_START_SECONDS,
) -> bool:
    """Claim the held messages of `session_id` from the relay.

    `emit` writes the hook output; it is called at most once, before the relay
    is told "done" and acks the messages. Returns True if messages were emitted.
    """
    start = time.monotonic()
    deadline = start + CLAIM_WAIT_SECONDS
    connection = await hook_ipc.connect(
        path, min(deadline, start + max(connect_seconds, 0.0))
    )
    if connection is None:
        return False
    reader, writer = connection
    try:
        await hook_ipc.send(
            writer, {"type": "claim", "session_id": session_id, "source": "prompt"}
        )
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
        await hook_ipc.send(writer, {"type": "done"})
        return True
    except (OSError, TimeoutError, ValueError):
        return False
    finally:
        await hook_ipc.close(writer)


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
        return
    mark = session_state.take_resume(claude_pid)
    if mark is None:
        return  # no resume waits for its first prompt
    resumed_session, age = mark
    if resumed_session != session_id:
        return  # a mark of an earlier session in this process
    asyncio.run(
        claim(
            session_state.ipc_path(claude_pid),
            session_id,
            _emit,
            connect_seconds=RELAY_START_SECONDS - age,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001, S110
        # Never let a hook failure block the prompt.
        pass
