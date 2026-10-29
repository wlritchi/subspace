# vim: filetype=python
"""End-to-end tests for delivery at resume: the real server, a relay that runs
under a stand-in for the Claude Code process, and the SessionStart hook's
handoff code. A session that was away gets the messages that passed its
threshold in the hook's output once, and not as a channel event; a start that
is not a resume declines, and the messages reach the channel as before."""

import importlib.util
import json
import shutil
import sys
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
import pytest
from mcp.client.stdio import StdioServerParameters

import _harness as h
import session_state

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform != "linux",
        reason="the stand-in Claude Code process is found by /proc",
    ),
]


def _load_hook():
    path = h.ROOT / "plugin" / "subspace" / "hooks" / "session-start.py"
    spec = importlib.util.spec_from_file_location("session_start", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


start = _load_hook()


@pytest.fixture
def short_dir():
    """The relay's XDG directory. A short path: Unix socket paths are limited to
    about 100 bytes, and pytest's tmp_path is often longer."""
    directory = Path(tempfile.mkdtemp(prefix="nt-"))
    yield directory
    shutil.rmtree(directory, ignore_errors=True)


def _stand_in_claude(directory: Path) -> Path:
    """A shell process named `claude` that runs the relay, so that the relay
    finds it as its Claude Code ancestor and keys the hook socket by its pid."""
    script = directory / "claude"
    script.write_text('#!/bin/sh\necho $$ > "$(dirname "$0")/claude.pid"\n"$@"\n')
    script.chmod(0o755)
    return script


async def _wait_for_file(path: Path, timeout: float = 30.0) -> None:
    with anyio.fail_after(timeout):
        while not path.exists() or (path.suffix == ".pid" and not path.read_text()):
            await anyio.sleep(0.05)


@asynccontextmanager
async def _resumed_session(
    stack: h.Stack,
    short_dir: Path,
    session_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple]:
    """A push-mode relay for `session_id` under the stand-in Claude Code
    process. Yields its MCP streams and the hook socket path. This process takes
    the relay's state directory too, so that the hook code here finds the socket
    where the relay made it."""
    wrapper = _stand_in_claude(short_dir)
    env = stack.push_env(session_id, xdg_dir=short_dir)
    monkeypatch.setenv("XDG_RUNTIME_DIR", env["XDG_RUNTIME_DIR"])
    params = StdioServerParameters(
        command=str(wrapper),
        args=["uv", "run", "-qs", "--python", h._PYTHON, h.RELAY],
        env=env,
    )
    async with h.stdio_client(params) as (read, write):
        await h.mcp_handshake(read, write)
        await _wait_for_file(short_dir / "claude.pid")
        claude_pid = int((short_dir / "claude.pid").read_text())
        socket = session_state.ipc_path(claude_pid)
        yield read, write, socket


async def _setup_room(stack: h.Stack) -> None:
    """agent-a and agent-b join #room; then B goes away and A posts an urgent
    message (passes B's 'direct' threshold) and a quiet one (held)."""
    async with stack.session("sid-B") as (read, write):
        text, _ = await h.mcp_call(
            read, write, 2, "register_agent", {"name": "agent-b"}
        )
        assert "Registered as 'agent-b'" in text
        text, _ = await h.mcp_call(read, write, 3, "join_channel", {"channel": "room"})
        assert "Joined #room" in text
    async with stack.session("sid-A") as (read, write):
        text, _ = await h.mcp_call(
            read, write, 2, "register_agent", {"name": "agent-a"}
        )
        assert "Registered as 'agent-a'" in text
        text, _ = await h.mcp_call(read, write, 3, "join_channel", {"channel": "room"})
        assert "Joined #room" in text
        text, _ = await h.mcp_call(
            read,
            write,
            4,
            "post",
            {"channel": "room", "body": "all hands", "severity": "high"},
        )
        assert "Posted to #room" in text
        text, _ = await h.mcp_call(
            read, write, 5, "post", {"channel": "room", "body": "fyi thing"}
        )
        assert "Posted to #room" in text


def test_resume_puts_held_messages_into_the_first_turn(
    tmp_path, short_dir, monkeypatch
):
    with h.core(tmp_path) as stack:

        async def scenario():
            await _setup_room(stack)
            emitted: list[str] = []
            async with _resumed_session(stack, short_dir, "sid-B", monkeypatch) as (
                read,
                write,
                socket,
            ):
                # As the hook does: the state file first, then the claim.
                session_state.write_session(int(socket.stem), "sid-B", "resume")
                assert await start.handoff(socket, "sid-B", "resume", emitted.append)
                output = json.loads(emitted[0])
                context = output["hookSpecificOutput"]["additionalContext"]
                assert "all hands" in context and "agent-a" in context
                assert "fyi thing" not in context  # below the threshold: held

                # The claimed message does not also arrive as a channel event.
                assert (
                    await h.mcp_await_channel_with(read, "all hands", timeout=4) is None
                )

                # The quiet one is still there for catch_up; the claimed one is
                # acked.
                text, _ = await h.mcp_call(read, write, 2, "catch_up")
                assert "fyi thing" in text
                assert "all hands" not in text

        anyio.run(scenario)


def test_start_that_is_not_a_resume_declines(tmp_path, short_dir, monkeypatch):
    with h.core(tmp_path) as stack:

        async def scenario():
            await _setup_room(stack)
            emitted: list[str] = []
            async with _resumed_session(stack, short_dir, "sid-B", monkeypatch) as (
                read,
                _write,
                socket,
            ):
                assert not await start.handoff(
                    socket, "sid-B", "startup", emitted.append
                )
                assert emitted == []
                # The decline ends the hold: the urgent message reaches the
                # channel well before the hold would end on its own.
                event = await h.mcp_await_channel_with(read, "all hands", timeout=10)
                assert event is not None
                assert "fyi thing" not in event.params["content"]

        anyio.run(scenario)
