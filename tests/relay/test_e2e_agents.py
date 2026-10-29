# vim: filetype=python
"""End-to-end tests for the agent directory through the relay: the real server
and two relay sessions. One session registers and a second, independent session
sees it through list_agents, with its presence and profile."""

import time
from collections.abc import Callable

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow


async def _list_until(
    read,
    write,
    start_id: int,
    predicate: Callable[[str], bool],
    *,
    timeout: float = 20.0,
    interval: float = 0.1,
) -> tuple[str, int]:
    """Poll list_agents until `predicate(text)` holds, or until `timeout`.
    Presence changes after the server processes a disconnect, so a fixed sleep
    is not enough. Returns the last text and the next free request id."""
    deadline = time.monotonic() + timeout
    req_id = start_id
    text = ""
    while True:
        result, _ = await h.mcp_try_call(read, write, req_id, "list_agents", {})
        req_id += 1
        if result is not None:
            text = result
            if predicate(text):
                return text, req_id
        if time.monotonic() >= deadline:
            return text, req_id
        await anyio.sleep(interval)


def test_register_then_listed_as_connected(tmp_path):
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    2,
                    "register_agent",
                    {"name": "agent-a", "description": "does A things"},
                )
                assert "Registered as 'agent-a'" in text
                assert "direct" in text  # the default wake threshold

                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(read_b, write_b, 2, "list_agents", {})
                    assert "agent-a" in text
                    assert "connected" in text
                    assert "does A things" in text

        anyio.run(scenario)


def test_two_agent_directory(tmp_path):
    """Two live sessions each register; either one's list_agents shows both,
    both connected, each with its own profile."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    2,
                    "register_agent",
                    {"name": "agent-a", "description": "does A things"},
                )
                assert "Registered as 'agent-a'" in text

                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(
                        read_b,
                        write_b,
                        2,
                        "register_agent",
                        {"name": "agent-b", "description": "does B things"},
                    )
                    assert "Registered as 'agent-b'" in text

                    text_a, _ = await h.mcp_call(read_a, write_a, 3, "list_agents", {})
                    text_b, _ = await h.mcp_call(read_b, write_b, 3, "list_agents", {})
                    for text in (text_a, text_b):
                        assert "agent-a" in text
                        assert "agent-b" in text
                        assert "does A things" in text
                        assert "does B things" in text
                        assert "offline" not in text
                        assert text.count("connected") == 2

        anyio.run(scenario)


def test_collision_rejected_across_live_sessions(tmp_path):
    """B cannot take A's live name: the register fails with 'already taken' and
    B stays unregistered. B can then take a free name."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "register_agent", {"name": "agent-a"}
                )
                assert "Registered as 'agent-a'" in text

                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(
                        read_b, write_b, 2, "register_agent", {"name": "agent-a"}
                    )
                    assert "Could not register as 'agent-a'" in text
                    assert "already taken" in text

                    # One agent-a: one presence line, marked by its separator.
                    text, _ = await h.mcp_call(read_b, write_b, 3, "list_agents", {})
                    assert "agent-a" in text
                    assert text.count(" — ") == 1

                    text, _ = await h.mcp_call(
                        read_b, write_b, 4, "register_agent", {"name": "agent-b"}
                    )
                    assert "Registered as 'agent-b'" in text

                    text, _ = await h.mcp_call(read_b, write_b, 5, "list_agents", {})
                    assert "agent-a" in text
                    assert "agent-b" in text
                    assert text.count(" — ") == 2

        anyio.run(scenario)


def test_set_availability_reflected_in_list(tmp_path):
    """After set_availability('urgent'), list_agents shows the threshold as
    urgent, and a re-register without default_threshold keeps it."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "register_agent", {"name": "agent-a"}
                )
                assert "Registered as 'agent-a'" in text
                assert "direct" in text

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    3,
                    "set_availability",
                    {"default_threshold": "urgent"},
                )
                assert "urgent" in text

                text, _ = await h.mcp_call(read_a, write_a, 4, "list_agents", {})
                assert "wake threshold: urgent" in text

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    5,
                    "register_agent",
                    {"name": "agent-a", "description": "updated"},
                )
                assert "Registered as 'agent-a'" in text
                assert "wake threshold: urgent" in text

                text, _ = await h.mcp_call(read_a, write_a, 6, "list_agents", {})
                assert "updated" in text
                assert "wake threshold: urgent" in text
                assert "wake threshold: direct" not in text

        anyio.run(scenario)


def test_disconnect_marks_offline(tmp_path):
    """Once A's relay session closes, list_agents from B shows agent-a offline."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "register_agent", {"name": "agent-a"}
                )
                assert "Registered as 'agent-a'" in text

            async with stack.session("sid-B") as (read_b, write_b):
                text, _ = await _list_until(
                    read_b, write_b, 2, lambda t: "offline" in t
                )
                assert "agent-a" in text
                assert "offline" in text

        anyio.run(scenario)


def test_unregister_removes_from_directory(tmp_path):
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "register_agent", {"name": "agent-a"}
                )
                assert "Registered as 'agent-a'" in text

                text, _ = await h.mcp_call(read_a, write_a, 3, "unregister_agent", {})
                assert "Unregistered 'agent-a'" in text

                text, _ = await h.mcp_call(read_a, write_a, 4, "list_agents", {})
                assert "agent-a" not in text
                assert "none registered" in text

        anyio.run(scenario)


def test_superseded_session_told_to_stand_down(tmp_path):
    """The relay keeps (name, session_id) in its identity file on registration.
    When a later register fails with 'already taken' and the file shows that
    this session held the name, the agent gets a SUPERSEDED verdict instead of
    a plain failure. With the handle idle time at 0, another session can take
    the name as soon as its holder disconnects."""
    with h.core(tmp_path, handle_idle="0") as stack:

        async def scenario():
            async with stack.session("sid-old") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "register_agent", {"name": "worker"}
                )
                assert "Registered as 'worker'" in text
            identity = (stack.xdg / "agent-identity-sid-old.json").read_text()
            assert '"worker"' in identity and '"sid-old"' in identity

            async with stack.session("sid-new") as (read_b, write_b):
                text, next_id = await _list_until(
                    read_b, write_b, 2, lambda t: "offline" in t
                )
                text, _ = await h.mcp_call(
                    read_b, write_b, next_id, "register_agent", {"name": "worker"}
                )
                assert "Registered as 'worker'" in text

                # The displaced session comes back while the successor is live
                # and tries to take its name back.
                async with stack.session("sid-old") as (read_back, write_back):
                    text, _ = await h.mcp_call(
                        read_back, write_back, 2, "register_agent", {"name": "worker"}
                    )
                    assert "SUPERSEDED" in text
                    assert "dm 'worker'" in text

        anyio.run(scenario)
