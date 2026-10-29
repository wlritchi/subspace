# vim: filetype=python
"""End-to-end tests for the relay's connection to the server: the scheduled
callback path, reconnection, a refused token, and a server restart.

These spawn the real server and relay with `uv run` and drive the relay with a
raw MCP client over stdio."""

import time

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow

EXPECTED_TOOLS = [
    "catch_up",
    "dm",
    "get_session_id",
    "join_channel",
    "leave_channel",
    "list_agents",
    "list_channels",
    "list_forgejo_pr_subscriptions",
    "list_github_pr_subscriptions",
    "list_scheduled_notifications",
    "list_subscriptions",
    "message_status",
    "post",
    "react",
    "refresh_forgejo_pr",
    "refresh_github_pr",
    "register_agent",
    "schedule_test_notification",
    "set_availability",
    "set_channel_topic",
    "set_threshold",
    "subscribe_forgejo_pr",
    "subscribe_github_pr",
    "unregister_agent",
    "unsubscribe_forgejo_pr",
    "unsubscribe_github_pr",
]


async def _wait_connected(read, write, first_id: int, timeout: float = 40.0) -> int:
    """Poll get_session_id until the relay reports a connection. Returns the next
    free request id."""
    request_id = first_id
    deadline = time.time() + timeout
    while time.time() < deadline:
        text, _ = await h.mcp_call(read, write, request_id, "get_session_id")
        request_id += 1
        if "server: connected" in text:
            return request_id
        await anyio.sleep(1)
    raise AssertionError("the relay did not connect to the server")


async def _schedule_and_receive(read, write, request_id: int, session_id: str) -> None:
    text, channels = await h.mcp_call(
        read, write, request_id, "schedule_test_notification", {"delay_seconds": 2}
    )
    assert "Scheduled callback" in text
    event = channels[0] if channels else await h.mcp_await_channel(read)
    assert event is not None and f"session_id={session_id}" in event.params["content"]


def test_schedule_deliver_and_ack(tmp_path):
    with h.core(tmp_path) as stack:

        async def scenario():
            async with h.stdio_client(h.relay_params(stack.push_env("sid-A"))) as (
                read,
                write,
            ):
                caps = await h.mcp_handshake(read, write)
                assert caps.get("experimental", {}).get("claude/channel") == {}
                assert await h.mcp_list_tools(read, write, 2) == EXPECTED_TOOLS
                await _schedule_and_receive(read, write, 3, "sid-A")
                # Delivered and acked: the server has nothing left for the session.
                request_id = 4
                deadline = time.time() + 10
                while True:
                    text, _ = await h.mcp_call(
                        read, write, request_id, "list_scheduled_notifications"
                    )
                    request_id += 1
                    if "No scheduled notifications" in text or time.time() > deadline:
                        break
                    await anyio.sleep(0.3)
                assert "No scheduled notifications" in text

        anyio.run(scenario)


def test_reconnects_when_server_appears(tmp_path):
    stack = h.Stack(tmp_path, h.free_port(), tmp_path / "store")
    stack.data_dir.mkdir()

    async def scenario():
        async with h.stdio_client(h.relay_params(stack.push_env("sid-A"))) as (
            read,
            write,
        ):
            await h.mcp_handshake(read, write)
            text, _ = await h.mcp_call(read, write, 2, "get_session_id")
            assert "server: disconnected" in text  # no server yet

            with h.core_process(h.core_env(stack.ws_port, stack.data_dir)):
                request_id = await _wait_connected(read, write, 3)
                await _schedule_and_receive(read, write, request_id, "sid-A")

    anyio.run(scenario)


def test_wrong_token_leaves_relay_running(tmp_path):
    """A relay the server refuses stays up and reports the state; a tool call
    still gets an answer."""
    with h.core(tmp_path) as stack:
        env = stack.push_env("sid-A")
        env["NOTIFICATIONS_TOKEN"] = "wrong"

        async def scenario():
            async with h.stdio_client(h.relay_params(env)) as (read, write):
                await h.mcp_handshake(read, write)
                await anyio.sleep(2)  # time for a connect attempt and its refusal
                text, _ = await h.mcp_call(read, write, 2, "get_session_id")
                assert "server: disconnected" in text
                text, _ = await h.mcp_call(read, write, 3, "list_agents")
                assert "not reachable" in text

        anyio.run(scenario)


def test_relay_reconnects_after_server_restart(tmp_path):
    """A SIGTERM to the server closes the relay's connection cleanly; the relay
    reconnects when the server is back and delivers again."""
    stack = h.Stack(tmp_path, h.free_port(), tmp_path / "store")
    stack.data_dir.mkdir()
    env = h.core_env(stack.ws_port, stack.data_dir)

    async def scenario():
        async with h.stdio_client(h.relay_params(stack.push_env("sid-A"))) as (
            read,
            write,
        ):
            await h.mcp_handshake(read, write)
            with h.core_process(env):
                request_id = await _wait_connected(read, write, 2)
            # The context manager sent SIGTERM and waited for the exit.
            with h.core_process(env):
                request_id = await _wait_connected(read, write, request_id)
                await _schedule_and_receive(read, write, request_id, "sid-A")

    anyio.run(scenario)
