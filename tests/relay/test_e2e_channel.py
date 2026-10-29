# vim: filetype=python
"""End-to-end test for pull mode: a relay that finds it was not loaded as a
channel buffers notifications instead of pushing them, offers catch_up, and acks
only on catch_up."""

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow


def test_pull_mode_buffers_then_catch_up(tmp_path):
    with h.core(tmp_path) as stack:

        async def scenario():
            async with h.stdio_client(h.relay_params(stack.pull_env("sid-A"))) as (
                read,
                write,
            ):
                await h.mcp_handshake(read, write)

                # Schedule a callback. In pull mode the relay must not push it.
                text, channels = await h.mcp_call(
                    read, write, 2, "schedule_test_notification", {"delay_seconds": 2}
                )
                assert "Scheduled callback" in text
                assert not channels
                assert await h.mcp_await_channel(read, timeout=8) is None

                assert "catch_up" in await h.mcp_list_tools(read, write, 3)

                # get_session_id reports the inactive channel and the fallback.
                text, _ = await h.mcp_call(read, write, 4, "get_session_id")
                assert "catch_up to pull" in text

                # catch_up returns the buffered notification, acks it, and says
                # why push is off, with the skip reason from the log.
                text, _ = await h.mcp_call(read, write, 5, "catch_up")
                assert "Pending notifications" in text and "sid-A" in text
                assert "Live push is disabled" in text
                assert "not in --channels list" in text

                # A second catch_up is empty, with the same note.
                text, _ = await h.mcp_call(read, write, 6, "catch_up")
                assert "No pending notifications" in text
                assert "Live push is disabled" in text

        anyio.run(scenario)
