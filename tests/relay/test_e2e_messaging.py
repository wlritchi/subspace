# vim: filetype=python
"""End-to-end tests for agent messaging through the relay: the real server and
two or three channel (push) relay sessions. An @here clears B's default 'direct'
threshold and arrives as a channel event; a plain fyi is below it and is held
until B's catch_up drains it."""

import re
import time
from collections.abc import Callable
from itertools import count

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow


async def _register(read, write, ids, name: str) -> None:
    text, _ = await h.mcp_call(read, write, next(ids), "register_agent", {"name": name})
    assert f"Registered as '{name}'" in text


async def _join(read, write, ids, channel: str, **extra) -> str:
    text, _ = await h.mcp_call(
        read, write, next(ids), "join_channel", {"channel": channel, **extra}
    )
    assert f"Joined #{channel}" in text
    return text


async def _poll_call_until(
    read,
    write,
    ids,
    tool: str,
    predicate: Callable[[str], bool],
    *,
    arguments: dict | None = None,
    timeout: float = 20.0,
    interval: float = 0.2,
) -> str:
    """Poll a tool until `predicate` holds on its text, or until `timeout`."""
    deadline = time.monotonic() + timeout
    text = ""
    while True:
        result, _ = await h.mcp_try_call(read, write, next(ids), tool, arguments or {})
        if result is not None:
            text = result
            if predicate(text):
                return text
        if time.monotonic() >= deadline:
            return text
        await anyio.sleep(interval)


def test_post_surfaces_or_holds_by_threshold(tmp_path):
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                # An @here (severity=high) is urgent for everyone: it clears B's
                # 'direct' threshold and arrives as a channel event.
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "all hands", "severity": "high"},
                )
                assert "Posted to #room" in text
                event = await h.mcp_await_channel_with(read_b, "all hands", timeout=20)
                assert event is not None
                assert "agent-a" in event.params["content"]

                # A plain fyi is ambient, below B's 'direct' threshold: held.
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "fyi thing"},
                )
                assert "Posted to #room" in text
                assert (
                    await h.mcp_await_channel_with(read_b, "fyi thing", timeout=5)
                    is None
                )

                # catch_up drains the held message, and acks it.
                text, _ = await h.mcp_call(read_b, write_b, next(ids_b), "catch_up")
                assert "fyi thing" in text
                text, _ = await h.mcp_call(read_b, write_b, next(ids_b), "catch_up")
                assert "No pending notifications" in text

        anyio.run(scenario)


def test_dm_delivers_and_reuses_thread(tmp_path):
    """A DM is 'direct' for its recipient, so it clears B's default threshold and
    arrives as a `[dm] sender: body (→ you)` event. A second DM among the same
    participants, started by another member, reuses the one thread for that set."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
                stack.session("sid-C") as (read_c, write_c),
            ):
                ids_a, ids_b, ids_c = count(2), count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _register(read_c, write_c, ids_c, "agent-c")

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "dm",
                    {"to": ["agent-b"], "body": "ping one"},
                )
                assert "Sent DM to agent-b" in text
                event = await h.mcp_await_channel_with(read_b, "ping one", timeout=20)
                assert event is not None
                content = event.params["content"]
                assert "[dm] agent-a:" in content
                assert "(→ you)" in content

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "dm",
                    {"to": ["agent-b", "agent-c"], "body": "hello all"},
                )
                assert "Sent DM to" in text
                assert await h.mcp_await_channel_with(read_b, "hello all", timeout=20)
                assert await h.mcp_await_channel_with(read_c, "hello all", timeout=20)

                # A DM from B to {A, C} lands in that same thread.
                text, _ = await h.mcp_call(
                    read_b,
                    write_b,
                    next(ids_b),
                    "dm",
                    {"to": ["agent-a", "agent-c"], "body": "reply all"},
                )
                assert "Sent DM to" in text
                assert await h.mcp_await_channel_with(read_a, "reply all", timeout=20)
                assert await h.mcp_await_channel_with(read_c, "reply all", timeout=20)

                # agent-c is in one DM thread; agent-a in two.
                subs_c, _ = await h.mcp_call(
                    read_c, write_c, next(ids_c), "list_subscriptions"
                )
                assert subs_c.count("(dm)") == 1
                subs_a, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "list_subscriptions"
                )
                assert subs_a.count("(dm)") == 2

        anyio.run(scenario)


def test_mention_surfaces_only_for_mentioned(tmp_path):
    """A plain fyi that mentions agent-b is 'direct' for B and 'ambient' for C.
    B gets it with the `(→ you)` marker; C does not get it pushed, but C's
    catch_up drains it."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
                stack.session("sid-C") as (read_c, write_c),
            ):
                ids_a, ids_b, ids_c = count(2), count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _register(read_c, write_c, ids_c, "agent-c")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")
                await _join(read_c, write_c, ids_c, "room")

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "look here", "mentions": ["agent-b"]},
                )
                assert "Posted to #room" in text

                event = await h.mcp_await_channel_with(read_b, "look here", timeout=20)
                assert event is not None
                assert "(→ you)" in event.params["content"]

                assert (
                    await h.mcp_await_channel_with(read_c, "look here", timeout=5)
                    is None
                )
                drained = await _poll_call_until(
                    read_c, write_c, ids_c, "catch_up", lambda t: "look here" in t
                )
                assert "look here" in drained

        anyio.run(scenario)


def test_flush_on_clear_batches_held_backlog(tmp_path):
    """Two plain fyis held below B's threshold are flushed with the @here that
    clears it: one coalesced channel event carries all three bodies."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                for body in ("quiet one", "quiet two"):
                    text, _ = await h.mcp_call(
                        read_a,
                        write_a,
                        next(ids_a),
                        "post",
                        {"channel": "room", "body": body},
                    )
                    assert "Posted to #room" in text
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "all hands now", "severity": "high"},
                )
                assert "Posted to #room" in text

                event = await h.mcp_await_channel_with(
                    read_b, "all hands now", timeout=20
                )
                assert event is not None
                content = event.params["content"]
                assert "quiet one" in content
                assert "quiet two" in content
                assert "all hands now" in content

        anyio.run(scenario)


def test_history_tail_rendered_on_join(tmp_path):
    """Messages A posts before B joins #room are not delivered to B as wakes;
    the join reply shows them as recent history."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")

                for body in ("earlier one", "earlier two"):
                    await h.mcp_call(
                        read_a,
                        write_a,
                        next(ids_a),
                        "post",
                        {"channel": "room", "body": body},
                    )

                join_text = await _join(read_b, write_b, ids_b, "room")
                assert "Recent messages:" in join_text
                assert "earlier one" in join_text
                assert "earlier two" in join_text
                assert (
                    await h.mcp_await_channel_with(read_b, "earlier one", timeout=5)
                    is None
                )

        anyio.run(scenario)


def test_channel_topic_shown_in_list(tmp_path):
    """A topic set on the first join, then changed with set_channel_topic, shows
    in list_channels both times."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                ids_a = count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _join(read_a, write_a, ids_a, "room", topic="release planning")

                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "list_channels"
                )
                assert "#room" in text
                assert "release planning" in text

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "set_channel_topic",
                    {"channel": "room", "topic": "ship it friday"},
                )
                assert "ship it friday" in text

                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "list_channels"
                )
                assert "ship it friday" in text
                assert "release planning" not in text

        anyio.run(scenario)


def test_set_threshold_changes_surfacing(tmp_path):
    """A per-channel threshold applies at delivery time. With B raised to
    'urgent' on chan:room, a mention (normally 'direct') is held; lowered to
    'all', a plain fyi (normally 'ambient') is pushed."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                # set_threshold takes the context key that list_subscriptions
                # shows for the channel.
                subs, _ = await h.mcp_call(
                    read_b, write_b, next(ids_b), "list_subscriptions"
                )
                assert "chan:room" in subs
                text, _ = await h.mcp_call(
                    read_b,
                    write_b,
                    next(ids_b),
                    "set_threshold",
                    {"context": "chan:room", "threshold": "urgent"},
                )
                assert "set to 'urgent'" in text

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "hold me", "mentions": ["agent-b"]},
                )
                assert "Posted to #room" in text
                assert (
                    await h.mcp_await_channel_with(read_b, "hold me", timeout=5) is None
                )

                text, _ = await h.mcp_call(
                    read_b,
                    write_b,
                    next(ids_b),
                    "set_threshold",
                    {"context": "chan:room", "threshold": "all"},
                )
                assert "set to 'all'" in text
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "surface me"},
                )
                assert "Posted to #room" in text
                assert await h.mcp_await_channel_with(read_b, "surface me", timeout=20)

        anyio.run(scenario)


def test_reaction_is_ambient_and_visible_in_status(tmp_path):
    """A reaction is 'ambient': it never wakes the author, but it is recorded.
    A sees agent-b's 👍 in message_status and, against the original message, in
    catch_up. The first post to a new #room is msg:chan:room:0."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "hello"},
                )
                assert "Posted to #room" in text
                hello_id = "msg:chan:room:0"

                text, _ = await h.mcp_call(
                    read_b,
                    write_b,
                    next(ids_b),
                    "react",
                    {"message_id": hello_id, "reaction": "👍"},
                )
                assert "👍" in text and hello_id in text

                assert (
                    await h.mcp_await_channel_with(read_a, "reacted", timeout=5) is None
                )

                status, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "message_status",
                    {"message_id": hello_id},
                )
                assert "👍" in status and "agent-b" in status

                drained = await _poll_call_until(
                    read_a, write_a, ids_a, "catch_up", lambda t: "reacted" in t
                )
                assert "👍" in drained
                assert "agent-b reacted" in drained
                assert 'agent-a\'s "hello"' in drained

        anyio.run(scenario)


def test_message_status_reflects_gating(tmp_path):
    """A plain fyi held below B's threshold reads as pending until B drains it,
    then as delivered. The author is not counted."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "fyi thing"},
                )
                assert "Posted to #room" in text
                msg_id = "msg:chan:room:0"

                status, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "message_status",
                    {"message_id": msg_id},
                )
                assert "pending: agent-b" in status
                assert "agent-a" not in status

                drained = await _poll_call_until(
                    read_b, write_b, ids_b, "catch_up", lambda t: "fyi thing" in t
                )
                assert "fyi thing" in drained

                status = await _poll_call_until(
                    read_a,
                    write_a,
                    ids_a,
                    "message_status",
                    lambda t: "Delivered to 1 of 1" in t,
                    arguments={"message_id": msg_id},
                )
                assert "Delivered to 1 of 1" in status
                assert "agent-b" in status

        anyio.run(scenario)


def test_global_handle_drives_react_and_status(tmp_path):
    """The post reply carries the message's #N handle, and B's channel event
    starts with the same #N. B reacts by handle, and A reads the reaction back
    by handle and by the full msg: id."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "ship it", "severity": "high"},
                )
                assert "Posted to #room" in text
                match = re.search(r"\(#(\d+)\)", text)
                assert match is not None, text
                handle = match.group(1)

                event = await h.mcp_await_channel_with(read_b, "ship it", timeout=20)
                assert event is not None
                content = event.params["content"]
                assert content.lstrip().startswith(f"#{handle} "), content
                assert f"#{handle} [#room] agent-a: ship it" in content

                react_text, _ = await h.mcp_call(
                    read_b,
                    write_b,
                    next(ids_b),
                    "react",
                    {"message_id": f"#{handle}", "reaction": "👍"},
                )
                assert "Could not react" not in react_text
                assert "👍" in react_text

                status, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "message_status",
                    {"message_id": f"#{handle}"},
                )
                assert "👍" in status and "agent-b" in status

                status_by_id, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "message_status",
                    {"message_id": "msg:chan:room:0"},
                )
                assert "👍" in status_by_id and "agent-b" in status_by_id

        anyio.run(scenario)


def test_invalid_reaction_rejected(tmp_path):
    """An empty or multi-line reaction is refused by the relay and never reaches
    the server: message_status shows no reactions."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with (
                stack.session("sid-A") as (read_a, write_a),
                stack.session("sid-B") as (read_b, write_b),
            ):
                ids_a, ids_b = count(2), count(2)
                await _register(read_a, write_a, ids_a, "agent-a")
                await _register(read_b, write_b, ids_b, "agent-b")
                await _join(read_a, write_a, ids_a, "room")
                await _join(read_b, write_b, ids_b, "room")

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "post",
                    {"channel": "room", "body": "react to me"},
                )
                assert "Posted to #room" in text
                msg_id = "msg:chan:room:0"

                for bad in ("", "bad\nreaction"):
                    text, _ = await h.mcp_call(
                        read_b,
                        write_b,
                        next(ids_b),
                        "react",
                        {"message_id": msg_id, "reaction": bad},
                    )
                    assert "Could not react" in text

                status, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "message_status",
                    {"message_id": msg_id},
                )
                assert "reactions:" not in status

        anyio.run(scenario)


def test_message_status_annotates_offline_recipient(tmp_path):
    """message_status marks a recipient with no connection '(offline, last seen
    ...)', so that 'pending' is not read as 'a running session has it'."""
    with h.core(tmp_path) as stack:

        async def scenario():
            async with stack.session("sid-s") as (read_s, write_s):
                ids_s = count(2)
                await _register(read_s, write_s, ids_s, "sender")
                async with stack.session("sid-t") as (read_t, write_t):
                    await _register(read_t, write_t, count(2), "target")
                # target's session is gone; DM its name and read the receipt.
                text, _ = await h.mcp_call(
                    read_s,
                    write_s,
                    next(ids_s),
                    "dm",
                    {"to": ["target"], "body": "anyone home?"},
                )
                assert "Sent DM to target" in text
                status = await _poll_call_until(
                    read_s,
                    write_s,
                    ids_s,
                    "message_status",
                    lambda t: "(offline" in t,
                    arguments={"message_id": "#1"},
                )
                assert "pending: target (offline" in status

        anyio.run(scenario)
