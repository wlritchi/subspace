# vim: filetype=python
"""End-to-end tests for GitHub PR monitoring through the relay: the real server,
the relay, and a fake GraphQL endpoint. Subscribe, not found, live updates as
channel events, acks, no replay for a new subscriber, merge auto-unsubscribe,
an on-demand refresh, the pending count in pull mode, and the server's
synthetic error events."""

from itertools import count

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow

NUMBER = 7
KEY = f"octo/demo#{NUMBER}"


def base_pr(number: int = NUMBER) -> dict:
    key = f"octo/demo#{number}"
    return {
        "title": "Add feature",
        "url": f"https://gh/{key}",
        "state": "OPEN",
        "merged": False,
        "isDraft": False,
        "headRefOid": "a" * 40,
        "mergedBy": None,
        "mergeable": "MERGEABLE",
        "labels": {"nodes": []},
        "reviewRequests": {"nodes": []},
        "reviews": {"nodes": []},
        "reviewThreads": {"nodes": []},
        "comments": {"nodes": []},
        "commits": {
            "nodes": [{"commit": {"oid": "a" * 40, "statusCheckRollup": None}}]
        },
        "timelineItems": {"nodes": []},
    }


def _comment(login: str, body: str) -> dict:
    return {
        "nodes": [
            {
                "id": f"IC-{login}",
                "author": {"login": login},
                "body": body,
                "url": f"https://gh/ic/{login}",
            }
        ]
    }


async def _subscriptions_until(read, write, ids, needle: str) -> str:
    """Poll list_github_pr_subscriptions until its text contains `needle`."""
    return await h.poll_call_until(
        read, write, ids, "list_github_pr_subscriptions", lambda t: needle in t
    )


def test_pr_monitoring_full_chain(tmp_path):
    with (
        h.FakeGitHub(NUMBER, base_pr()) as gh,
        h.core(tmp_path, graphql_url=gh.graphql_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                ids_a = count(2)
                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "subscribe_github_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text and "open" in text

                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "subscribe_github_pr",
                    {"pr": "octo/demo#999"},
                )
                assert "Could not subscribe" in text and "not found" in text

                # A failing check, a changes-requested review, and a conflict.
                gh.pr["mergeable"] = "CONFLICTING"
                gh.pr["reviews"] = {
                    "nodes": [
                        {
                            "id": "R1",
                            "state": "CHANGES_REQUESTED",
                            "author": {"login": "alice"},
                            "body": "needs work on error handling",
                            "url": "https://gh/r/11",
                        }
                    ]
                }
                gh.pr["commits"] = {
                    "nodes": [
                        {
                            "commit": {
                                "oid": "a" * 40,
                                "statusCheckRollup": {
                                    "state": "FAILURE",
                                    "contexts": {
                                        "nodes": [
                                            {
                                                "__typename": "CheckRun",
                                                "id": "C1",
                                                "name": "build",
                                                "status": "COMPLETED",
                                                "conclusion": "FAILURE",
                                                "detailsUrl": "https://gh/ch/1",
                                                "title": "3 failing",
                                                "summary": "test_a test_b",
                                            }
                                        ]
                                    },
                                },
                            }
                        }
                    ]
                }
                # All three land in one poll: one coalesced event, attributed to
                # the PR.
                event = await h.mcp_await_channel(read_a, timeout=25)
                assert event is not None
                content = event.params["content"]
                assert KEY in content
                assert "test_a" in content
                assert "alice" in content
                assert "merge conflicts" in content
                meta = event.params["meta"]
                assert meta["kind"] == "batch"
                assert meta["severity"] == "high"
                assert meta["count"] == "3"

                # The push relay acks what it delivered.
                text = await _subscriptions_until(read_a, write_a, ids_a, "pending=0")
                assert KEY in text and "pending=0" in text

                # A second subscriber does not get the earlier events again.
                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(
                        read_b, write_b, 2, "subscribe_github_pr", {"pr": KEY}
                    )
                    assert f"Subscribed to {KEY}" in text
                    gh.pr["comments"] = _comment("dave", "LGTM after fixes")
                    event_b = await h.mcp_await_channel(read_b, timeout=25)
                    assert event_b is not None
                    content_b = event_b.params["content"]
                    assert KEY in content_b
                    assert "dave" in content_b
                    assert "merge conflicts" not in content_b
                    assert "test_a" not in content_b
                    assert "alice" not in content_b

                # Merge: a terminal event and auto-unsubscribe. A also gets the
                # 'dave' comment first, so read past it.
                gh.pr.update(
                    {"state": "MERGED", "merged": True, "mergedBy": {"login": "carol"}}
                )
                merged = await h.mcp_await_channel_with(read_a, "carol", 25)
                assert merged is not None
                assert KEY in merged.params["content"]
                assert "unsubscrib" in merged.params["content"]
                text = await _subscriptions_until(read_a, write_a, ids_a, "No active")
                assert "No active" in text

        anyio.run(scenario)


def test_refresh_polls_immediately_and_delivers(tmp_path):
    """With the scheduled poll an hour away, refresh_github_pr makes the server
    poll at once, and the event reaches the subscriber as a channel event."""
    with (
        h.FakeGitHub(NUMBER, base_pr()) as gh,
        h.core(tmp_path, graphql_url=gh.graphql_url, poll_seconds="3600") as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "refresh_github_pr", {"pr": KEY}
                )
                assert "Could not refresh" in text and "not tracked" in text

                text, _ = await h.mcp_call(
                    read_a, write_a, 3, "subscribe_github_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text

                gh.pr["comments"] = _comment("dave", "ping")
                # Another session may ask for the poll.
                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(
                        read_b, write_b, 2, "refresh_github_pr", {"pr": KEY}
                    )
                    assert f"Polling {KEY} now" in text and "1 subscriber" in text

                event = await h.mcp_await_channel(read_a, timeout=10)
                assert event is not None and "dave" in event.params["content"]

        anyio.run(scenario)


def test_pull_mode_counts_pending_until_catch_up(tmp_path):
    """In pull mode the relay holds a PR event without an ack, so the server
    counts it as pending in the subscription list. catch_up delivers and acks
    it, and the count goes to 0."""
    with (
        h.FakeGitHub(NUMBER, base_pr()) as gh,
        h.core(tmp_path, graphql_url=gh.graphql_url) as stack,
    ):

        async def scenario():
            async with h.stdio_client(h.relay_params(stack.pull_env("sid-A"))) as (
                read,
                write,
            ):
                await h.mcp_handshake(read, write)
                ids = count(2)
                text, _ = await h.mcp_call(
                    read, write, next(ids), "subscribe_github_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text

                gh.pr["comments"] = _comment("dave", "waiting for you")
                text = await _subscriptions_until(read, write, ids, "pending=1")
                assert f"{KEY}" in text and "pending=1" in text
                assert await h.mcp_await_channel(read, timeout=2) is None

                text, _ = await h.mcp_call(read, write, next(ids), "catch_up")
                assert "waiting for you" in text and KEY in text
                text = await _subscriptions_until(read, write, ids, "pending=0")
                assert "pending=0" in text

        anyio.run(scenario)


def test_auth_error_emits_event(tmp_path):
    """After a good subscribe, a 401 on the next poll gives one high-severity
    pr_auth_error event that names the PR and the token to fix."""
    with (
        h.FakeGitHub(NUMBER, base_pr()) as gh,
        h.core(tmp_path, graphql_url=gh.graphql_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "subscribe_github_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text

                gh.set_fault(401)
                event = await h.mcp_await_channel_with(read_a, "GITHUB_TOKEN", 25)
                assert event is not None
                content = event.params["content"]
                assert f"GitHub access to {KEY} failed" in content
                assert "Polling is paused" in content
                meta = event.params["meta"]
                assert meta["kind"] == "pr_auth_error"
                assert meta["severity"] == "high"

        anyio.run(scenario)


def test_not_found_emits_pr_gone_and_unsubscribes(tmp_path):
    """A PR that disappears gives a terminal pr_gone event, and the session is
    unsubscribed."""
    with (
        h.FakeGitHub(NUMBER, base_pr()) as gh,
        h.core(tmp_path, graphql_url=gh.graphql_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                ids_a = count(2)
                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "subscribe_github_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text

                # The fake now serves another number: this PR is not found.
                gh.number = NUMBER + 1
                event = await h.mcp_await_channel_with(
                    read_a, "could not be fetched", 25
                )
                assert event is not None
                content = event.params["content"]
                assert KEY in content and "unsubscrib" in content
                assert event.params["meta"]["kind"] == "pr_gone"
                assert event.params["meta"]["severity"] == "high"

                text = await _subscriptions_until(read_a, write_a, ids_a, "No active")
                assert "No active" in text

        anyio.run(scenario)
