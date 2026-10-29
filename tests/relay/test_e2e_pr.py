# vim: filetype=python
"""End-to-end tests for GitHub PR monitoring through the relay: the real server,
the relay, and a fake GraphQL endpoint. Subscribe, not found, live updates as
channel events, acks, no replay for a new subscriber, merge auto-unsubscribe,
and an on-demand refresh."""

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


def test_pr_monitoring_full_chain(tmp_path):
    with (
        h.FakeGitHub(NUMBER, base_pr()) as gh,
        h.core(tmp_path, graphql_url=gh.graphql_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "subscribe_github_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text and "open" in text

                text, _ = await h.mcp_call(
                    read_a, write_a, 3, "subscribe_github_pr", {"pr": "octo/demo#999"}
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
                # All three land in one poll: one coalesced event.
                event = await h.mcp_await_channel(read_a, timeout=25)
                assert event is not None
                content = event.params["content"]
                assert "test_a" in content
                assert "alice" in content
                assert "merge conflicts" in content
                meta = event.params["meta"]
                assert meta["kind"] == "batch"
                assert meta["severity"] == "high"
                assert meta["count"] == "3"

                await anyio.sleep(3)  # let the acks land
                text, _ = await h.mcp_call(
                    read_a, write_a, 4, "list_github_pr_subscriptions"
                )
                assert KEY in text and "pending=0" in text

                # A second subscriber does not get the earlier events again.
                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(
                        read_b, write_b, 2, "subscribe_github_pr", {"pr": KEY}
                    )
                    assert f"Subscribed to {KEY}" in text
                    gh.pr["comments"] = {
                        "nodes": [
                            {
                                "id": "IC1",
                                "author": {"login": "dave"},
                                "body": "LGTM after fixes",
                                "url": "https://gh/ic/99",
                            }
                        ]
                    }
                    event_b = await h.mcp_await_channel(read_b, timeout=25)
                    assert event_b is not None
                    content_b = event_b.params["content"]
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
                assert "unsubscrib" in merged.params["content"]
                await anyio.sleep(3)
                text, _ = await h.mcp_call(
                    read_a, write_a, 5, "list_github_pr_subscriptions"
                )
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

                gh.pr["comments"] = {
                    "nodes": [
                        {
                            "id": "IC1",
                            "author": {"login": "dave"},
                            "body": "ping",
                            "url": "https://gh/ic/1",
                        }
                    ]
                }
                # Another session may ask for the poll.
                async with stack.session("sid-B") as (read_b, write_b):
                    text, _ = await h.mcp_call(
                        read_b, write_b, 2, "refresh_github_pr", {"pr": KEY}
                    )
                    assert f"Polling {KEY} now" in text and "1 subscriber" in text

                event = await h.mcp_await_channel(read_a, timeout=10)
                assert event is not None and "dave" in event.params["content"]

        anyio.run(scenario)
