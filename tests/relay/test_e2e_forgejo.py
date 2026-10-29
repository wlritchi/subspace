# vim: filetype=python
"""End-to-end tests for Forgejo PR monitoring through the relay: the real
server, the relay, and a fake Gitea REST endpoint. One subscribe, update, merge
flow, and the `instance:` prefix for a named instance."""

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow

NUMBER = 5
KEY = f"owner/repo#{NUMBER}"


def test_forgejo_subscribe_update_merge(tmp_path):
    with (
        h.FakeForgejo(NUMBER, h.forgejo_pr(NUMBER)) as fj,
        h.core(tmp_path, forgejo_url=fj.api_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "subscribe_forgejo_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text and "Forgejo" in text
                assert "open" in text

                fj.set_reviews(
                    [
                        {
                            "id": 1,
                            "state": "REQUEST_CHANGES",
                            "user": {"login": "alice"},
                            "body": "needs work on error handling",
                            "html_url": "https://fj/r/1",
                        }
                    ]
                )
                fj.set_statuses(
                    [
                        {
                            "context": "ci/build",
                            "status": "failure",
                            "target_url": "https://fj/ci/1",
                            "description": "build failed",
                        }
                    ]
                )
                event = await h.mcp_await_channel(read_a, timeout=25)
                assert event is not None
                content = event.params["content"]
                assert "alice" in content
                assert "ci/build" in content
                assert event.params["meta"]["severity"] == "high"

                await anyio.sleep(3)  # let the relay's ack land
                text, _ = await h.mcp_call(
                    read_a, write_a, 3, "list_forgejo_pr_subscriptions"
                )
                assert KEY in text and "pending=0" in text

                fj.set_pr(state="closed", merged=True, merged_by={"login": "carol"})
                merged = await h.mcp_await_channel_with(read_a, "carol", 25)
                assert merged is not None
                assert "unsubscrib" in merged.params["content"]
                assert merged.params["meta"]["kind"] == "pr_merged"

                await anyio.sleep(3)
                text, _ = await h.mcp_call(
                    read_a, write_a, 4, "list_forgejo_pr_subscriptions"
                )
                assert "No active" in text

        anyio.run(scenario)


def test_forgejo_two_instances_isolated(tmp_path):
    """One server with a default and a named ("external") Forgejo instance, and
    the same owner/repo#N on both. The bare ref and external:<ref> are two
    independent subscriptions: an update on the named instance arrives tagged
    with its alias and does not reach the default subscription."""
    with (
        h.FakeForgejo(NUMBER, h.forgejo_pr(NUMBER)) as fj_default,
        h.FakeForgejo(NUMBER, h.forgejo_pr(NUMBER)) as fj_external,
        h.core(
            tmp_path,
            forgejo_url=fj_default.api_url,
            forgejo_instances={"external": fj_external.api_url},
        ) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                text, _ = await h.mcp_call(
                    read_a, write_a, 2, "subscribe_forgejo_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY} (Forgejo)." in text
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    3,
                    "subscribe_forgejo_pr",
                    {"pr": f"external:{KEY}"},
                )
                assert f"external:{KEY}" in text and "Forgejo: external" in text

                text, _ = await h.mcp_call(
                    read_a, write_a, 4, "list_forgejo_pr_subscriptions"
                )
                assert f"  {KEY}" in text
                assert f"external:{KEY}" in text

                fj_external.set_reviews(
                    [
                        {
                            "id": 1,
                            "state": "REQUEST_CHANGES",
                            "user": {"login": "erin"},
                            "body": "external-only review",
                            "html_url": "https://ext/r/1",
                        }
                    ]
                )
                event = await h.mcp_await_channel_with(read_a, "erin", 25)
                assert event is not None
                assert "external-only review" in event.params["content"]

                await anyio.sleep(3)
                text, _ = await h.mcp_call(
                    read_a, write_a, 5, "list_forgejo_pr_subscriptions"
                )
                assert f"  {KEY}" in text and f"external:{KEY}" in text
                default_line = next(
                    line
                    for line in text.splitlines()
                    if KEY in line and "external:" not in line
                )
                assert "pending=0" in default_line

        anyio.run(scenario)
