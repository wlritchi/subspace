# vim: filetype=python
"""End-to-end tests for Forgejo PR monitoring through the relay: the real
server, the relay, and a fake Gitea REST endpoint. A subscribe, update, merge
flow, the `instance:` prefix for a named instance, the pending count in pull
mode, and a PR that disappears."""

from itertools import count

import anyio
import pytest

import _harness as h

pytestmark = pytest.mark.slow

NUMBER = 5
KEY = f"owner/repo#{NUMBER}"


def _review(login: str, body: str) -> list[dict]:
    return [
        {
            "id": 1,
            "state": "REQUEST_CHANGES",
            "user": {"login": login},
            "body": body,
            "html_url": f"https://fj/r/{login}",
        }
    ]


async def _subscriptions_until(read, write, ids, needle: str) -> str:
    """Poll list_forgejo_pr_subscriptions until its text contains `needle`."""
    return await h.poll_call_until(
        read, write, ids, "list_forgejo_pr_subscriptions", lambda t: needle in t
    )


def test_forgejo_subscribe_update_merge(tmp_path):
    with (
        h.FakeForgejo(NUMBER, h.forgejo_pr(NUMBER)) as fj,
        h.core(tmp_path, forgejo_url=fj.api_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                ids_a = count(2)
                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "subscribe_forgejo_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text and "Forgejo" in text
                assert "open" in text

                fj.set_reviews(_review("alice", "needs work on error handling"))
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
                assert KEY in content
                assert "alice" in content
                assert "ci/build" in content
                assert event.params["meta"]["severity"] == "high"

                # The push relay acks what it delivered.
                text = await _subscriptions_until(read_a, write_a, ids_a, "pending=0")
                assert KEY in text and "pending=0" in text

                fj.set_pr(state="closed", merged=True, merged_by={"login": "carol"})
                merged = await h.mcp_await_channel_with(read_a, "carol", 25)
                assert merged is not None
                assert KEY in merged.params["content"]
                assert "unsubscrib" in merged.params["content"]
                assert merged.params["meta"]["kind"] == "pr_merged"

                text = await _subscriptions_until(read_a, write_a, ids_a, "No active")
                assert "No active" in text

        anyio.run(scenario)


def test_forgejo_two_instances_isolated(tmp_path):
    """One server with a default and a named ("external") Forgejo instance, and
    the same owner/repo#N on both. The bare ref and external:<ref> are two
    independent subscriptions: an update on the named instance is delivered
    once and does not reach the default subscription."""
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
                ids_a = count(2)
                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "subscribe_forgejo_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY} (Forgejo)." in text
                text, _ = await h.mcp_call(
                    read_a,
                    write_a,
                    next(ids_a),
                    "subscribe_forgejo_pr",
                    {"pr": f"external:{KEY}"},
                )
                assert f"external:{KEY}" in text and "Forgejo: external" in text

                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "list_forgejo_pr_subscriptions"
                )
                assert f"  {KEY}" in text
                assert f"external:{KEY}" in text

                fj_external.set_reviews(_review("erin", "external-only review"))
                event = await h.mcp_await_channel_with(read_a, "erin", 25)
                assert event is not None
                content = event.params["content"]
                # The event names the PR. The server does not put the instance
                # alias in the event text or its meta.
                assert KEY in content
                assert "external-only review" in content

                # The default subscription never saw that review: after the ack
                # of the external event, both lines show pending=0.
                text = await _subscriptions_until(read_a, write_a, ids_a, "pending=0")
                lines = [line for line in text.splitlines() if KEY in line]
                assert len(lines) == 2, text
                assert all("pending=0" in line for line in lines), text

        anyio.run(scenario)


def test_forgejo_pull_mode_counts_pending_until_catch_up(tmp_path):
    """In pull mode the relay holds a PR event without an ack, so the server
    counts it as pending. catch_up delivers and acks it."""
    with (
        h.FakeForgejo(NUMBER, h.forgejo_pr(NUMBER)) as fj,
        h.core(tmp_path, forgejo_url=fj.api_url) as stack,
    ):

        async def scenario():
            async with h.stdio_client(h.relay_params(stack.pull_env("sid-A"))) as (
                read,
                write,
            ):
                await h.mcp_handshake(read, write)
                ids = count(2)
                text, _ = await h.mcp_call(
                    read, write, next(ids), "subscribe_forgejo_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text

                fj.set_reviews(_review("alice", "waiting for you"))
                text = await _subscriptions_until(read, write, ids, "pending=1")
                assert KEY in text and "pending=1" in text
                assert await h.mcp_await_channel(read, timeout=2) is None

                text, _ = await h.mcp_call(read, write, next(ids), "catch_up")
                assert "waiting for you" in text and KEY in text
                text = await _subscriptions_until(read, write, ids, "pending=0")
                assert "pending=0" in text

        anyio.run(scenario)


def test_forgejo_not_found_emits_pr_gone(tmp_path):
    with (
        h.FakeForgejo(NUMBER, h.forgejo_pr(NUMBER)) as fj,
        h.core(tmp_path, forgejo_url=fj.api_url) as stack,
    ):

        async def scenario():
            async with stack.session("sid-A") as (read_a, write_a):
                ids_a = count(2)
                text, _ = await h.mcp_call(
                    read_a, write_a, next(ids_a), "subscribe_forgejo_pr", {"pr": KEY}
                )
                assert f"Subscribed to {KEY}" in text

                # The fake now serves another number: /pulls/{NUMBER} is a 404.
                fj.number = NUMBER + 1
                event = await h.mcp_await_channel_with(
                    read_a, "could not be fetched", 25
                )
                assert event is not None
                content = event.params["content"]
                assert KEY in content and "unsubscrib" in content
                assert event.params["meta"]["kind"] == "pr_gone"

                text = await _subscriptions_until(read_a, write_a, ids_a, "No active")
                assert "No active" in text

        anyio.run(scenario)
