# vim: filetype=python
"""End-to-end tests for the remote-agent gateway: a spawned core and a spawned
gateway, driven by the MCP SDK's client. See
docs/specs/2026-10-10-remote-agent-mcp.md."""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

import subspace_protocol as proto
from _harness import free_port, gateway_process, server_env, server_process
from _wsclient import Client as WsClient
from test_e2e_identity import MASTER, _accounts_file

pytestmark = pytest.mark.slow


class Stack:
    def __init__(
        self, core_port: int, gateway_port: int, data_dir: Path, tokens: dict
    ) -> None:
        self.core_port: int = core_port
        self.gateway_port: int = gateway_port
        self.data_dir: Path = data_dir
        self.tokens: dict[str, str] = tokens

    def url(self, query: str = "") -> str:
        return f"http://127.0.0.1:{self.gateway_port}/mcp{query}"

    @contextlib.asynccontextmanager
    async def mcp(
        self, query: str = "", account: str = "ci", mode: str = "auto"
    ) -> AsyncIterator[Client]:
        http = httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {self.tokens[account]}"},
            timeout=httpx2.Timeout(30, read=60),
        )
        async with http:
            transport = streamable_http_client(url=self.url(query), http_client=http)
            async with Client(transport, mode=mode) as client:
                yield client

    async def local(self, session_id: str, name: str) -> WsClient:
        client = await WsClient.open(self.core_port, MASTER)
        await client.register(session_id)
        reply = await client.request({"type": proto.REGISTER_AGENT, "name": name})
        assert reply["type"] == proto.AGENT_OK, reply
        return client


@contextlib.contextmanager
def _stack(data_dir: Path, **gateway_settings: str) -> Iterator[Stack]:
    tokens = _accounts_file(data_dir)
    core_port = free_port()
    env = server_env(core_port, data_dir)
    env["SUBSPACE_TOKEN"] = MASTER
    env["SUBSPACE_ACCOUNTS_CHECK_SECONDS"] = "0.2"
    gateway_port = free_port()
    with server_process(env):
        with gateway_process(core_port, gateway_port, **gateway_settings):
            yield Stack(core_port, gateway_port, data_dir, tokens)


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    with _stack(tmp_path_factory.mktemp("gateway")) as s:
        yield s


async def _text(client: Client, tool: str, **arguments: object) -> str:
    result = await client.call_tool(tool, arguments)
    return "\n".join(getattr(c, "text", "") for c in result.content)


def test_http_auth(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            health = await http.get(f"http://127.0.0.1:{stack.gateway_port}/healthz")
            assert health.status_code == 200
            body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            for headers in (
                {},
                {"Authorization": "Bearer nope"},
                {"Authorization": f"Bearer {MASTER}"},
                {"Authorization": "Bearer ssp_abcdefghijkl_" + "x" * 40},
            ):
                reply = await http.post(stack.url(), json=body, headers=headers)
                assert reply.status_code == 401, headers
                assert reply.headers["www-authenticate"].startswith("Bearer")
            reply = await http.post(
                stack.url(),
                json=body,
                headers={
                    "Authorization": f"Bearer {stack.tokens['ci']}",
                    "Origin": "https://evil.example",
                },
            )
            assert reply.status_code == 403

    asyncio.run(scenario())


def test_tools_follow_permissions(stack: Stack) -> None:
    async def scenario() -> None:
        async with stack.mcp("?handle=perm-bot") as client:
            names = {t.name for t in (await client.list_tools()).tools}
        assert {"post", "dm", "catch_up", "whoami", "attach"} <= names
        assert "subscribe_github_pr" not in names
        assert "schedule_test_notification" not in names

    asyncio.run(scenario())


def test_messaging_round_trip(stack: Stack) -> None:
    async def scenario() -> None:
        alice = await stack.local("gw-s1", "gw-alice")
        await alice.request({"type": proto.JOIN_CHANNEL, "channel": "ops"})
        async with stack.mcp("?handle=gw-bot") as bot:
            who = await _text(bot, "whoami")
            assert "account: ci" in who and "handle: gw-bot" in who
            reply = await _text(
                bot,
                "post",
                channel="ops",
                body="build red",
                mentions=["gw-alice@default"],
            )
            assert "Posted #" in reply
            frame = await alice.notify_of_kind("message")
            assert "gw-bot@ci: build red" in frame["content"]

            sent = await alice.request(
                {"type": proto.DM, "to": ["gw-bot@ci"], "body": "on it?"}
            )
            first = await _text(bot, "catch_up")
            assert "gw-alice@default: on it?" in first
            assert "[direct]" in first
            # Not acked until the next pull.
            status = await alice.request(
                {"type": proto.MESSAGE_STATUS, "target": f"#{sent['ordinal']}"}
            )
            assert status["pending"] == ["gw-bot@ci"]
            assert "No new notifications." in await _text(bot, "catch_up")
            status = await alice.request(
                {"type": proto.MESSAGE_STATUS, "target": f"#{sent['ordinal']}"}
            )
            assert status["delivered"] == ["gw-bot@ci"]

            reply = await _text(bot, "join_channel", channel="secret")
            assert "forbidden: channel" in reply
        await alice.close()

    asyncio.run(scenario())


def test_wait_for_messages(stack: Stack) -> None:
    async def scenario() -> None:
        alice = await stack.local("wait-s1", "wait-alice")
        async with stack.mcp("?handle=wait-bot") as bot:
            await _text(bot, "whoami")  # attach, so the handle exists

            async def later() -> None:
                await asyncio.sleep(0.5)
                await alice.request(
                    {"type": proto.DM, "to": ["wait-bot@ci"], "body": "ping"}
                )

            task = asyncio.create_task(later())
            loop = asyncio.get_running_loop()
            start = loop.time()
            reply = await _text(bot, "wait_for_messages", timeout_s=20)
            assert "wait-alice@default: ping" in reply
            assert loop.time() - start < 10
            await task
            reply = await _text(bot, "wait_for_messages", timeout_s=0.2)
            assert "No new notifications." in reply
        await alice.close()

    asyncio.run(scenario())


def test_session_by_header_and_by_id(stack: Stack) -> None:
    async def scenario() -> None:
        async with stack.mcp("?session=run-42") as bot:
            who = await _text(bot, "whoami")
            assert "session: run-42" in who
            assert "handle: (none" in who
            reply = await _text(bot, "register_agent", name="run-bot")
            assert "Registered as 'run-bot' in account 'ci'" in reply
            assert "handle: run-bot" in await _text(bot, "whoami")

    asyncio.run(scenario())


def test_attach_needs_a_session(stack: Stack) -> None:
    async def scenario() -> None:
        async with stack.mcp() as modern:
            reply = await _text(modern, "post", channel="ops", body="x")
            assert "names no session" in reply
            reply = await _text(modern, "attach", handle="modern-bot")
            assert "does not keep an MCP session" in reply
        async with stack.mcp(mode="legacy") as legacy:
            reply = await _text(legacy, "attach", handle="legacy-bot")
            assert "handle: legacy-bot" in reply
            reply = await _text(legacy, "post", channel="ops", body="hello")
            assert "Posted #" in reply

    asyncio.run(scenario())


def test_lost_batch_comes_back_after_gateway_restart(tmp_path: Path) -> None:
    with _stack(tmp_path) as s:

        async def first() -> None:
            alice = await s.local("lb-s1", "lb-alice")
            async with s.mcp("?handle=lb-bot") as bot:
                await _text(bot, "whoami")
                await alice.request(
                    {"type": proto.DM, "to": ["lb-bot@ci"], "body": "keep me"}
                )
                await asyncio.sleep(0.3)
                assert "keep me" in await _text(bot, "catch_up")
            await alice.close()

        asyncio.run(first())

        port = free_port()
        with gateway_process(s.core_port, port):
            s.gateway_port = port

            async def second() -> None:
                async with s.mcp("?handle=lb-bot") as bot:
                    await _text(bot, "whoami")
                    await asyncio.sleep(0.3)
                    assert "keep me" in await _text(bot, "catch_up")

            asyncio.run(second())


def test_revoked_credential(tmp_path: Path) -> None:
    with _stack(tmp_path, token_cache_seconds="0.2") as s:

        async def scenario() -> None:
            async with s.mcp("?handle=rv-bot") as bot:
                assert "handle: rv-bot" in await _text(bot, "whoami")
            (s.data_dir / "accounts.toml").write_text('[[account]]\nid = "ci"\n')
            await asyncio.sleep(1.0)
            async with httpx2.AsyncClient() as http:
                reply = await http.post(
                    s.url("?handle=rv-bot"),
                    json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                    headers={"Authorization": f"Bearer {s.tokens['ci']}"},
                )
                assert reply.status_code == 401

        asyncio.run(scenario())


def test_failed_auth_limit_uses_trusted_forwarded_entry(tmp_path: Path) -> None:
    with _stack(tmp_path, auth_failures_per_minute="2", trust_forwarded="1") as s:

        async def scenario() -> None:
            body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            bad = {"Authorization": "Bearer ssp_abcdefghijkl_" + "x" * 40}
            async with httpx2.AsyncClient() as http:
                codes = []
                for n in range(4):
                    # The client controls the first entries; the proxy adds the last.
                    headers = {**bad, "X-Forwarded-For": f"spoof-{n}, 10.0.0.1"}
                    reply = await http.post(s.url(), json=body, headers=headers)
                    codes.append(reply.status_code)
                assert codes == [401, 401, 429, 429]
                # A valid token from the blocked address is refused too.
                reply = await http.post(
                    s.url("?handle=x"),
                    json=body,
                    headers={
                        "Authorization": f"Bearer {s.tokens['ci']}",
                        "X-Forwarded-For": "10.0.0.1",
                    },
                )
                assert reply.status_code == 429
                # Another address is not affected.
                reply = await http.post(
                    s.url(), json=body, headers={**bad, "X-Forwarded-For": "10.0.0.2"}
                )
                assert reply.status_code == 401

        asyncio.run(scenario())
