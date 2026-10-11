# vim: filetype=python
"""End-to-end tests for accounts, sessions, and handles: a spawned server, driven
over WebSockets. See docs/specs/2026-10-10-identity.md."""

import asyncio
import contextlib
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from websockets.exceptions import ConnectionClosed

import accounts
import subspace_protocol as proto
from _harness import free_port, server_env, server_process
from _wsclient import Client
from test_unit_migrate import _old_data_dir

pytestmark = pytest.mark.slow

MASTER = "master-test-token"


class Server:
    def __init__(self, port: int, data_dir: Path, remote_token: str) -> None:
        self.port: int = port
        self.data_dir: Path = data_dir
        self.remote_token: str = remote_token

    async def local(self) -> Client:
        return await Client.open(self.port, MASTER)

    async def remote(self) -> Client:
        return await Client.open(self.port, self.remote_token)


def _accounts_file(data_dir: Path) -> str:
    token, credential_id, digest = accounts.new_token()
    (data_dir / "accounts.toml").write_text(
        f"""
[[account]]
id = "ci"
label = "CI reviewers"
channels = ["ops", "ci-*"]

[account.limits]
posts_per_minute = 5

[[account.credential]]
id = "{credential_id}"
secret_sha256 = "{digest}"
"""
    )
    return token


@contextlib.contextmanager
def _server(data_dir: Path) -> Iterator[Server]:
    token = _accounts_file(data_dir)
    port = free_port()
    env = server_env(port, data_dir)
    env["SUBSPACE_TOKEN"] = MASTER
    env["SUBSPACE_ACCOUNTS_CHECK_SECONDS"] = "0.2"
    with server_process(env):
        yield Server(port, data_dir, token)


@pytest.fixture(scope="module")
def srv(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Server]:
    with _server(tmp_path_factory.mktemp("e2e")) as server:
        yield server


async def _agent(server: Server, session_id: str, name: str, remote: bool = False):
    client = await (server.remote() if remote else server.local())
    await client.register(session_id)
    reply = await client.request({"type": proto.REGISTER_AGENT, "name": name})
    assert reply["type"] == proto.AGENT_OK, reply
    return client


def test_bad_token_is_rejected(srv: Server) -> None:
    async def scenario() -> None:
        client = await Client.open(srv.port, "wrong")
        with pytest.raises(ConnectionClosed) as info:
            await client.request({"type": proto.WHOAMI})
        assert info.value.rcvd is not None
        assert info.value.rcvd.code == proto.UNAUTHORIZED_CLOSE_CODE

    asyncio.run(scenario())


def test_whoami(srv: Server) -> None:
    async def scenario() -> None:
        local = await srv.local()
        reply = await local.request({"type": proto.WHOAMI})
        assert reply["account"] == "default"
        assert reply["default_account"] == "default"
        assert reply["session_id"] is None
        assert reply["permissions"] == ["messaging", "pr", "schedule"]
        remote = await srv.remote()
        await remote.register("@whoami-bot")
        reply = await remote.request({"type": proto.WHOAMI})
        assert reply["account"] == "ci"
        assert reply["label"] == "CI reviewers"
        assert reply["permissions"] == ["messaging"]
        assert reply["channels"] == ["ops", "ci-*"]
        assert reply["session_id"] == "@whoami-bot"
        assert reply["handle"] == "whoami-bot"
        assert reply["limits"]["posts_per_minute"] == 5
        await local.close()
        await remote.close()

    asyncio.run(scenario())


def test_local_dm(srv: Server) -> None:
    async def scenario() -> None:
        alice = await _agent(srv, "dm-s1", "dm-alice")
        bob = await _agent(srv, "dm-s2", "dm-bob")
        reply = await alice.request(
            {"type": proto.DM, "to": ["dm-bob"], "body": "hello"}
        )
        assert reply["type"] == proto.POSTED
        assert reply["context"] == "dm:dm-alice@default,dm-bob@default"
        frame = await bob.notify_of_kind("message")
        assert frame["meta"]["from"] == "dm-alice"
        assert frame["meta"]["account"] == "default"
        assert "[dm] dm-alice: hello (→ you)" in frame["content"]
        # The same pair reuses the thread.
        reply = await bob.request({"type": proto.DM, "to": ["dm-alice"], "body": "hi"})
        assert reply["context"] == "dm:dm-alice@default,dm-bob@default"
        await alice.close()
        await bob.close()

    asyncio.run(scenario())


def test_cross_account_messaging(srv: Server) -> None:
    async def scenario() -> None:
        alice = await _agent(srv, "x-s1", "x-alice")
        await alice.request({"type": proto.JOIN_CHANNEL, "channel": "ops"})
        bot = await srv.remote()
        await bot.register("@x-reviewer")

        # The remote post is capped at normal severity and shown qualified.
        reply = await bot.request(
            {
                "type": proto.POST,
                "channel": "ops",
                "body": "build red",
                "severity": "high",
                "mentions": ["x-alice@default"],
            }
        )
        assert reply["type"] == proto.POSTED, reply
        frame = await alice.notify_of_kind("message")
        assert frame["meta"]["from"] == "x-reviewer@ci"
        assert frame["meta"]["account"] == "ci"
        assert frame["meta"]["severity"] == "normal"
        assert frame["meta"]["mentions"] == ["x-alice"]
        assert "[#ops] x-reviewer@ci: build red (→ you)" in frame["content"]

        # The local agent replies by qualified handle; the remote reader sees the
        # local agent qualified.
        reply = await alice.request(
            {"type": proto.DM, "to": ["x-reviewer@ci"], "body": "looking"}
        )
        assert reply["type"] == proto.POSTED, reply
        frame = await bot.notify_of_kind("message")
        assert frame["meta"]["from"] == "x-alice@default"
        assert "x-alice@default: looking" in frame["content"]

        # Visibility: no session ids or working dirs across accounts.
        agents = (await bot.request({"type": proto.LIST_AGENTS}))["agents"]
        by_name = {a["name"]: a for a in agents}
        assert by_name["x-alice@default"]["session_id"] == ""
        assert by_name["x-reviewer"]["session_id"] == "@x-reviewer"
        assert all(a["working_dir"] == "" for a in agents)
        assert all(a["reclaim_key_hash"] == "" for a in agents)
        agents = (await alice.request({"type": proto.LIST_AGENTS}))["agents"]
        by_name = {a["name"]: a for a in agents}
        assert by_name["x-alice"]["session_id"] == "x-s1"
        assert by_name["x-reviewer@ci"]["account"] == "ci"

        # Members and history in channel_joined are relative to the reader.
        joined = await bot.request({"type": proto.JOIN_CHANNEL, "channel": "ops"})
        assert "x-alice@default" in joined["members"]
        assert joined["history"][-1]["sender"] == "x-reviewer"
        await alice.close()
        await bot.close()

    asyncio.run(scenario())


def test_remote_scope(srv: Server) -> None:
    async def scenario() -> None:
        bot = await srv.remote()
        await bot.register("@scope-bot")
        reply = await bot.request({"type": proto.JOIN_CHANNEL, "channel": "secret"})
        assert reply == {**reply, "type": proto.ERROR}
        assert "forbidden: channel" in reply["error"]
        reply = await bot.request({"type": proto.JOIN_CHANNEL, "channel": "ci-builds"})
        assert reply["type"] == proto.CHANNEL_JOINED
        reply = await bot.request({"type": proto.SCHEDULE, "delay_seconds": 1})
        assert reply["error"] == "forbidden: schedule"
        reply = await bot.request(
            {"type": proto.SUBSCRIBE_PR, "owner": "o", "repo": "r", "number": 1}
        )
        assert reply["error"] == "forbidden: pr"
        channels = (await bot.request({"type": proto.LIST_CHANNELS}))["channels"]
        assert all(
            c["name"] in {"ops"} or c["name"].startswith("ci-") for c in channels
        )
        # posts_per_minute = 5: the sixth post in a burst is refused.
        results = [
            await bot.request({"type": proto.POST, "channel": "ci-builds", "body": "x"})
            for _ in range(6)
        ]
        assert [r["type"] for r in results[:5]] == [proto.POSTED] * 5
        assert "rate limited" in results[5]["error"]
        await bot.close()

    asyncio.run(scenario())


def test_body_limit(srv: Server) -> None:
    async def scenario() -> None:
        alice = await _agent(srv, "body-s1", "body-alice")
        reply = await alice.request(
            {"type": proto.POST, "channel": "body", "body": "x" * (16 * 1024 + 1)}
        )
        assert "too long" in reply["error"]
        await alice.close()

    asyncio.run(scenario())


def test_handle_takeover(srv: Server) -> None:
    async def scenario() -> None:
        old = await _agent(srv, "tk-s1", "tk-name")
        new = await srv.local()
        await new.register("tk-s2")
        reply = await new.request({"type": proto.REGISTER_AGENT, "name": "tk-name"})
        assert reply["error"] == "name 'tk-name' is already taken"
        reply = await new.request(
            {"type": proto.REGISTER_AGENT, "name": "tk-name", "takeover": True}
        )
        assert reply["type"] == proto.AGENT_OK
        assert reply["agent"]["session_id"] == "tk-s2"
        assert reply["agent"]["generation"] == 1
        lost = await old.notify_of_kind("handle_lost")
        assert lost["meta"]["handle"] == "tk-name"
        reply = await old.request({"type": proto.POST, "channel": "x1", "body": "hi"})
        assert reply["error"] == "register_agent first"
        await old.close()
        await new.close()

    asyncio.run(scenario())


def test_attach_by_handle_finds_holder(srv: Server) -> None:
    async def scenario() -> None:
        holder = await _agent(srv, "abh-s1", "abh-name")
        other = await srv.local()
        await other.register("@abh-name")
        reply = await other.request({"type": proto.WHOAMI})
        assert reply["session_id"] == "abh-s1"
        assert reply["connections"] == 2
        await holder.close()
        await other.close()

    asyncio.run(scenario())


def test_multiple_connections_warning(srv: Server) -> None:
    async def scenario() -> None:
        first = await _agent(srv, "mc-s1", "mc-one")
        second = await srv.local()
        await second.register("mc-s1")
        for client in (first, second):
            frame = await client.notify_of_kind("multiple_connections")
            assert frame["meta"]["connections"] == 2
            assert "2 connections" in frame["content"]
        sender = await _agent(srv, "mc-s2", "mc-two")
        await sender.request({"type": proto.DM, "to": ["mc-one"], "body": "hi"})
        frame = await first.notify_of_kind("message")
        assert frame["meta"]["connections"] == 2
        await second.close()
        frame = await first.notify_of_kind("multiple_connections")
        assert frame["meta"]["connections"] == 1
        await first.close()
        await sender.close()

    asyncio.run(scenario())


def test_unregister_leaves_topics(srv: Server) -> None:
    async def scenario() -> None:
        alice = await _agent(srv, "ur-s1", "ur-alice")
        await alice.request({"type": proto.JOIN_CHANNEL, "channel": "ur-room"})
        reply = await alice.request({"type": proto.UNREGISTER_AGENT})
        assert reply["agent"]["name"] == "ur-alice"
        await alice.request({"type": proto.REGISTER_AGENT, "name": "ur-alice"})
        subs = await alice.request({"type": proto.LIST_SUBSCRIPTIONS})
        assert subs["subscriptions"] == []
        await alice.close()

    asyncio.run(scenario())


def test_revocation_closes_connection(tmp_path: Path) -> None:
    async def scenario(server: Server) -> None:
        bot = await server.remote()
        await bot.register("@revoked")
        (server.data_dir / "accounts.toml").write_text('[[account]]\nid = "ci"\n')
        with pytest.raises(ConnectionClosed) as info:
            for _ in range(50):
                await bot.request({"type": proto.WHOAMI})
                await asyncio.sleep(0.1)
        assert info.value.rcvd is not None
        assert info.value.rcvd.code == proto.UNAUTHORIZED_CLOSE_CODE
        # New connections with the token are refused too.
        again = await server.remote()
        with pytest.raises(ConnectionClosed):
            await again.request({"type": proto.WHOAMI})

    with _server(tmp_path) as server:
        asyncio.run(scenario(server))


def test_old_data_dir_is_migrated(tmp_path: Path) -> None:
    _old_data_dir(tmp_path)

    async def scenario(server: Server) -> None:
        client = await server.local()
        await client.register("sid-1")
        reply = await client.request({"type": proto.WHOAMI})
        assert reply["handle"] == "frontend"
        agents = (await client.request({"type": proto.LIST_AGENTS}))["agents"]
        assert {a["name"] for a in agents} == {"frontend", "backend"}
        subs = await client.request({"type": proto.LIST_SUBSCRIPTIONS})
        contexts = {s["context"]: s["threshold"] for s in subs["subscriptions"]}
        assert contexts == {"chan:ops": "urgent", "dm:backend,frontend": "all"}
        await client.close()

    with _server(tmp_path) as server:
        asyncio.run(scenario(server))
    assert (tmp_path / "agents.v1").is_dir()
    shutil.rmtree(tmp_path / "agents.v1")
