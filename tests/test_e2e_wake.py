# vim: filetype=python
"""End-to-end tests for webhook wake-up: a spawned core and gateway, and a
local HTTP server that stands in for the webhook receiver. See "Webhook
wake-up" in docs/specs/2026-10-10-remote-agent-mcp.md."""

import asyncio
import contextlib
import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import accounts
import subspace_protocol as proto
from _harness import free_port, gateway_process, server_env, server_process
from test_e2e_gateway import Stack, _text
from test_e2e_identity import MASTER

pytestmark = pytest.mark.slow

HOOK_KEY = "hook-key"
GRACE = 0.5
COOLDOWN = 3.0


class Hooks:
    """A webhook receiver that records each request. A path answers with the
    statuses queued for it, then 200."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.statuses: dict[str, list[int]] = {}
        self._lock = threading.Lock()
        hooks = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - http.server API
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                with hooks._lock:
                    hooks.requests.append(
                        {
                            "path": self.path,
                            "headers": {k.lower(): v for k, v in self.headers.items()},
                            "body": body,
                            "at": time.monotonic(),
                        }
                    )
                    queued = hooks.statuses.get(self.path) or []
                    status = queued.pop(0) if queued else 200
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port: int = self.server.server_address[1]

    def calls(self, path: str) -> list[dict]:
        with self._lock:
            return [r for r in self.requests if r["path"] == path]

    async def wait_for(self, path: str, count: int, timeout: float) -> list[dict]:
        deadline = time.monotonic() + timeout
        while len(self.calls(path)) < count:
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"{path}: {len(self.calls(path))} of {count} wake(s) after "
                    f"{timeout} s"
                )
            await asyncio.sleep(0.05)
        return self.calls(path)


def _accounts_file(data_dir: Path, hooks_port: int) -> dict[str, str]:
    token, credential_id, digest = accounts.new_token()
    base = f"http://127.0.0.1:{hooks_port}"
    webhooks = "".join(
        f'[[account.webhook]]\nhandle = "{handle}"\nurl = "{base}{path}"\n'
        f'key = "{HOOK_KEY}"\ngrace_seconds = {GRACE}\n'
        f"cooldown_seconds = {COOLDOWN}\n"
        for handle, path in (
            ("wk-bot", "/bot"),
            ("wk-wait", "/wait"),
            ("wk-flaky", "/flaky"),
            ("wk-gone", "/gone"),
            ("wk-restart", "/restart"),
            ("wk-pulled", "/pulled"),
        )
    )
    (data_dir / "accounts.toml").write_text(
        f'[[account]]\nid = "ci"\nchannels = ["ops"]\n'
        f'[[account.credential]]\nid = "{credential_id}"\n'
        f'secret_sha256 = "{digest}"\n{webhooks}'
    )
    return {"ci": token}


def _core_env(core_port: int, data_dir: Path, retry: str = "0.2,0.2,0.2") -> dict:
    env = server_env(core_port, data_dir)
    env["SUBSPACE_TOKEN"] = MASTER
    env["SUBSPACE_WAKE_RETRY_SECONDS"] = retry
    return env


@pytest.fixture(scope="module")
def hooks() -> Iterator[Hooks]:
    receiver = Hooks()
    thread = threading.Thread(target=receiver.server.serve_forever, daemon=True)
    thread.start()
    yield receiver
    receiver.server.shutdown()


@contextlib.contextmanager
def _stack(data_dir: Path, hooks: Hooks) -> Iterator[Stack]:
    tokens = _accounts_file(data_dir, hooks.port)
    core_port = free_port()
    gateway_port = free_port()
    with (
        server_process(_core_env(core_port, data_dir)),
        gateway_process(core_port, gateway_port, linger_seconds="0.2"),
    ):
        yield Stack(core_port, gateway_port, data_dir, tokens)


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory, hooks: Hooks) -> Iterator[Stack]:
    with _stack(tmp_path_factory.mktemp("wake"), hooks) as s:
        yield s


async def _dm(alice, to: str, body: str) -> int:
    reply = await alice.request({"type": proto.DM, "to": [to], "body": body})
    assert reply["type"] == proto.POSTED, reply
    return int(reply["ordinal"])


def test_offline_handle_is_woken(stack: Stack, hooks: Hooks) -> None:
    async def scenario() -> None:
        alice = await stack.local("wk-s1", "wk-alice")
        async with stack.mcp("?handle=wk-bot") as bot:
            await _text(bot, "join_channel", channel="ops")
        await alice.request({"type": proto.JOIN_CHANNEL, "channel": "ops"})

        agents = await alice.request({"type": proto.LIST_AGENTS})
        wakeable = {a["name"]: a.get("wakeable") for a in agents["agents"]}
        assert wakeable["wk-bot@ci"] is True
        assert wakeable["wk-alice"] is False

        # A channel post that does not address the bot is below its threshold.
        await alice.request({"type": proto.POST, "channel": "ops", "body": "fyi"})
        await asyncio.sleep(GRACE + 1.0)
        assert hooks.calls("/bot") == []

        first = await _dm(alice, "wk-bot@ci", "secret text")
        [wake] = await hooks.wait_for("/bot", 1, 5.0)
        assert wake["headers"]["authorization"] == f"Bearer {HOOK_KEY}"
        assert "secret text" not in json.dumps(wake["body"])
        assert wake["body"]["event"] == "wake"
        assert wake["body"]["account"] == "ci"
        assert wake["body"]["handle"] == "wk-bot"
        assert wake["body"]["pending"] == 1
        [item] = wake["body"]["items"]
        assert item["short_id"] == f"#{first}"
        assert item["from"] == "wk-alice@default"
        assert item["level"] == "direct"

        # A second message inside the cooldown waits for the cooldown.
        second = await _dm(alice, "wk-bot@ci", "second")
        await asyncio.sleep(1.0)
        assert len(hooks.calls("/bot")) == 1
        calls = await hooks.wait_for("/bot", 2, COOLDOWN + 2.0)
        assert calls[1]["at"] - calls[0]["at"] >= COOLDOWN - 0.1
        assert [i["short_id"] for i in calls[1]["body"]["items"]] == [f"#{second}"]

        # The agent pulls. After that, the grace time applies, not the cooldown.
        async with stack.mcp("?handle=wk-bot") as bot:
            batch = await _text(bot, "catch_up")
        assert "secret text" in batch and "second" in batch
        await _dm(alice, "wk-bot@ci", "third")
        await hooks.wait_for("/bot", 3, GRACE + 1.5)
        await alice.close()

    asyncio.run(scenario())


def test_no_wake_while_waiting(stack: Stack, hooks: Hooks) -> None:
    async def scenario() -> None:
        alice = await stack.local("wk-s2", "wk-alice2")
        async with stack.mcp("?handle=wk-wait") as bot:
            waiter = asyncio.create_task(
                _text(bot, "wait_for_messages", timeout_s=10.0)
            )
            await asyncio.sleep(0.5)
            await _dm(alice, "wk-wait@ci", "during the wait")
            assert "during the wait" in await waiter
        await asyncio.sleep(GRACE + 1.0)
        assert hooks.calls("/wait") == []
        await alice.close()

    asyncio.run(scenario())


def test_wake_retries_then_stops(stack: Stack, hooks: Hooks) -> None:
    hooks.statuses["/flaky"] = [500, 503]
    hooks.statuses["/gone"] = [404]

    async def scenario() -> None:
        alice = await stack.local("wk-s3", "wk-alice3")
        for handle in ("wk-flaky", "wk-gone"):
            async with stack.mcp(f"?handle={handle}") as bot:
                await _text(bot, "whoami")
        await asyncio.sleep(0.5)
        await _dm(alice, "wk-flaky@ci", "try again")
        await _dm(alice, "wk-gone@ci", "do not try again")
        await hooks.wait_for("/flaky", 3, 5.0)
        await hooks.wait_for("/gone", 1, 5.0)
        await asyncio.sleep(1.0)
        # 500 and 503 are tried again; the 200 ends it. A 404 is not.
        assert len(hooks.calls("/flaky")) == 3
        assert len(hooks.calls("/gone")) == 1
        await alice.close()

    asyncio.run(scenario())


def test_no_second_wake_after_a_core_restart(tmp_path: Path, hooks: Hooks) -> None:
    tokens = _accounts_file(tmp_path, hooks.port)
    core_port = free_port()
    gateway_port = free_port()
    env = _core_env(core_port, tmp_path)
    stack = Stack(core_port, gateway_port, tmp_path, tokens)

    async def first() -> None:
        alice = await stack.local("wk-s4", "wk-alice4")
        async with stack.mcp("?handle=wk-restart") as bot:
            await _text(bot, "whoami")
        await asyncio.sleep(0.5)
        await _dm(alice, "wk-restart@ci", "once")
        await hooks.wait_for("/restart", 1, 5.0)
        await alice.close()

    with gateway_process(core_port, gateway_port, linger_seconds="0.2"):
        with server_process(env):
            asyncio.run(first())
        with server_process(env):
            time.sleep(GRACE + 1.5)
    # The message is still unacked, but the first wake covered it.
    assert len(hooks.calls("/restart")) == 1


def test_no_retry_after_the_agent_pulled(tmp_path: Path, hooks: Hooks) -> None:
    hooks.statuses["/pulled"] = [500]
    tokens = _accounts_file(tmp_path, hooks.port)
    core_port = free_port()
    gateway_port = free_port()
    stack = Stack(core_port, gateway_port, tmp_path, tokens)

    async def scenario() -> None:
        alice = await stack.local("wk-s5", "wk-alice5")
        async with stack.mcp("?handle=wk-pulled") as bot:
            await _text(bot, "whoami")
            await asyncio.sleep(0.5)
            await _dm(alice, "wk-pulled@ci", "pull me")
            await hooks.wait_for("/pulled", 1, 5.0)
            # The agent pulls before the retry, then is offline again.
            assert "pull me" in await _text(bot, "catch_up")
        await asyncio.sleep(2.5)
        assert len(hooks.calls("/pulled")) == 1
        await alice.close()

    with (
        server_process(_core_env(core_port, tmp_path, retry="1.5")),
        gateway_process(core_port, gateway_port, linger_seconds="0.2"),
    ):
        asyncio.run(scenario())
