# vim: filetype=python
"""Shared helpers for the relay's end-to-end tests: a spawned server (the core), a
spawned relay driven by a raw MCP client over stdio, and the fake forges from
tests/_forges.py. The client reads raw frames, so that it sees the
notifications/claude/channel events a high-level client would drop."""

import contextlib
import json
import os
import re
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import anyio
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.message import SessionMessage
from mcp.types import (
    JSONRPCMessage,
    JSONRPCNotification,
    JSONRPCRequest,
    JSONRPCResponse,
)

import session_state

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "tests") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests"))

from _forges import FakeForgejo, FakeGitHub, forgejo_pr

SERVER = str(ROOT / "server" / "subspace-server.py")
RELAY = str(ROOT / "plugin" / "subspace" / "mcp" / "subspace-relay.py")
CHANNEL_METHOD = "notifications/claude/channel"
# The server's master token. The relay sends it as NOTIFICATIONS_TOKEN.
TOKEN = "test-token"
# Spawned scripts run on the test run's Python version, so that
# `UV_PYTHON=3.12 uv run -qs tests/run.py` also tests them on that version.
_PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port: int | str, timeout: float = 25.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.3)
            if s.connect_ex(("127.0.0.1", int(port))) == 0:
                return True
        time.sleep(0.2)
    return False


# Variables that could couple a test process to the machine's own setup: a relay
# pointed at a live server, forge credentials, and the live session's identity.
_ISOLATE_PREFIXES = ("NOTIFICATIONS_", "SUBSPACE_", "GITHUB_", "FORGEJO_", "CLAUDE_")


def _isolated_environ() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.startswith(_ISOLATE_PREFIXES)}


def core_env(
    ws_port: int,
    data_dir: Path,
    *,
    graphql_url: str | None = None,
    forgejo_url: str | None = None,
    forgejo_instances: dict[str, str] | None = None,
    poll_seconds: str = "1",
    handle_idle: str | None = None,
) -> dict[str, str]:
    env = _isolated_environ()
    env["SUBSPACE_WS_PORT"] = str(ws_port)
    env["SUBSPACE_DATA_DIR"] = str(data_dir)
    env["SUBSPACE_TOKEN"] = TOKEN
    env["SUBSPACE_PR_POLL_SECONDS"] = poll_seconds
    if handle_idle is not None:
        env["SUBSPACE_HANDLE_IDLE_SECONDS"] = handle_idle
    if graphql_url:
        env["GITHUB_GRAPHQL_URL"] = graphql_url
        env["GITHUB_TOKEN"] = "test-token"
    if forgejo_url:
        env["FORGEJO_API_URL"] = forgejo_url
        env["FORGEJO_TOKEN"] = "test-token"
    for alias, url in (forgejo_instances or {}).items():
        env_alias = alias.upper().replace("-", "_")
        env[f"FORGEJO_{env_alias}_API_URL"] = url
        env[f"FORGEJO_{env_alias}_TOKEN"] = "test-token"
    return env


@contextlib.contextmanager
def core_process(env: dict[str, str]) -> Iterator[subprocess.Popen]:
    proc = subprocess.Popen(
        ["uv", "run", "-qs", "--python", _PYTHON, SERVER],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_port(env["SUBSPACE_WS_PORT"]):
            raise RuntimeError("server did not start listening")
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


_FAKE_PROJECT = "/test/proj"


def relay_env(
    ws_port: int,
    xdg_dir: Path,
    session_id: str,
    *,
    cache_dir: Path | None = None,
    project_dir: str | None = None,
    debounce_seconds: str = "0.5",
) -> dict[str, str]:
    env = _isolated_environ()
    env["NOTIFICATIONS_WS_PORT"] = str(ws_port)
    env["NOTIFICATIONS_TOKEN"] = TOKEN
    env["NOTIFICATIONS_DATA_DIR"] = str(xdg_dir)
    # One state directory for each session: the relays of a test share one
    # Claude Code ancestor, so they would share one state file, and the file
    # wins over the environment's session id.
    state_root = Path(xdg_dir) / f"relay-{session_id}"
    state_root.mkdir(mode=0o700, exist_ok=True)
    env["XDG_RUNTIME_DIR"] = str(state_root)
    env["CLAUDE_CODE_SESSION_ID"] = session_id
    # A short push-mode debounce window: a burst still coalesces, and a lone
    # notification is delayed only briefly.
    env["NOTIFICATIONS_DEBOUNCE_SECONDS"] = debounce_seconds
    if cache_dir is not None:
        env["XDG_CACHE_HOME"] = str(cache_dir)
        # The explicit override makes the seeded-log lookup the same on every
        # platform.
        env["NOTIFICATIONS_MCP_LOG_CACHE_DIR"] = str(cache_dir)
    else:
        # Always pin the log-cache root: the relay probes its cwd (a checkout on a
        # dev machine) as a detection candidate, and could find a real MCP log.
        env["NOTIFICATIONS_MCP_LOG_CACHE_DIR"] = str(xdg_dir)
    if project_dir is not None:
        env["CLAUDE_PROJECT_DIR"] = project_dir
    # One identity file for each session, as one pod has one in production. A
    # re-opened session with the same id sees its earlier registration.
    env["NOTIFICATIONS_IDENTITY_FILE"] = str(
        Path(xdg_dir) / f"agent-identity-{session_id}.json"
    )
    _write_old_session_state(state_root, session_id)
    return env


def _write_old_session_state(xdg_dir: Path, session_id: str) -> None:
    """Under a running Claude Code session the relay finds that process as its
    ancestor and, with no state file, holds deliveries at start for a hook that
    never comes. An hour-old file says that the hook ran long ago. The resume
    tests give their relay a stand-in Claude Code process of its own."""
    claude_pid = session_state.resolve_claude_pid()
    if claude_pid is None:
        return
    directory = xdg_dir / "claude-notifications"
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / f"{claude_pid}.json"
    path.write_text(
        json.dumps(
            {
                "session_id": session_id,
                "claude_pid": claude_pid,
                "start_time": session_state._proc_start_time(claude_pid),
            }
        )
    )
    old = time.time() - 3600
    os.utime(path, (old, old))


def seed_channel_log(
    cache_dir: Path, *, registered: bool, project_dir: str = _FAKE_PROJECT
) -> None:
    """Write a fake Claude Code MCP log, so that the relay detects push
    (registered) or pull mode."""
    encoded = re.sub(r"[/.]", "-", project_dir)
    directory = (
        Path(cache_dir)
        / "claude-cli-nodejs"
        / encoded
        / "mcp-logs-plugin-subspace-subspace"
    )
    directory.mkdir(parents=True, exist_ok=True)
    marker = (
        "Channel notifications registered"
        if registered
        else "Channel notifications skipped: server not in --channels list for this session"
    )
    log = directory / "2026-01-01T00-00-00-000Z.jsonl"
    log.write_text(json.dumps({"message": marker}) + "\n")
    # The relay trusts only a log written close to its own start. The seed is
    # written before the relay starts, and a slow boot could make it look stale,
    # so stamp its mtime forward.
    future = time.time() + 3600
    os.utime(log, (future, future))


def relay_params(env: dict[str, str]) -> StdioServerParameters:
    return StdioServerParameters(
        command="uv", args=["run", "-qs", "--python", _PYTHON, RELAY], env=env
    )


class Stack:
    """A running core and the directories its relays use."""

    def __init__(self, tmp_path: Path, ws_port: int, data_dir: Path) -> None:
        self.tmp_path: Path = tmp_path
        self.ws_port: int = ws_port
        self.data_dir: Path = data_dir
        self.xdg: Path = tmp_path / "xdg"
        self.xdg.mkdir(exist_ok=True)

    def push_env(
        self, session_id: str, *, xdg_dir: Path | None = None
    ) -> dict[str, str]:
        """A relay environment in channel (push) mode: a seeded 'registered' log."""
        cache = self.tmp_path / "cache"
        seed_channel_log(cache, registered=True)
        return relay_env(
            self.ws_port,
            xdg_dir if xdg_dir is not None else self.xdg,
            session_id,
            cache_dir=cache,
            project_dir=_FAKE_PROJECT,
        )

    def pull_env(self, session_id: str) -> dict[str, str]:
        """A relay environment in pull mode: a seeded 'skipped' log."""
        cache = self.tmp_path / "cache-pull"
        seed_channel_log(cache, registered=False)
        return relay_env(
            self.ws_port,
            self.xdg,
            session_id,
            cache_dir=cache,
            project_dir=_FAKE_PROJECT,
        )

    @contextlib.asynccontextmanager
    async def session(self, session_id: str) -> AsyncIterator[tuple]:
        """A push-mode relay for `session_id` after the MCP handshake: its (read,
        write) streams."""
        async with stdio_client(relay_params(self.push_env(session_id))) as (
            read,
            write,
        ):
            await mcp_handshake(read, write)
            yield read, write


@contextlib.contextmanager
def core(tmp_path: Path, **settings: object) -> Iterator[Stack]:
    """A core on a free port with its data under `tmp_path`. `settings` are
    core_env keyword arguments."""
    data_dir = tmp_path / "store"
    data_dir.mkdir(exist_ok=True)
    ws_port = free_port()
    with core_process(core_env(ws_port, data_dir, **settings)):  # type: ignore[arg-type]
        yield Stack(tmp_path, ws_port, data_dir)


# --------------------------------------------------------------------------- #
# raw MCP-over-stdio client helpers (run inside an anyio scope)
# --------------------------------------------------------------------------- #


async def mcp_send(write, obj) -> None:
    await write.send(SessionMessage(message=JSONRPCMessage(obj)))


async def mcp_await_response(read, want_id: int, timeout: float = 20.0):
    """Return (response, [channel events seen meanwhile])."""
    channels: list = []
    with anyio.move_on_after(timeout):
        async for message in read:
            root = message.message.root
            if isinstance(root, JSONRPCNotification) and root.method == CHANNEL_METHOD:
                channels.append(root)
            elif isinstance(root, JSONRPCResponse) and root.id == want_id:
                return root, channels
    return None, channels


async def mcp_await_channel(read, timeout: float = 20.0):
    with anyio.move_on_after(timeout):
        async for message in read:
            root = message.message.root
            if isinstance(root, JSONRPCNotification) and root.method == CHANNEL_METHOD:
                return root
    return None


async def mcp_await_channel_with(read, needle: str, timeout: float = 20.0):
    """Await the next channel event whose content contains `needle`."""
    with anyio.move_on_after(timeout):
        async for message in read:
            root = message.message.root
            if (
                isinstance(root, JSONRPCNotification)
                and root.method == CHANNEL_METHOD
                and needle in (root.params.get("content") or "")
            ):
                return root
    return None


async def mcp_handshake(read, write) -> dict:
    await mcp_send(
        write,
        JSONRPCRequest(
            jsonrpc="2.0",
            id=1,
            method="initialize",
            params={
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            },
        ),
    )
    resp, _ = await mcp_await_response(read, 1)
    await mcp_send(
        write,
        JSONRPCNotification(
            jsonrpc="2.0", method="notifications/initialized", params={}
        ),
    )
    return resp.result.get("capabilities", {}) if resp else {}


async def mcp_call(
    read, write, request_id: int, name: str, arguments: dict | None = None
):
    """Call a tool; return (text, [channel events seen while waiting])."""
    await mcp_send(
        write,
        JSONRPCRequest(
            jsonrpc="2.0",
            id=request_id,
            method="tools/call",
            params={"name": name, "arguments": arguments or {}},
        ),
    )
    resp, channels = await mcp_await_response(read, request_id)
    assert resp is not None, f"no reply to {name} within the timeout"
    return resp.result["content"][0]["text"], channels


async def mcp_try_call(
    read,
    write,
    request_id: int,
    name: str,
    arguments: dict | None = None,
    *,
    timeout: float = 12.0,
):
    """Like mcp_call, but return (None, channels) when no response arrives within
    `timeout`, so that a poll loop can try again."""
    await mcp_send(
        write,
        JSONRPCRequest(
            jsonrpc="2.0",
            id=request_id,
            method="tools/call",
            params={"name": name, "arguments": arguments or {}},
        ),
    )
    resp, channels = await mcp_await_response(read, request_id, timeout=timeout)
    if resp is None:
        return None, channels
    return resp.result["content"][0]["text"], channels


async def mcp_list_tools(read, write, request_id: int) -> list[str]:
    await mcp_send(
        write,
        JSONRPCRequest(jsonrpc="2.0", id=request_id, method="tools/list", params={}),
    )
    resp, _ = await mcp_await_response(read, request_id)
    return sorted(t["name"] for t in resp.result["tools"])


__all__ = [
    "RELAY",
    "SERVER",
    "TOKEN",
    "FakeForgejo",
    "FakeGitHub",
    "Stack",
    "core",
    "core_env",
    "core_process",
    "forgejo_pr",
    "free_port",
    "mcp_await_channel",
    "mcp_await_channel_with",
    "mcp_await_response",
    "mcp_call",
    "mcp_handshake",
    "mcp_list_tools",
    "mcp_send",
    "mcp_try_call",
    "relay_env",
    "relay_params",
    "seed_channel_log",
    "stdio_client",
    "wait_port",
]
