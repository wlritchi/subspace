# vim: filetype=python
"""Shared test helpers: server process spawning, and the fake GitHub GraphQL and
Forgejo servers from _forges."""

import contextlib
import importlib.util
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import accounts
import sessions
from _forges import FakeForgejo, FakeGitHub, forgejo_pr

__all__ = ["FakeForgejo", "FakeGitHub", "forgejo_pr"]

ROOT = Path(__file__).resolve().parent.parent
SERVER = str(ROOT / "server" / "subspace-server.py")
GATEWAY = str(ROOT / "server" / "subspace-gateway.py")
# Spawned scripts run on the test run's Python version, so that
# `UV_PYTHON=3.12 uv run -qs tests/run.py` also tests them on the image's version.
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


# Env-var prefixes that could couple a test process to the machine's real setup:
# SUBSPACE_WS_URL/SUBSPACE_TOKEN pointing relays at a live server (uri()
# prefers the full-URL override, ignoring the test's host/port), real GitHub
# credentials, and the live session's CLAUDE_* identity. Tests re-set exactly what
# they need.
_ISOLATE_PREFIXES = ("SUBSPACE_", "GITHUB_", "FORGEJO_", "CLAUDE_")


def _isolated_environ() -> dict:
    return {k: v for k, v in os.environ.items() if not k.startswith(_ISOLATE_PREFIXES)}


def server_env(
    ws_port: int,
    data_dir: Path,
    *,
    graphql_url: str | None = None,
    forgejo_url: str | None = None,
    forgejo_instances: dict[str, str] | None = None,
    poll_seconds: str = "1",
    warm_ttl: str | None = None,
    handle_idle: str | None = None,
    channel_ttl: str | None = None,
) -> dict:
    env = _isolated_environ()
    env["SUBSPACE_WS_PORT"] = str(ws_port)
    env["SUBSPACE_DATA_DIR"] = str(data_dir)
    env["SUBSPACE_PR_POLL_SECONDS"] = poll_seconds
    if warm_ttl is not None:
        env["SUBSPACE_PR_WARM_TTL_SECONDS"] = warm_ttl
    if handle_idle is not None:
        env["SUBSPACE_HANDLE_IDLE_SECONDS"] = handle_idle
    if channel_ttl is not None:
        env["SUBSPACE_CHANNEL_TTL_SECONDS"] = channel_ttl
    if graphql_url:
        env["GITHUB_GRAPHQL_URL"] = graphql_url
        env["GITHUB_TOKEN"] = "test-token"
    if forgejo_url:
        # Point the spawned server's default ForgejoClient at the fake (client appends
        # /api/v1). This is the DEFAULT instance (unaliased FORGEJO_API_URL/TOKEN).
        env["FORGEJO_API_URL"] = forgejo_url
        env["FORGEJO_TOKEN"] = "test-token"
    # Named Forgejo instances: {alias -> instance root url}. Each becomes a
    # FORGEJO_<ALIAS>_API_URL / FORGEJO_<ALIAS>_TOKEN env pair (alias uppercased,
    # '-' -> '_'), so the server builds one named ForgejoClient per entry.
    for alias, url in (forgejo_instances or {}).items():
        env_alias = alias.upper().replace("-", "_")
        env[f"FORGEJO_{env_alias}_API_URL"] = url
        env[f"FORGEJO_{env_alias}_TOKEN"] = "test-token"
    return env


@contextlib.contextmanager
def server_process(env: dict):
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


@contextlib.contextmanager
def gateway_process(core_port: int, port: int, **settings: str):
    """Run the gateway against the core on ``core_port``. ``settings`` become
    SUBSPACE_GATEWAY_<NAME> environment variables."""
    env = _isolated_environ()
    env["SUBSPACE_CORE_URL"] = f"ws://127.0.0.1:{core_port}"
    env["SUBSPACE_GATEWAY_PORT"] = str(port)
    for name, value in settings.items():
        env[f"SUBSPACE_GATEWAY_{name.upper()}"] = value
    proc = subprocess.Popen(
        ["uv", "run", "-qs", "--python", _PYTHON, GATEWAY],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_port(port, timeout=60.0):
            raise RuntimeError("gateway did not start listening")
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def load_server(data_dir: Path):
    """Import the server script as a module, with the state that main() loads
    set up for an empty data directory."""
    spec = importlib.util.spec_from_file_location("subspace_server", SERVER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ACCOUNTS = accounts.AccountStore(
        data_dir / "accounts.toml", "test-token", "default"
    )
    module.SESSIONS = sessions.SessionRegistry(data_dir)
    return module
