# vim: filetype=python
"""Unit tests for the relay's server URL and credential: the resolution order,
the token files, the logs, redirects, a missing or refused credential, and the
plugin manifest. See docs/specs/2026-11-11-relay-credentials.md."""

import contextlib
import importlib.util
import json
import os
import re
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from types import ModuleType

import anyio
import anyio.abc
import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.http11 import Request, Response

import connection
import wsproto

PLUGIN = Path(__file__).resolve().parents[2] / "plugin" / "subspace"
RELAY = PLUGIN / "mcp" / "subspace-relay.py"

_PROXY_NAMES = {"http_proxy", "https_proxy", "ws_proxy", "all_proxy", "no_proxy"}


@pytest.fixture(scope="module")
def relay() -> ModuleType:
    spec = importlib.util.spec_from_file_location("subspace_relay_credentials", RELAY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Remove every setting that the relay reads for its URL and credential, and
    move the home directory, so that the machine's own setup has no effect."""
    for name in list(os.environ):
        if (
            name.startswith(("SUBSPACE_", "NOTIFICATIONS_"))
            or name.lower() in _PROXY_NAMES
            or name == "XDG_DATA_HOME"
        ):
            monkeypatch.delenv(name)
    directory = tmp_path / "home"
    directory.mkdir()
    monkeypatch.setenv("HOME", str(directory))
    return directory


def _server_token(home: Path) -> Path:
    return home / ".local" / "share" / "subspace" / "token"


def _legacy_token(home: Path) -> Path:
    return home / ".claude" / "notifications" / "token"


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


# --------------------------------------------------------------------------- #
# resolution order
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("env", "url", "label"),
    [
        (
            {"SUBSPACE_OPTION_WS_URL": "wss://option.example"},
            "wss://option.example",
            "the server_url option",
        ),
        (
            {"SUBSPACE_WS_URL": "wss://env.example"},
            "wss://env.example",
            "SUBSPACE_WS_URL",
        ),
        (
            {"NOTIFICATIONS_WS_URL": "wss://old.example"},
            "wss://old.example",
            "NOTIFICATIONS_WS_URL",
        ),
        (
            {"NOTIFICATIONS_WS_HOST": "10.0.0.5", "NOTIFICATIONS_WS_PORT": "9000"},
            "ws://10.0.0.5:9000",
            "NOTIFICATIONS_WS_HOST and NOTIFICATIONS_WS_PORT",
        ),
        (
            {"NOTIFICATIONS_WS_HOST": "10.0.0.5"},
            "ws://10.0.0.5:8137",
            "NOTIFICATIONS_WS_HOST",
        ),
        (
            {"NOTIFICATIONS_WS_PORT": "9000"},
            "ws://127.0.0.1:9000",
            "NOTIFICATIONS_WS_PORT",
        ),
        ({}, "ws://127.0.0.1:8137", "the default URL"),
    ],
)
def test_each_url_step(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], url: str, label: str
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    settings = connection.resolve()
    assert settings.url == url
    assert settings.url_source.label == label


def test_url_steps_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    names = [
        ("SUBSPACE_OPTION_WS_URL", "wss://one.example"),
        ("SUBSPACE_WS_URL", "wss://two.example"),
        ("NOTIFICATIONS_WS_URL", "wss://three.example"),
        ("NOTIFICATIONS_WS_PORT", "9000"),
    ]
    for name, value in names:
        monkeypatch.setenv(name, value)
    assert connection.resolve().url == "wss://one.example"
    # Claude Code gives an unset option as an empty string.
    monkeypatch.setenv("SUBSPACE_OPTION_WS_URL", "")
    assert connection.resolve().url == "wss://two.example"
    monkeypatch.delenv("SUBSPACE_WS_URL")
    assert connection.resolve().url == "wss://three.example"
    monkeypatch.delenv("NOTIFICATIONS_WS_URL")
    assert connection.resolve().url == "ws://127.0.0.1:9000"


@pytest.mark.parametrize(
    ("name", "label"),
    [
        ("SUBSPACE_OPTION_TOKEN", "the token option"),
        ("SUBSPACE_TOKEN", "SUBSPACE_TOKEN"),
        ("NOTIFICATIONS_TOKEN", "NOTIFICATIONS_TOKEN"),
    ],
)
def test_each_token_variable_step(
    monkeypatch: pytest.MonkeyPatch, name: str, label: str
) -> None:
    monkeypatch.setenv(name, "value-a")
    settings = connection.resolve()
    assert settings.token == "value-a"
    assert settings.token_source is not None
    assert settings.token_source.label == label


def test_token_file_step(home: Path) -> None:
    path = _write(_legacy_token(home), "file-value\n")
    settings = connection.resolve()
    assert settings.token == "file-value"
    assert settings.token_source is not None
    assert settings.token_source.label == f"the token file ({path})"
    assert not settings.uses_server_token_file


def test_server_token_file_step(home: Path) -> None:
    path = _write(_server_token(home), "master-value\n")
    settings = connection.resolve()
    assert settings.token == "master-value"
    assert settings.token_source is not None
    assert settings.token_source.label == f"the server's token file ({path})"
    assert settings.uses_server_token_file


def test_no_token_step() -> None:
    settings = connection.resolve()
    assert settings.token is None
    assert settings.token_source is None


def test_empty_option_gives_subspace_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBSPACE_OPTION_TOKEN", "")
    monkeypatch.setenv("SUBSPACE_TOKEN", "from-env")
    assert connection.resolve().token == "from-env"


def test_option_wins_over_subspace_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBSPACE_OPTION_TOKEN", "from-option")
    monkeypatch.setenv("SUBSPACE_TOKEN", "from-env")
    monkeypatch.setenv("NOTIFICATIONS_TOKEN", "from-old-env")
    settings = connection.resolve()
    assert settings.token == "from-option"
    assert settings.token_source is not None
    assert settings.token_source.kind == connection.OPTION


def test_options_alone_give_the_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUBSPACE_OPTION_WS_URL", "wss://option.example")
    monkeypatch.setenv("SUBSPACE_OPTION_TOKEN", "from-option")
    settings = connection.resolve()
    assert (settings.url, settings.token) == ("wss://option.example", "from-option")
    assert connection.source_lines(settings) == [
        "URL from the server_url option, credential from the token option"
    ]


@pytest.mark.parametrize(
    ("env", "warns"),
    [
        ({"SUBSPACE_OPTION_WS_URL": "wss://a", "SUBSPACE_TOKEN": "t"}, True),
        ({"SUBSPACE_WS_URL": "wss://a", "SUBSPACE_OPTION_TOKEN": "t"}, True),
        ({"SUBSPACE_OPTION_WS_URL": "wss://a", "NOTIFICATIONS_TOKEN": "t"}, True),
        ({"SUBSPACE_OPTION_WS_URL": "wss://a", "SUBSPACE_OPTION_TOKEN": "t"}, False),
        ({"SUBSPACE_WS_URL": "wss://a", "SUBSPACE_TOKEN": "t"}, False),
        # The default URL is not a source.
        ({"SUBSPACE_OPTION_TOKEN": "t"}, False),
    ],
)
def test_mixed_sources_log_a_warning(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env: dict[str, str],
    warns: bool,
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    relay.DaemonClient()._resolve_settings()
    assert ("warning:" in capsys.readouterr().err) is warns


def test_option_url_with_token_file_logs_a_warning(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(_legacy_token(home), "file-value")
    monkeypatch.setenv("SUBSPACE_OPTION_WS_URL", "wss://a.example")
    settings = connection.resolve()
    assert settings.mixed_sources
    assert any(
        line.startswith("warning:") for line in connection.source_lines(settings)
    )


@pytest.mark.parametrize(
    ("env", "label", "replacement"),
    [
        (
            {"NOTIFICATIONS_WS_URL": "wss://old.example"},
            "NOTIFICATIONS_WS_URL",
            "NOTIFICATIONS_WS_URL is deprecated: use SUBSPACE_WS_URL",
        ),
        (
            {"NOTIFICATIONS_WS_PORT": "9000"},
            "NOTIFICATIONS_WS_PORT",
            "use SUBSPACE_WS_URL=ws://127.0.0.1:9000",
        ),
        (
            {"NOTIFICATIONS_WS_HOST": "10.0.0.5", "NOTIFICATIONS_WS_PORT": "9000"},
            "NOTIFICATIONS_WS_HOST and NOTIFICATIONS_WS_PORT",
            "use SUBSPACE_WS_URL=ws://10.0.0.5:9000",
        ),
        (
            {"NOTIFICATIONS_TOKEN": "t"},
            "NOTIFICATIONS_TOKEN",
            "NOTIFICATIONS_TOKEN is deprecated: use the token option or SUBSPACE_TOKEN",
        ),
    ],
)
def test_deprecated_name_selects_its_step_and_logs_its_replacement(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env: dict[str, str],
    label: str,
    replacement: str,
) -> None:
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    settings = relay.DaemonClient()._resolve_settings()
    sources = {settings.url_source.label}
    if settings.token_source is not None:
        sources.add(settings.token_source.label)
    assert label in sources
    err = capsys.readouterr().err
    assert replacement in err
    assert err.count("deprecated") == 1


def test_deprecated_token_file_logs_its_replacement(
    home: Path, capsys: pytest.CaptureFixture[str], relay: ModuleType
) -> None:
    _write(_legacy_token(home), "file-value")
    relay.DaemonClient()._resolve_settings()
    err = capsys.readouterr().err
    assert "is deprecated: use the token option or SUBSPACE_TOKEN" in err


def test_sources_log_again_only_on_a_change(
    home: Path, relay: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    client = relay.DaemonClient()
    client._resolve_settings()
    client._resolve_settings()
    err = capsys.readouterr().err
    assert err.count("URL from the default URL, no credential") == 1
    _write(_server_token(home), "master-value")
    client._resolve_settings()
    assert "credential from the server's token file" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# the deprecated token file (step 4)
# --------------------------------------------------------------------------- #


def test_token_file_in_custom_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    custom = tmp_path / "custom"
    _write(custom / "token", "custom-value")
    monkeypatch.setenv("NOTIFICATIONS_DATA_DIR", str(custom))
    monkeypatch.setenv("SUBSPACE_WS_URL", "wss://a.example")
    assert connection.resolve().token == "custom-value"


def test_token_file_link_to_an_existing_file(home: Path, tmp_path: Path) -> None:
    target = _write(tmp_path / "target", "linked-value")
    link = _legacy_token(home)
    link.parent.mkdir(parents=True)
    link.symlink_to(target)
    assert connection.resolve().token == "linked-value"


def test_token_file_dangling_link(home: Path, tmp_path: Path) -> None:
    link = _legacy_token(home)
    link.parent.mkdir(parents=True)
    link.symlink_to(tmp_path / "gone")
    assert connection.read_token_file(link).problem == connection.DANGLING
    assert connection.resolve().token is None


def test_token_file_empty(home: Path) -> None:
    path = _write(_legacy_token(home), " \n")
    assert connection.read_token_file(path).problem == connection.EMPTY
    assert connection.resolve().token is None


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads every file")
def test_token_file_unreadable(home: Path) -> None:
    path = _write(_legacy_token(home), "secret")
    path.chmod(0)
    try:
        assert connection.read_token_file(path).problem == connection.UNREADABLE
        assert connection.resolve().token is None
    finally:
        path.chmod(0o600)


# --------------------------------------------------------------------------- #
# the server's token file (step 5)
# --------------------------------------------------------------------------- #


def test_server_token_file_default_location(home: Path) -> None:
    _write(_server_token(home), "master-value")
    settings = connection.resolve()
    assert settings.url == "ws://127.0.0.1:8137"
    assert settings.token == "master-value"
    assert settings.uses_server_token_file


def test_server_token_file_follows_subspace_data_dir(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(_server_token(home), "wrong-place")
    _write(tmp_path / "xdg" / "subspace" / "token", "wrong-place")
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    _write(tmp_path / "data" / "token", "data-dir-value")
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path / "data"))
    assert connection.resolve().token == "data-dir-value"


def test_server_token_file_follows_xdg_data_home(
    tmp_path: Path, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(_server_token(home), "wrong-place")
    _write(tmp_path / "xdg" / "subspace" / "token", "xdg-value")
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    assert connection.resolve().token == "xdg-value"


@pytest.mark.parametrize(
    "name",
    [
        "SUBSPACE_OPTION_WS_URL",
        "SUBSPACE_WS_URL",
        "NOTIFICATIONS_WS_URL",
        "NOTIFICATIONS_WS_HOST",
        "NOTIFICATIONS_WS_PORT",
    ],
)
@pytest.mark.parametrize("url", ["ws://127.0.0.1:8137", "ws://localhost:8137"])
def test_url_setting_turns_off_the_server_token_file(
    home: Path, monkeypatch: pytest.MonkeyPatch, name: str, url: str
) -> None:
    _write(_server_token(home), "master-value")
    if name == "NOTIFICATIONS_WS_HOST":
        value = url.split("//")[1].split(":")[0]
    elif name == "NOTIFICATIONS_WS_PORT":
        value = "8137"
    else:
        value = url
    monkeypatch.setenv(name, value)
    settings = connection.resolve()
    assert settings.token is None
    assert settings.server_token_file is None
    error = connection.no_credential_error(settings)
    assert error.startswith("no credential.")
    assert "server's token file" not in error
    assert "start the server" not in error


def test_token_file_comes_before_server_token_file(home: Path) -> None:
    _write(_server_token(home), "master-value")
    _write(_legacy_token(home), "file-value")
    settings = connection.resolve()
    assert settings.token == "file-value"
    assert not settings.uses_server_token_file


@pytest.mark.parametrize(
    ("make", "reason", "advice"),
    [
        ("missing", "it does not exist", True),
        ("dangling", "it is a dangling link", False),
        ("directory", "it is not a regular file", False),
        ("unreadable", "it cannot be read", False),
        ("empty", "it is empty", False),
    ],
)
def test_no_credential_error_gives_the_reason(
    home: Path, tmp_path: Path, make: str, reason: str, advice: bool
) -> None:
    if make == "unreadable" and os.geteuid() == 0:
        pytest.skip("root reads every file")
    path = _server_token(home)
    path.parent.mkdir(parents=True)
    if make == "dangling":
        path.symlink_to(tmp_path / "gone")
    elif make == "directory":
        path.mkdir()
    elif make == "unreadable":
        path.write_text("secret")
        path.chmod(0)
    elif make == "empty":
        path.write_text("")
    try:
        error = connection.no_credential_error(connection.resolve())
    finally:
        if make == "unreadable":
            path.chmod(0o600)
    assert f"The relay also read the server's token file ({path}): {reason}." in error
    assert ("For a server on this machine, start the server first." in error) is advice


# --------------------------------------------------------------------------- #
# connections: fake servers and a fake proxy
# --------------------------------------------------------------------------- #


class FakeServer:
    """A WebSocket server on a free local port. It records each handshake
    request. If `status` is set, it answers the handshake with that status. If
    `close_code` is set, it accepts the handshake, and closes the connection
    with that code at frame `close_at_frame` from the client (the first frame
    is the register). If `greeting` is set, it sends that frame first."""

    def __init__(
        self,
        status: int | None = None,
        location: str | None = None,
        close_code: int | None = None,
        close_at_frame: int = 1,
        greeting: str | None = None,
    ) -> None:
        self.status: int | None = status
        self.location: str | None = location
        self.close_code: int | None = close_code
        self.close_at_frame: int = close_at_frame
        self.greeting: str | None = greeting
        self.requests: list[Request] = []
        self.frames: list[str] = []
        self.port: int = 0

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def bearers(self) -> list[str]:
        return [
            value
            for request in self.requests
            for value in request.headers.get_all("Authorization")
            if value.startswith("Bearer ")
        ]

    def _process_request(
        self, conn: ServerConnection, request: Request
    ) -> Response | None:
        self.requests.append(request)
        if self.status is None:
            return None
        response = conn.respond(self.status, "")
        if self.location is not None:
            response.headers["Location"] = self.location
        return response

    async def _handler(self, conn: ServerConnection) -> None:
        with contextlib.suppress(ConnectionClosed):
            if self.greeting is not None:
                await conn.send(self.greeting)
            count = 0
            async for frame in conn:
                self.frames.append(str(frame))
                count += 1
                if self.close_code is not None and count >= self.close_at_frame:
                    await conn.close(code=self.close_code, reason="unauthorized")


@contextlib.asynccontextmanager
async def running(server: FakeServer) -> AsyncIterator[FakeServer]:
    async with serve(
        server._handler, "127.0.0.1", 0, process_request=server._process_request
    ) as ws_server:
        server.port = next(iter(ws_server.sockets)).getsockname()[1]
        yield server


class FakeProxy:
    """A TCP listener that counts connections and closes each one."""

    def __init__(self, port: int) -> None:
        self.port: int = port
        self.hits: int = 0

    async def handle(self, stream: anyio.abc.SocketStream) -> None:
        self.hits += 1
        await stream.aclose()


@contextlib.asynccontextmanager
async def fake_proxy() -> AsyncIterator[FakeProxy]:
    listener = await anyio.create_tcp_listener(local_host="127.0.0.1")
    proxy = FakeProxy(listener.extra(anyio.abc.SocketAttribute.local_port))
    async with anyio.create_task_group() as tg:
        tg.start_soon(listener.serve, proxy.handle)
        yield proxy
        tg.cancel_scope.cancel()


async def _until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    with anyio.fail_after(timeout):
        while not predicate():
            await anyio.sleep(0.02)


async def _attempt_until_connected(client: object, timeout: float = 5.0) -> None:
    """Run one connection attempt until it connects, then end it."""
    async with anyio.create_task_group() as tg:
        tg.start_soon(client._connect_once)  # type: ignore[attr-defined]
        await _until(lambda: client.connected, timeout)  # type: ignore[attr-defined]
        tg.cancel_scope.cancel()


@contextlib.asynccontextmanager
async def running_client(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[object]:
    """A relay client with its reconnect loop running, as the tools' DAEMON."""
    monkeypatch.setattr(relay, "STARTUP_GRACE_SECONDS", 0.0)
    client = relay.DaemonClient()
    monkeypatch.setattr(relay, "DAEMON", client)
    async with anyio.create_task_group() as tg:
        tg.start_soon(client.run)
        await _until(lambda: client._attempts >= 1 or client.connected)
        yield client
        tg.cancel_scope.cancel()


def _set_proxy(monkeypatch: pytest.MonkeyPatch, port: int) -> None:
    for name in ("http_proxy", "https_proxy", "ws_proxy"):
        monkeypatch.setenv(name, f"http://127.0.0.1:{port}")


def test_server_token_file_connection_uses_no_proxy(
    relay: ModuleType, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(_server_token(home), "master-value")

    async def scenario() -> tuple[int, list[str]]:
        async with fake_proxy() as proxy, running(FakeServer()) as server:
            _set_proxy(monkeypatch, proxy.port)
            monkeypatch.setattr(connection, "DEFAULT_URL", server.url)
            client = relay.DaemonClient()
            await _attempt_until_connected(client)
            return proxy.hits, server.bearers()

    hits, bearers = anyio.run(scenario)
    assert hits == 0
    assert bearers == ["Bearer master-value"]


def test_url_setting_connection_uses_the_proxy(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control for the test above: the fake proxy gets a connection that
    the environment sends to it."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")

    async def scenario() -> tuple[int, int]:
        async with fake_proxy() as proxy, running(FakeServer()) as server:
            _set_proxy(monkeypatch, proxy.port)
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            client = relay.DaemonClient()
            # The fake proxy closes the connection, so the attempt fails.
            with contextlib.suppress(OSError, WebSocketException):
                await client._connect_once()
            return proxy.hits, len(server.requests)

    hits, requests = anyio.run(scenario)
    assert hits == 1
    assert requests == 0


def test_server_token_file_that_appears_later_is_used(
    relay: ModuleType,
    home: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def scenario() -> FakeServer:
        async with running(FakeServer()) as server:
            monkeypatch.setattr(connection, "DEFAULT_URL", server.url)
            client = relay.DaemonClient()
            await client._connect_once()
            assert not client.connected
            assert client.credential_error is not None
            assert server.requests == []
            _write(_server_token(home), "late-value")
            await _attempt_until_connected(client)
            assert client.credential_error is None
            return server

    server = anyio.run(scenario)
    assert server.bearers() == ["Bearer late-value"]
    err = capsys.readouterr().err
    assert "no credential" in err
    assert "credential from the server's token file" in err


# --------------------------------------------------------------------------- #
# redirects
# --------------------------------------------------------------------------- #


def test_redirect_is_not_followed(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("SUBSPACE_TOKEN", "redirect-sentinel")

    async def scenario() -> tuple[FakeServer, FakeServer, object]:
        async with running(FakeServer()) as target:
            location = f"ws://127.0.0.1:{target.port}/elsewhere-sentinel"
            async with running(FakeServer(302, location)) as origin:
                monkeypatch.setenv("SUBSPACE_WS_URL", origin.url)
                client = relay.DaemonClient()
                await client._connect_once()
                return origin, target, client

    origin, target, client = anyio.run(scenario)
    assert not client.connected  # type: ignore[attr-defined]
    assert client.credential_error is None  # type: ignore[attr-defined]
    assert len(origin.requests) == 1
    assert target.requests == []
    err = capsys.readouterr().err
    assert "HTTP 302" in err
    assert "elsewhere-sentinel" not in err
    assert f":{target.port}" not in err
    assert "redirect-sentinel" not in err


# --------------------------------------------------------------------------- #
# a missing or refused credential
# --------------------------------------------------------------------------- #


def test_no_credential_does_not_connect_or_create_a_file(
    relay: ModuleType,
    home: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def scenario() -> tuple[FakeServer, str]:
        async with running(FakeServer()) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch):
                startup = capsys.readouterr().err
                assert "URL from SUBSPACE_WS_URL, no credential" in startup
                assert "subspace relay: no credential." in startup
                with anyio.fail_after(5):
                    text = await relay.list_agents()
            return server, text

    server, text = anyio.run(scenario)
    assert server.requests == []
    assert text == (
        "subspace: no credential. Set the plugin's token option "
        "(/plugin configure subspace@subspace), or SUBSPACE_TOKEN."
    )
    assert not _legacy_token(home).exists()
    assert not _server_token(home).exists()
    assert not _legacy_token(home).parent.exists()


def test_no_credential_with_default_url_names_the_server_token_file(
    relay: ModuleType, home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> str:
        async with running_client(relay, monkeypatch):
            with anyio.fail_after(5):
                return await relay.list_agents()

    text = anyio.run(scenario)
    assert text == (
        "subspace: no credential. Set the plugin's token option "
        "(/plugin configure subspace@subspace), or SUBSPACE_TOKEN. "
        "The relay also read the server's token file "
        f"({_server_token(home)}): it does not exist. "
        "For a server on this machine, start the server first."
    )


@pytest.mark.parametrize("status", [401, 403])
def test_refused_handshake_names_the_credential_source(
    relay: ModuleType,
    home: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    status: int,
) -> None:
    path = _write(_legacy_token(home), "file-value")

    async def scenario() -> tuple[str, str, bool]:
        async with running(FakeServer(status)) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch) as client:
                with anyio.fail_after(5):
                    text = await relay.list_agents()
                # The reconnect loop goes on: a server that accepts the
                # credential gets the next attempt.
                server.status = None
                connected = await client.wait_connected(timeout=5)  # type: ignore[attr-defined]
            return text, server.url, connected

    text, url, connected = anyio.run(scenario)
    assert text == (
        f"subspace: the server at {url} refused the credential from the token "
        f"file ({path})."
    )
    assert connected
    err = capsys.readouterr().err
    assert (
        f"refused the credential from the token file ({path}). (HTTP {status})" in err
    )


def test_refused_handshake_with_subspace_token(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")

    async def scenario() -> tuple[str, str]:
        async with running(FakeServer(401)) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch):
                with anyio.fail_after(5):
                    return await relay.list_agents(), server.url

    text, url = anyio.run(scenario)
    assert text == (
        f"subspace: the server at {url} refused the credential from SUBSPACE_TOKEN."
    )


def test_unauthorized_close_after_connect_is_a_refusal(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The server accepts the handshake, and closes at the first frame (the
    register) with the unauthorized close code."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")
    code = wsproto.UNAUTHORIZED_CLOSE_CODE

    async def scenario() -> tuple[str, str]:
        async with running(FakeServer(close_code=code)) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch) as client:
                await _until(lambda: client.credential_error is not None)  # type: ignore[attr-defined]
                with anyio.fail_after(5):
                    return await relay.list_agents(), server.url

    text, url = anyio.run(scenario)
    assert text == (
        f"subspace: the server at {url} refused the credential from SUBSPACE_TOKEN."
    )
    assert f"(WebSocket close code {code})" in capsys.readouterr().err


def test_unauthorized_close_of_a_live_connection_is_a_refusal(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A credential that the server revokes while the relay is connected."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")

    async def scenario() -> tuple[bool, str | None, str]:
        async with running(FakeServer()) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch) as client:
                await _until(lambda: client.connected)  # type: ignore[attr-defined]
                server.close_code = wsproto.UNAUTHORIZED_CLOSE_CODE
                # A frame from the client makes the fake server close.
                await client._ws.send(json.dumps({"type": wsproto.WHOAMI}))  # type: ignore[attr-defined]
                await _until(lambda: client.credential_error is not None)  # type: ignore[attr-defined]
                return (
                    client.connected,  # type: ignore[attr-defined]
                    client.credential_error,  # type: ignore[attr-defined]
                    server.url,
                )

    connected, error, url = anyio.run(scenario)
    assert not connected
    assert error == (
        f"subspace: the server at {url} refused the credential from SUBSPACE_TOKEN."
    )


def test_normal_close_is_not_a_refusal(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")

    async def scenario() -> tuple[int, str | None]:
        async with running(FakeServer(close_code=1000)) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            client = relay.DaemonClient()
            with contextlib.suppress(ConnectionClosed):
                await client._connect_once()
            return len(server.requests), client.credential_error

    requests, error = anyio.run(scenario)
    assert requests == 1
    assert error is None


def test_repeated_refusal_logs_once(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Each attempt passes the handshake and then gets the unauthorized close.
    The log gives the refusal again only if the outcome or the source changes,
    or after the server accepted the credential."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")
    code = wsproto.UNAUTHORIZED_CLOSE_CODE
    refused = "refused the credential from"

    async def attempts(client: object, count: int) -> None:
        for _ in range(count):
            with contextlib.suppress(ConnectionClosed):
                await client._connect_once()  # type: ignore[attr-defined]

    async def scenario() -> list[int]:
        counts: list[int] = []
        server = FakeServer(close_code=code)
        async with running(server):
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            client = relay.DaemonClient()
            await attempts(client, 3)
            counts.append(capsys.readouterr().err.count(refused))
            # A different outcome, and then the close again.
            server.status = 401
            await attempts(client, 2)
            server.status = None
            await attempts(client, 2)
            counts.append(capsys.readouterr().err.count(refused))
            # A different source.
            monkeypatch.setenv("SUBSPACE_OPTION_TOKEN", "option-value")
            await attempts(client, 2)
            counts.append(capsys.readouterr().err.count(refused))
            # A frame from the server shows that it accepted the credential.
            # Its later close is a revocation, and the log gives it.
            server.greeting = json.dumps({"type": "hello"})
            await attempts(client, 1)
            counts.append(capsys.readouterr().err.count(refused))
        return counts

    assert anyio.run(scenario) == [1, 2, 1, 1]


def test_refusal_after_a_request_is_sent(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The server closes with the unauthorized code only at the first request,
    after the register. The tool call that sent the request gets the refused
    error, not a timeout."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")
    code = wsproto.UNAUTHORIZED_CLOSE_CODE

    async def scenario() -> tuple[str, str, list[str]]:
        async with running(FakeServer(close_code=code, close_at_frame=2)) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch) as client:
                await _until(lambda: client.connected)  # type: ignore[attr-defined]
                with anyio.fail_after(2):
                    text = await relay.list_agents()
            return text, server.url, server.frames

    text, url, frames = anyio.run(scenario)
    assert [json.loads(frame)["type"] for frame in frames] == [
        wsproto.REGISTER,
        wsproto.LIST_AGENTS,
    ]
    assert text == (
        f"subspace: the server at {url} refused the credential from SUBSPACE_TOKEN."
    )


def test_request_on_a_closed_connection_is_not_reachable(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A normal close at the first request ends the request at once, with the
    not-reachable error."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")

    async def scenario() -> str:
        async with running(FakeServer(close_code=1000, close_at_frame=2)) as server:
            monkeypatch.setenv("SUBSPACE_WS_URL", server.url)
            async with running_client(relay, monkeypatch) as client:
                await _until(lambda: client.connected)  # type: ignore[attr-defined]
                with anyio.fail_after(2):
                    return await relay._daemon_request({"type": wsproto.LIST_AGENTS})

    text = anyio.run(scenario)
    assert isinstance(text, str)
    assert text.startswith("The subspace server is not reachable at")


def test_unreachable_error_names_the_new_settings(
    relay: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUBSPACE_TOKEN", "env-value")
    # A port with no listener.
    monkeypatch.setenv("SUBSPACE_WS_URL", "ws://127.0.0.1:9")
    monkeypatch.setattr(relay, "DAEMON", relay.DaemonClient())
    text = relay._daemon_unreachable_message()
    assert text.startswith("The subspace server is not reachable at ws://127.0.0.1:9.")
    assert "server_url option or SUBSPACE_WS_URL" in text
    assert "NOTIFICATIONS_" not in text


# --------------------------------------------------------------------------- #
# secrets
# --------------------------------------------------------------------------- #

# Each secret in these tests starts with SENTINEL.
_USERINFO = "SENTINEL-USER-2e8a:SENTINEL-PASS-3b9e"


def _secret_url(case: str, port: int) -> str:
    query = "?key=SENTINEL-QUERY-5a2f"
    if case == "unreachable":
        # The client refuses a URL with a fragment, so only this case has one.
        return f"ws://{_USERINFO}@127.0.0.1:{port}/relay{query}#SENTINEL-FRAG-9d4c"
    if case == "hash":
        return f"ws://SENTINEL-USER:SENTINEL-PW1#SENTINEL-PW2@127.0.0.1:{port}/relay"
    if case == "slash":
        return f"ws://SENTINEL-USER:SENTINEL-PW1/SENTINEL-PW2@127.0.0.1:{port}/x"
    return f"ws://{_USERINFO}@127.0.0.1:{port}/relay{query}"


@pytest.mark.parametrize(
    "case", ["refused", "closed", "redirect", "unreachable", "hash", "slash"]
)
def test_secrets_stay_out_of_logs_and_tool_errors(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    case: str,
) -> None:
    """A refusal at the handshake and with the close code, a redirect, a
    server that is not reachable, and two URLs with a "#" or a "/" in the
    password. The URL is from an option and the credential from the
    environment, so that the log also gives the mixed-sources warning."""
    monkeypatch.setenv("SUBSPACE_TOKEN", "SENTINEL-TOKEN-7c1d")
    fakes = {
        "refused": FakeServer(401),
        "closed": FakeServer(close_code=wsproto.UNAUTHORIZED_CLOSE_CODE),
        "redirect": FakeServer(302, "ws://127.0.0.1:9/?SENTINEL-QUERY-5a2f"),
    }

    async def scenario() -> tuple[list[str], int]:
        texts: list[str] = []
        async with running(fakes.get(case, FakeServer(401))) as server:
            port = server.port if case in fakes else 9
            monkeypatch.setenv("SUBSPACE_OPTION_WS_URL", _secret_url(case, port))
            async with running_client(relay, monkeypatch) as client:
                if case in ("refused", "closed"):
                    await _until(lambda: client.credential_error is not None)  # type: ignore[attr-defined]
                    with anyio.fail_after(5):
                        texts.append(await relay.list_agents())
                # A tool call waits 8 s before it gives this error.
                texts.append(relay._daemon_unreachable_message())
                texts.append(client.display_url())  # type: ignore[attr-defined]
        return texts, port

    texts, port = anyio.run(scenario)
    err = capsys.readouterr().err
    assert "warning:" in err
    if case in ("hash", "slash"):
        assert "<URL with user information>" in texts[0]
        assert "is not a valid WebSocket URL" in err
    else:
        assert f"ws://127.0.0.1:{port}/" in texts[0]
    if case in ("refused", "closed"):
        assert "refused the credential from SUBSPACE_TOKEN" in texts[0]
    elif case == "redirect":
        assert "HTTP 302" in err
    assert "SENTINEL" not in err
    for text in texts:
        assert "SENTINEL" not in text


def test_secret_in_deprecated_host_stays_out_of_logs(
    relay: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("NOTIFICATIONS_WS_HOST", f"{_USERINFO}@127.0.0.1")
    monkeypatch.setenv("NOTIFICATIONS_WS_PORT", "9")
    monkeypatch.setenv("SUBSPACE_TOKEN", "SENTINEL-TOKEN-7c1d")
    client = relay.DaemonClient()
    monkeypatch.setattr(relay, "DAEMON", client)
    settings = client._resolve_settings()
    assert settings.url == f"ws://{_USERINFO}@127.0.0.1:9"
    text = relay._daemon_unreachable_message()
    err = capsys.readouterr().err
    assert "use SUBSPACE_WS_URL=ws://127.0.0.1:9" in err
    assert "SENTINEL" not in err
    assert "SENTINEL" not in text


def test_settings_repr_hides_the_credential(
    home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUBSPACE_TOKEN", "SENTINEL-TOKEN-7c1d")
    _write(_server_token(home), "SENTINEL-TOKEN-7c1d")
    assert "SENTINEL" not in repr(connection.resolve())
    assert "SENTINEL" not in repr(connection.read_token_file(_server_token(home)))


@pytest.mark.parametrize(
    ("url", "shown"),
    [
        ("wss://u:p@host.example:8443/a?b=c#d", "wss://host.example:8443/a"),
        ("ws://[::1]:8137/", "ws://[::1]:8137/"),
        ("wss://alice:pa#ss@host.example", "<URL with user information>"),
        ("wss://alice:pa/ss@host.example/x", "<URL with user information>"),
        ("wss://alice:pa?ss@host.example", "<URL with user information>"),
        ("wss://host.example", "wss://host.example"),
        ("not a url", "<invalid URL>"),
        ("ws//u:p@host", "<invalid URL>"),
    ],
)
def test_display_url(url: str, shown: str) -> None:
    assert connection.display_url(url) == shown


# --------------------------------------------------------------------------- #
# manifest
# --------------------------------------------------------------------------- #


def _manifests() -> tuple[dict, dict]:
    mcp = json.loads((PLUGIN / ".mcp.json").read_text())
    plugin = json.loads((PLUGIN / ".claude-plugin" / "plugin.json").read_text())
    return mcp, plugin


def test_mcp_json_maps_exactly_the_options() -> None:
    mcp, _ = _manifests()
    env = mcp["mcpServers"]["subspace"]["env"]
    assert env == {
        "SUBSPACE_OPTION_WS_URL": "${user_config.server_url}",
        "SUBSPACE_OPTION_TOKEN": "${user_config.token}",
    }
    assert env.keys() == {connection.OPTION_URL, connection.OPTION_TOKEN}
    for name in env:
        assert name not in ("SUBSPACE_TOKEN", "SUBSPACE_WS_URL")
        assert not name.startswith("NOTIFICATIONS_")


def test_each_referenced_option_is_declared() -> None:
    mcp, plugin = _manifests()
    referenced = set(re.findall(r"\$\{user_config\.([^}]+)\}", json.dumps(mcp)))
    assert referenced == {"server_url", "token"}
    assert referenced <= set(plugin["userConfig"])


def test_token_option_is_sensitive() -> None:
    _, plugin = _manifests()
    options = plugin["userConfig"]
    assert options["token"]["sensitive"] is True
    assert not options["server_url"].get("sensitive", False)
    assert options["token"]["required"] is False
    assert options["server_url"]["required"] is False
