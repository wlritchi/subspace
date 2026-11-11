# vim: filetype=python
"""The server URL that the relay connects to, and the credential that it sends.

The design is docs/specs/2026-11-11-relay-credentials.md. Each source is a
step. The first step that gives a value that is not empty wins. The relay calls
resolve() before each connection attempt, so that a change to a token file or
a link applies at the next attempt.

The relay must not import server code. So this module has its own copy of the
server's data-dir rule (lib/settings.py in the subspace repo).

stdlib only.
"""

import os
import stat
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

import wsproto

# A literal address, so that no name is resolved. The port is the port that
# the server listens on when it has no configuration.
DEFAULT_URL = f"ws://{wsproto.DEFAULT_HOST}:{wsproto.DEFAULT_PORT}"

# Claude Code sets these from the plugin options (.mcp.json). It always sets
# them, and an unset option gives an empty string.
OPTION_URL = "SUBSPACE_OPTION_WS_URL"
OPTION_TOKEN = "SUBSPACE_OPTION_TOKEN"

OPTION = "option"
ENV = "env"
FILE = "file"
DEFAULT = "default"

MISSING = "it does not exist"
DANGLING = "it is a dangling link"
NOT_REGULAR = "it is not a regular file"
UNREADABLE = "it cannot be read"
EMPTY = "it is empty"


@dataclass(frozen=True)
class Source:
    """Where a value came from. `label` names it in logs and errors."""

    label: str
    kind: str  # OPTION, ENV, FILE, or DEFAULT


@dataclass(frozen=True)
class TokenFile:
    """The result of a read of a token file. `problem` is None if the file is
    usable."""

    path: Path
    value: str | None = field(repr=False)
    problem: str | None


@dataclass(frozen=True)
class Settings:
    url: str
    url_source: Source
    token: str | None = field(repr=False)
    token_source: Source | None
    # The read of the server's token file. None if a source gives a URL: then
    # the relay does not read that file.
    server_token_file: TokenFile | None
    # One line for each deprecated source in use, with its replacement.
    deprecations: tuple[str, ...]
    # True if the credential is from the server's token file.
    uses_server_token_file: bool = False

    @property
    def mixed_sources(self) -> bool:
        """True if one value is from an option and the other is from the
        environment or a file. The default URL is not a source."""
        if self.token_source is None or self.url_source.kind == DEFAULT:
            return False
        kinds = {self.url_source.kind, self.token_source.kind}
        return OPTION in kinds and len(kinds) == 2


def _env(name: str) -> str | None:
    """The value of `name`. An empty value counts as not set."""
    return os.environ.get(name) or None


def read_token_file(path: Path) -> TokenFile:
    """Read a token file. The file is usable only if it is a regular file
    after links are followed, the relay can read it, and it is not empty."""
    try:
        mode = os.stat(path).st_mode
    except (FileNotFoundError, NotADirectoryError):
        return TokenFile(path, None, DANGLING if os.path.islink(path) else MISSING)
    except OSError:
        return TokenFile(path, None, UNREADABLE)
    if not stat.S_ISREG(mode):
        return TokenFile(path, None, NOT_REGULAR)
    try:
        value = path.read_text().strip()
    except (OSError, UnicodeDecodeError):
        return TokenFile(path, None, UNREADABLE)
    if not value:
        return TokenFile(path, None, EMPTY)
    return TokenFile(path, value, None)


def legacy_token_path() -> Path:
    """<NOTIFICATIONS_DATA_DIR>/token, default ~/.claude/notifications/token."""
    base = _env("NOTIFICATIONS_DATA_DIR")
    return (Path(base) if base else Path.home() / ".claude" / "notifications") / "token"


def server_token_path() -> Path:
    """<data dir>/token, with the server's data-dir rule: SUBSPACE_DATA_DIR,
    else $XDG_DATA_HOME/subspace, else ~/.local/share/subspace."""
    base = _env("SUBSPACE_DATA_DIR")
    if base:
        return Path(base) / "token"
    xdg = _env("XDG_DATA_HOME")
    root = Path(xdg) if xdg else Path.home() / ".local" / "share"
    return root / "subspace" / "token"


def _resolve_url(deprecations: list[str]) -> tuple[str, Source]:
    value = _env(OPTION_URL)
    if value:
        return value, Source("the server_url option", OPTION)
    value = _env("SUBSPACE_WS_URL")
    if value:
        return value, Source("SUBSPACE_WS_URL", ENV)
    value = _env("NOTIFICATIONS_WS_URL")
    if value:
        deprecations.append("NOTIFICATIONS_WS_URL is deprecated: use SUBSPACE_WS_URL")
        return value, Source("NOTIFICATIONS_WS_URL", ENV)
    host = _env("NOTIFICATIONS_WS_HOST")
    raw_port = _env("NOTIFICATIONS_WS_PORT")
    if host or raw_port:
        port = wsproto.DEFAULT_PORT
        if raw_port:
            try:
                port = int(raw_port)
            except ValueError:
                pass
        url = f"ws://{host or wsproto.DEFAULT_HOST}:{port}"
        names = [
            name
            for name, value in (
                ("NOTIFICATIONS_WS_HOST", host),
                ("NOTIFICATIONS_WS_PORT", raw_port),
            )
            if value
        ]
        label = " and ".join(names)
        verb = "are" if len(names) == 2 else "is"
        deprecations.append(f"{label} {verb} deprecated: use SUBSPACE_WS_URL={url}")
        return url, Source(label, ENV)
    return DEFAULT_URL, Source("the default URL", DEFAULT)


def resolve() -> Settings:
    """Resolve the URL and the credential from all sources, in order."""
    deprecations: list[str] = []
    url, url_source = _resolve_url(deprecations)
    # The server's token file holds the master token of the server on this
    # machine. Only the default URL is sure to reach that server: a URL
    # setting can name a different server, also if its host is local.
    server_file = (
        read_token_file(server_token_path()) if url_source.kind == DEFAULT else None
    )

    def settings(
        token: str | None, source: Source | None, *, server: bool = False
    ) -> Settings:
        return Settings(
            url=url,
            url_source=url_source,
            token=token,
            token_source=source,
            server_token_file=server_file,
            deprecations=tuple(deprecations),
            uses_server_token_file=server,
        )

    value = _env(OPTION_TOKEN)
    if value:
        return settings(value, Source("the token option", OPTION))
    value = _env("SUBSPACE_TOKEN")
    if value:
        return settings(value, Source("SUBSPACE_TOKEN", ENV))
    value = _env("NOTIFICATIONS_TOKEN")
    if value:
        deprecations.append(
            "NOTIFICATIONS_TOKEN is deprecated: use the token option or SUBSPACE_TOKEN"
        )
        return settings(value, Source("NOTIFICATIONS_TOKEN", ENV))
    legacy = read_token_file(legacy_token_path())
    if legacy.value is not None:
        deprecations.append(
            f"the token file {legacy.path} is deprecated: use the token option or "
            "SUBSPACE_TOKEN"
        )
        return settings(legacy.value, Source(f"the token file ({legacy.path})", FILE))
    if server_file is not None and server_file.value is not None:
        return settings(
            server_file.value,
            Source(f"the server's token file ({server_file.path})", FILE),
            server=True,
        )
    return settings(None, None)


def display_url(url: str) -> str:
    """`url` without its user information, query, and fragment, for logs and
    errors. These parts can hold a secret."""
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return "<invalid URL>"
    if not parts.scheme or not parts.netloc:
        # The path of a URL with no host can hold the user information.
        return "<invalid URL>"
    host = parts.netloc.rpartition("@")[2]
    return urllib.parse.urlunsplit((parts.scheme, host, parts.path, "", ""))


def source_lines(settings: Settings) -> list[str]:
    """The log lines that give the source of each value. They never include
    the credential."""
    credential = (
        f"credential from {settings.token_source.label}"
        if settings.token_source is not None
        else "no credential"
    )
    lines = [f"URL from {settings.url_source.label}, {credential}"]
    if settings.mixed_sources and settings.token_source is not None:
        lines.append(
            f"warning: the URL is from {settings.url_source.label}, but the "
            f"credential is from {settings.token_source.label}. Make sure that "
            f"the credential is for the server at {display_url(settings.url)}"
        )
    lines.extend(settings.deprecations)
    return lines


def no_credential_error(settings: Settings) -> str:
    """The error when no source gives a credential."""
    text = (
        "no credential. Set the plugin's token option "
        "(/plugin configure subspace@subspace), or SUBSPACE_TOKEN."
    )
    server_file = settings.server_token_file
    if server_file is not None:
        text += (
            " The relay also read the server's token file "
            f"({server_file.path}): {server_file.problem}."
        )
        if server_file.problem == MISSING:
            text += " For a server on this machine, start the server first."
    return text


def refused_error(settings: Settings) -> str:
    """The error when the server refuses the credential at the handshake."""
    source = settings.token_source.label if settings.token_source else "no source"
    return (
        f"the server at {display_url(settings.url)} refused the credential from "
        f"{source}."
    )
