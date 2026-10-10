# vim: filetype=python
"""Server settings from the environment: bind address, data dir, and token.

The wire protocol itself is in the subspace-protocol package (../protocol).
"""

import os
import secrets
import time
from pathlib import Path

import subspace_protocol as proto


def host() -> str:
    return os.environ.get("SUBSPACE_WS_HOST", proto.DEFAULT_HOST)


def port() -> int:
    raw = os.environ.get("SUBSPACE_WS_PORT")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return proto.DEFAULT_PORT


def data_dir() -> Path:
    """Root persistence directory: SUBSPACE_DATA_DIR, else $XDG_DATA_HOME/subspace."""
    base = os.environ.get("SUBSPACE_DATA_DIR")
    if base:
        return Path(base)
    xdg = os.environ.get("XDG_DATA_HOME")
    return (Path(xdg) if xdg else Path.home() / ".local" / "share") / "subspace"


def token() -> str:
    """Shared secret that clients must present to connect.

    Returns SUBSPACE_TOKEN if set. Otherwise reads <data dir>/token, and
    creates it (mode 0600) on first use. Concurrent callers get the same value.
    """
    env = os.environ.get("SUBSPACE_TOKEN")
    if env:
        return env
    path = data_dir() / "token"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return _read_token(path)
    try:
        value = secrets.token_urlsafe(32)
        os.write(fd, value.encode())
    finally:
        os.close(fd)
    return value


def _read_token(path: Path) -> str:
    # The O_EXCL create winner may not have written the secret yet; the file exists
    # but is momentarily empty. Retry briefly rather than return a blank token.
    for _ in range(100):
        try:
            value = path.read_text().strip()
        except OSError:
            value = ""
        if value:
            return value
        time.sleep(0.01)
    return path.read_text().strip()
