# vim: filetype=python
"""The hooks' side of the relay's local socket (see session_state.ipc_path):
newline-delimited JSON objects.

stdlib only, so the hooks can run without building a dependency environment.
"""

import asyncio
import contextlib
import json
import time
from pathlib import Path

CONNECT_POLL_SECONDS = 0.1
# A connection to a local socket is immediate when the relay is up. This lets
# a caller with no time left try once.
CONNECT_ATTEMPT_SECONDS = 1.0


async def connect(
    path: Path, deadline: float
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
    """Connect to the relay's socket. Try at least once, and try again until
    the monotonic `deadline`: the socket appears only when the relay starts."""
    while True:
        timeout = max(deadline - time.monotonic(), CONNECT_ATTEMPT_SECONDS)
        try:
            return await asyncio.wait_for(
                asyncio.open_unix_connection(str(path)), timeout=timeout
            )
        except (OSError, TimeoutError):
            if time.monotonic() + CONNECT_POLL_SECONDS >= deadline:
                return None
            await asyncio.sleep(CONNECT_POLL_SECONDS)


async def send(writer: asyncio.StreamWriter, message: dict) -> None:
    writer.write(json.dumps(message).encode() + b"\n")
    await writer.drain()


async def close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()
