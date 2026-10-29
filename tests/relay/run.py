#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Run the relay plugin's unit tests.

    uv run -qs tests/relay/run.py            # all relay tests
    uv run -qs tests/relay/run.py -k hold    # filter

The relay uses version 1 of the MCP SDK and the gateway uses version 2, so these
tests have their own environment. tests/run.py runs them through
tests/test_unit_relay_suite.py.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest>=8",
#     "mcp>=1.28,<2",
#     "anyio",
#     "websockets",
# ]
# ///

import sys
from pathlib import Path

import pytest

if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    # Stop conftest discovery here: tests/conftest.py puts the server's lib/ on the
    # path, and it has a messaging.py of its own.
    raise SystemExit(
        pytest.main(
            [
                str(here),
                "--rootdir",
                str(here),
                "--confcutdir",
                str(here),
                "-p",
                "no:cacheprovider",
                *sys.argv[1:],
            ]
        )
    )
