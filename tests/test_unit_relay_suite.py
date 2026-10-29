# vim: filetype=python
"""Run the relay plugin's tests (tests/relay/) in their own environment.

The relay uses version 1 of the MCP SDK and the gateway uses version 2, so the
relay tests cannot run in this environment.
"""

import subprocess
import sys
from pathlib import Path

RUNNER = Path(__file__).resolve().parent / "relay" / "run.py"
# Same Python version as this run, so `UV_PYTHON=3.12` also covers the relay.
_PYTHON = f"{sys.version_info.major}.{sys.version_info.minor}"


def test_relay_unit_tests() -> None:
    result = subprocess.run(
        ["uv", "run", "-qs", "--python", _PYTHON, str(RUNNER), "-q"],
        capture_output=True,
        text=True,
        check=False,
        timeout=1200,
    )
    assert result.returncode == 0, result.stdout + result.stderr
