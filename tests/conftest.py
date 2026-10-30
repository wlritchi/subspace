# vim: filetype=python
"""Pytest configuration: put lib/ on the path and register markers."""

import os
import sys
from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).resolve().parent
_LIB = _TESTS_DIR.parent / "lib"
for _p in (str(_LIB), str(_TESTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# The relay plugin tests need another MCP SDK version. test_unit_relay_suite.py
# runs them in their own environment.
collect_ignore = ["relay"]

# CI runners set GITHUB_* and FORGEJO_* (for example FORGEJO_API_URL), and the
# forge clients read these as their configuration. The harnesses remove them
# from the servers they start; this does the same for in-process tests.
_FORGE_ENV_PREFIXES = ("GITHUB_", "FORGEJO_")


@pytest.fixture(autouse=True)
def _no_forge_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith(_FORGE_ENV_PREFIXES):
            monkeypatch.delenv(name)


def pytest_configure(config) -> None:
    config.addinivalue_line("markers", "slow: end-to-end tests that spawn the server")
