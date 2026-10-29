# vim: filetype=python
"""Pytest configuration: put lib/ on the path and register markers."""

import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent
_LIB = _TESTS_DIR.parent / "lib"
for _p in (str(_LIB), str(_TESTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# The relay plugin tests need another MCP SDK version. test_unit_relay_suite.py
# runs them in their own environment.
collect_ignore = ["relay"]


def pytest_configure(config) -> None:
    config.addinivalue_line("markers", "slow: end-to-end tests that spawn the server")
