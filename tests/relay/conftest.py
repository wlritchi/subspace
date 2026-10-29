# vim: filetype=python
"""Pytest configuration for the relay plugin tests: put the plugin's lib/ on the
path and register markers."""

import sys
from pathlib import Path

_PLUGIN_LIB = Path(__file__).resolve().parents[2] / "plugin" / "subspace" / "lib"
if str(_PLUGIN_LIB) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_LIB))


def pytest_configure(config) -> None:
    config.addinivalue_line(
        "markers", "slow: end-to-end tests that spawn the server and the relay"
    )
