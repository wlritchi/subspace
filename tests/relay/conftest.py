# vim: filetype=python
"""Pytest configuration for the relay plugin tests: put the plugin's lib/ on the path."""

import sys
from pathlib import Path

_PLUGIN_LIB = Path(__file__).resolve().parents[2] / "plugin" / "subspace" / "lib"
if str(_PLUGIN_LIB) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_LIB))
