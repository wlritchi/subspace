#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Run the server test suite.

    uv run -qs tests/run.py            # all tests
    uv run -qs tests/run.py -k diff    # filter

The test deps live here (PEP 723) so there is no project to set up. Nothing hits the
network at test time (local fake GitHub GraphQL and Forgejo servers are used).
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest>=8",
#     "anyio",
#     "websockets",
#     "httpx",
#     "tzdata",
#     "subspace-protocol",
# ]
#
# [tool.uv.sources]
# subspace-protocol = { path = "../protocol", editable = true }
# ///

import sys
from pathlib import Path

import pytest

if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    raise SystemExit(pytest.main([str(here), *sys.argv[1:]]))
