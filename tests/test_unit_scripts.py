"""Regression tests for administrative scripts."""

from pathlib import Path
import subprocess
import sys

import pytest

import accounts

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("expiry", ["2027-01-01T00:00:00Z", "2027-01-01t00:00:00.123z", "2027-01-01T00:00:00+05:30"])
def test_credential_expiry_round_trips(expiry):
    result = subprocess.run([sys.executable, str(ROOT / "scripts/new-credential.py"), "ci", "--expires", expiry], text=True, capture_output=True, check=True)
    parsed = accounts.parse('[[account]]\nid = "ci"\n' + result.stderr, "ci")
    assert parsed["ci"].credentials
    assert accounts.parse_token(result.stdout.strip()) is not None


@pytest.mark.parametrize("expiry", ["20270101T000000Z", "2027-01-01", "2027-01-01T00:00:00", "2027-01-01X00:00:00Z", "2027-02-30T00:00:00Z"])
def test_credential_rejects_invalid_expiry(expiry):
    result = subprocess.run([sys.executable, str(ROOT / "scripts/new-credential.py"), "ci", "--expires", expiry], text=True, capture_output=True)
    assert result.returncode == 2
    assert not result.stdout
    assert "not an RFC 3339 date-time" in result.stderr
