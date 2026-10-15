"""Execute release checkout steps against a branch that advanced in the queue."""

import os
from pathlib import Path
import subprocess
import textwrap

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("workflow", ["build.yaml", "protocol.yaml"])
def test_checkout_uses_event_commit(tmp_path, workflow):
    remote = tmp_path / "repo.git"
    remote.mkdir()

    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(remote), *args], text=True
        ).strip()

    git("init", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    git("-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "fix: event")
    event = git("rev-parse", "HEAD")
    git("tag", "1.0.0")
    git(
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--allow-empty",
        "-m",
        "later [skip release]",
    )
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    source = (ROOT / ".forgejo/workflows" / workflow).read_text()
    step = textwrap.dedent(
        source.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
    )
    subprocess.run(
        ["sh", "-c", step],
        cwd=checkout,
        check=True,
        env={
            **os.environ,
            "GITHUB_SERVER_URL": str(tmp_path),
            "GITHUB_REPOSITORY": "repo",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_SHA": event,
            "GH_TOKEN": "test",
        },
    )
    assert (
        subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=checkout, text=True
        ).strip()
        == event
    )
    assert (
        subprocess.check_output(
            ["git", "tag", "--merged", "HEAD"], cwd=checkout, text=True
        ).strip()
        == "1.0.0"
    )
    assert (
        subprocess.run(["git", "symbolic-ref", "-q", "HEAD"], cwd=checkout).returncode
        != 0
    )
