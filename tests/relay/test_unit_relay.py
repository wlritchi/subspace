# vim: filetype=python
"""Unit tests for the relay plugin: the session-id state file, the token and URL
settings, channel detection, and the relay's delivery, backoff, and resume hold."""

import importlib.util
import json
import os
import re
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import anyio
import pytest
from anyio.streams.buffered import BufferedByteReceiveStream

import channel_detect as cd
import session_state
import wsproto

DEAD_PID = 2147483647  # never a live pid
RELAY = (
    Path(__file__).resolve().parents[2]
    / "plugin"
    / "subspace"
    / "mcp"
    / "subspace-relay.py"
)


def seed_channel_log(cache_dir: Path, *, registered: bool, project_dir: str) -> None:
    """Write a fake Claude Code MCP log so the relay detects push or pull mode."""
    directory = (
        cache_dir
        / "claude-cli-nodejs"
        / re.sub(r"[/.]", "-", project_dir)
        / "mcp-logs-plugin-subspace-subspace"
    )
    directory.mkdir(parents=True, exist_ok=True)
    marker = (
        "Channel notifications registered"
        if registered
        else "Channel notifications skipped: server not in --channels list"
    )
    log = directory / "2026-01-01T00-00-00-000Z.jsonl"
    log.write_text(json.dumps({"message": marker}) + "\n")
    # Detection ignores logs older than the relay's start, so date it ahead.
    future = time.time() + 3600
    os.utime(log, (future, future))


# --------------------------------------------------------------------------- #
# session_state
# --------------------------------------------------------------------------- #


def test_secure_state_dir_makes_a_private_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    directory = session_state.secure_state_dir()
    assert directory == tmp_path / "claude-notifications"
    assert directory.is_dir()
    assert directory.stat().st_mode & 0o777 == 0o700


def test_secure_state_dir_refuses_wrong_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    directory = tmp_path / "claude-notifications"
    directory.mkdir(mode=0o750)
    assert session_state.secure_state_dir() is None
    assert session_state.write_session(4242, "sid") is None
    assert not (directory / "4242.json").exists()


def test_secure_state_dir_refuses_wrong_owner(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    (tmp_path / "claude-notifications").mkdir(mode=0o700)
    other_uid = os.getuid() + 1
    monkeypatch.setattr(session_state.os, "getuid", lambda: other_uid)
    assert session_state.secure_state_dir() is None


def test_read_session_refuses_a_dir_that_is_not_private(tmp_path, monkeypatch):
    """A planted state file in a directory another user made must not choose
    the session the relay registers: the env id wins."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv(session_state.SESSION_ID_ENV_VAR, "env-sid")
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    directory = tmp_path / "claude-notifications"
    directory.mkdir(mode=0o777)
    (directory / "4242.json").write_text('{"session_id": "planted"}')
    assert session_state.read_session(4242) is None
    assert session_state.effective_session_id() == (
        "env-sid",
        f"env ({session_state.SESSION_ID_ENV_VAR})",
    )


def test_secure_state_dir_refuses_symlink(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    target = tmp_path / "elsewhere"
    target.mkdir(mode=0o700)
    (tmp_path / "claude-notifications").symlink_to(target)
    assert session_state.secure_state_dir() is None


def test_effective_session_id_prefers_state_file(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv(session_state.SESSION_ID_ENV_VAR, "env-sid")
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    session_state.write_session(4242, "real-sid", "resume")
    sid, source = session_state.effective_session_id()
    assert sid == "real-sid"
    assert "state file" in source


def test_effective_session_id_env_fallback(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv(session_state.SESSION_ID_ENV_VAR, "env-sid")
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: None)
    sid, source = session_state.effective_session_id()
    assert sid == "env-sid"
    assert source.startswith("env")


def test_write_read_roundtrip_and_reap(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    session_state.write_session(DEAD_PID, "ghost")
    assert session_state.read_session(DEAD_PID) == "ghost"
    session_state.reap_stale()  # DEAD_PID is not alive -> file removed
    assert session_state.read_session(DEAD_PID) is None


def test_read_session_accepts_matching_start_time(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 111)
    session_state.write_session(4242, "sid")  # records start_time=111
    assert session_state.read_session(4242) == "sid"  # still 111 -> match


def test_read_session_rejects_recycled_pid(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 111)
    session_state.write_session(4242, "sid")  # records start_time=111
    # A new process now holds pid 4242 with a different start time.
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 222)
    assert session_state.read_session(4242) is None  # stale -> ignored


def test_read_session_backward_compat_no_start_time(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    # Older-format file without a start_time key is still accepted.
    path = session_state._state_path(4242)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text('{"session_id": "old-sid", "claude_pid": 4242}')
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 999)
    assert session_state.read_session(4242) == "old-sid"


def test_effective_session_id_env_fallback_on_recycled_pid(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv(session_state.SESSION_ID_ENV_VAR, "env-sid")
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 111)
    session_state.write_session(4242, "real-sid", "resume")  # records 111
    # Pid recycled: the process now holding 4242 has a different start time.
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 222)
    sid, source = session_state.effective_session_id()
    assert sid == "env-sid"
    assert source.startswith("env")


# --------------------------------------------------------------------------- #
# wsproto shared token
# --------------------------------------------------------------------------- #


def test_token_autocreates_stable_and_0600(tmp_path, monkeypatch):
    monkeypatch.delenv("NOTIFICATIONS_TOKEN", raising=False)
    monkeypatch.setenv("NOTIFICATIONS_DATA_DIR", str(tmp_path))
    first = wsproto.token()
    assert first
    token_file = tmp_path / "token"
    assert token_file.read_text().strip() == first
    assert (token_file.stat().st_mode & 0o777) == 0o600
    assert wsproto.token() == first  # stable on repeated calls


def test_token_env_overrides_file(tmp_path, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NOTIFICATIONS_TOKEN", "explicit-secret")
    assert wsproto.token() == "explicit-secret"
    assert not (tmp_path / "token").exists()  # env path never touches the file


def test_uri_builds_from_host_port_by_default(monkeypatch):
    monkeypatch.delenv("NOTIFICATIONS_WS_URL", raising=False)
    monkeypatch.setenv("NOTIFICATIONS_WS_HOST", "127.0.0.1")
    monkeypatch.setenv("NOTIFICATIONS_WS_PORT", "8137")
    assert wsproto.uri() == "ws://127.0.0.1:8137"


def test_uri_full_url_override_wins(monkeypatch):
    # A remote daemon behind a TLS-terminating ingress: the relay connects via wss://
    # and the host/port are ignored entirely.
    monkeypatch.setenv("NOTIFICATIONS_WS_HOST", "127.0.0.1")
    monkeypatch.setenv("NOTIFICATIONS_WS_PORT", "8137")
    monkeypatch.setenv("NOTIFICATIONS_WS_URL", "wss://subspace.example.com")
    assert wsproto.uri() == "wss://subspace.example.com"


# --------------------------------------------------------------------------- #
# channel-load detection
# --------------------------------------------------------------------------- #


def _write_log(cache_root, project_dir, server_dir, ts, content):
    directory = (
        cache_root / "claude-cli-nodejs" / cd.encode_cwd(project_dir) / server_dir
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{ts}.jsonl"
    path.write_text(content)
    return path


class TestChannelDetect:
    PROJECT = "/home/u/proj"
    SERVER_DIR = "mcp-logs-plugin-subspace-subspace"

    def test_encode_cwd(self):
        assert cd.encode_cwd("/home/u/my-repo") == "-home-u-my-repo"
        assert cd.encode_cwd("/home/u/.claude/x") == "-home-u--claude-x"

    def test_cache_root_override_wins_over_darwin(self, tmp_path, monkeypatch):
        # The explicit override must bypass the platform branch entirely, so even on
        # macOS (simulated) detection points at the given cache root, not ~/Library/Caches.
        monkeypatch.setattr(cd.sys, "platform", "darwin")
        monkeypatch.setenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", str(tmp_path))
        assert cd._cache_root() == tmp_path

    def test_cache_root_without_override_honors_xdg(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", raising=False)
        monkeypatch.setattr(cd.sys, "platform", "linux")
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        assert cd._cache_root() == tmp_path

    def test_cache_root_without_override_falls_back_to_dot_cache(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.delenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", raising=False)
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        monkeypatch.setattr(cd.sys, "platform", "linux")
        assert cd._cache_root() == Path.home() / ".cache"

    def test_override_makes_detection_work_on_darwin(self, tmp_path, monkeypatch):
        # Full round-trip proving the seam works on macOS: a seeded "registered" log
        # under the override cache root is found even on a simulated darwin platform.
        monkeypatch.setattr(cd.sys, "platform", "darwin")
        monkeypatch.setenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", str(tmp_path))
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications registered"}\n',
        )
        assert cd.detect_channel_mode("subspace", self.PROJECT) == cd.REGISTERED

    def test_registered(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications registered"}\n',
        )
        assert cd.detect_channel_mode("subspace", self.PROJECT) == cd.REGISTERED

    def test_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications skipped: not in --channels list"}\n',
        )
        assert cd.detect_channel_mode("subspace", self.PROJECT) == cd.SKIPPED

    def test_unknown_when_no_logs_or_no_marker(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        assert cd.detect_channel_mode("subspace", self.PROJECT) == cd.UNKNOWN  # no dir
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"some other line"}\n',
        )
        assert (
            cd.detect_channel_mode("subspace", self.PROJECT) == cd.UNKNOWN
        )  # no marker
        assert cd.detect_channel_mode("subspace", None) == cd.UNKNOWN  # no project dir

    def test_newer_than_filters_stale_logs(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        log = _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications registered"}\n',
        )
        old = time.time() - 10_000
        os.utime(log, (old, old))
        assert (
            cd.detect_channel_mode("subspace", self.PROJECT, newer_than=time.time())
            == cd.UNKNOWN
        )
        assert (
            cd.detect_channel_mode("subspace", self.PROJECT, newer_than=0)
            == cd.REGISTERED
        )

    def test_latest_log_wins(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        old = _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications skipped: x"}\n',
        )
        os.utime(old, (time.time() - 100, time.time() - 100))
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T01-00-00Z",
            '{"message":"Channel notifications registered"}\n',
        )
        assert cd.detect_channel_mode("subspace", self.PROJECT) == cd.REGISTERED

    def test_by_session_matches_across_cwd_dirs(self, tmp_path, monkeypatch):
        # Regression (flux's worktree pull-mode bug): Claude Code keys the marker log by
        # the *session* cwd (here a worktree path), which is not the relay's process cwd.
        # A cwd-scoped probe of the process cwd misses it; sessionId matching finds it.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            "/home/u/proj/.claude/worktrees/wt",  # session (worktree) cwd, not proc cwd
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications registered","sessionId":"sess-A",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        # the pre-fix path (probe the process cwd) reads /home/u/proj and sees nothing
        assert cd.detect_channel_mode("subspace", "/home/u/proj") == cd.UNKNOWN
        # sessionId matching finds it regardless of which cwd dir it landed under
        assert cd.detect_channel_mode_by_session("subspace", "sess-A") == cd.REGISTERED

    def test_by_session_ignores_other_sessions_marker(self, tmp_path, monkeypatch):
        # A claimed-spare session's marker sharing the workspace cwd must not be
        # misattributed to my session (flux's 7e459fc3 case).
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            "/home/u/workspace",
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications registered","sessionId":"spare-1",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        # my session has no marker anywhere -> UNKNOWN, not the spare's REGISTERED
        assert cd.detect_channel_mode_by_session("subspace", "mine-2") == cd.UNKNOWN
        # the spare's own id still resolves to its marker
        assert cd.detect_channel_mode_by_session("subspace", "spare-1") == cd.REGISTERED

    def test_by_session_latest_marker_wins(self, tmp_path, monkeypatch):
        # Latest marker for the id wins by line timestamp (a resume that flips a session
        # from channel to non-channel is honored), across different cwd dirs.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            "/home/u/a",
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications registered","sessionId":"s",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        _write_log(
            tmp_path,
            "/home/u/b",
            self.SERVER_DIR,
            "2026-06-26T01-00-00Z",
            '{"debug":"Channel notifications skipped: x","sessionId":"s",'
            '"timestamp":"2026-06-26T01:00:00.000Z"}\n',
        )
        assert cd.detect_channel_mode_by_session("subspace", "s") == cd.SKIPPED

    def test_by_session_newer_than_ignores_resumed_sessions_old_marker(
        self, tmp_path, monkeypatch
    ):
        # Regression: a session resumed with --continue keeps its id. An earlier run
        # logged 'skipped'; this run's 'registered' line comes only after the relay
        # starts. Until then, the old marker must not give a verdict.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-10-03T15-12-34-013Z",
            '{"debug":"Channel notifications skipped: not in --channels list",'
            '"sessionId":"s","timestamp":"2026-10-03T15:12:34.872Z"}\n',
        )
        started = datetime(2026, 10, 3, 18, 29, 54, tzinfo=UTC).timestamp()
        assert cd.detect_channel_mode_by_session("subspace", "s") == cd.SKIPPED
        assert (
            cd.detect_channel_mode_by_session("subspace", "s", newer_than=started)
            == cd.UNKNOWN
        )
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-10-03T18-29-54-440Z",
            '{"debug":"Channel notifications registered","sessionId":"s",'
            '"timestamp":"2026-10-03T18:29:55.929Z"}\n',
        )
        assert (
            cd.detect_channel_mode_by_session("subspace", "s", newer_than=started)
            == cd.REGISTERED
        )

    def test_by_session_accepts_any_of_several_ids(self, tmp_path, monkeypatch):
        # A resumed session can run under a temporary env id and a state-file id;
        # the harness can tag the marker with either one.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications registered","sessionId":"temp-1",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        assert (
            cd.detect_channel_mode_by_session("subspace", ["real-1", "temp-1"])
            == cd.REGISTERED
        )
        assert cd.detect_channel_mode_by_session("subspace", ["real-1"]) == cd.UNKNOWN
        assert cd.detect_channel_mode_by_session("subspace", []) == cd.UNKNOWN

    def test_cwd_probe_ignore_session_tagged(self, tmp_path, monkeypatch):
        # Another session in the same cwd logged 'registered'. With its own ids
        # known, the relay must not take that marker from the cwd probe. Untagged
        # lines still count.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications registered","sessionId":"other",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        assert cd.detect_channel_mode("subspace", self.PROJECT) == cd.REGISTERED
        assert (
            cd.detect_channel_mode("subspace", self.PROJECT, ignore_session_tagged=True)
            == cd.UNKNOWN
        )
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications skipped: x"}\n'
            '{"debug":"Channel notifications registered","sessionId":"other",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        assert cd.detect_channel_mode_ex(
            "subspace", self.PROJECT, ignore_session_tagged=True
        ) == (cd.SKIPPED, "x")

    def test_by_session_requires_real_id_field(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        assert cd.detect_channel_mode_by_session("subspace", None) == cd.UNKNOWN
        assert (
            cd.detect_channel_mode_by_session("subspace", "sess-A") == cd.UNKNOWN
        )  # no dir yet
        # a line that only MENTIONS the id in free text (not as the sessionId field)
        # must not match — else a log referencing another session could misfire.
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications registered for sess-A","sessionId":"other",'
            '"timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        assert cd.detect_channel_mode_by_session("subspace", "sess-A") == cd.UNKNOWN

    def test_ex_cwd_probe_extracts_skip_reason(self, tmp_path, monkeypatch):
        # The reason is the harness's own text after 'skipped:' — surfaced so a
        # session stuck in pull mode (e.g. a respawn that lost --channels from its
        # argv, the neon-eviction incident) can self-announce WHY.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications skipped: server not in --channels'
            ' list for this session"}\n',
        )
        verdict, reason = cd.detect_channel_mode_ex("subspace", self.PROJECT)
        assert verdict == cd.SKIPPED
        assert reason == "server not in --channels list for this session"

    def test_ex_registered_has_no_reason(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"message":"Channel notifications registered"}\n',
        )
        assert cd.detect_channel_mode_ex("subspace", self.PROJECT) == (
            cd.REGISTERED,
            None,
        )

    def test_by_session_ex_extracts_reason_and_stops_at_quote(
        self, tmp_path, monkeypatch
    ):
        # Extraction must stop at the JSON string's closing quote — trailing fields
        # on the same line (sessionId, timestamp) must not leak into the reason.
        monkeypatch.setattr(cd, "_cache_root", lambda: tmp_path)
        _write_log(
            tmp_path,
            self.PROJECT,
            self.SERVER_DIR,
            "2026-06-26T00-00-00Z",
            '{"debug":"Channel notifications skipped: not in --channels list",'
            '"sessionId":"sess-R","timestamp":"2026-06-26T00:00:00.000Z"}\n',
        )
        verdict, reason = cd.detect_channel_mode_by_session_ex("subspace", "sess-R")
        assert verdict == cd.SKIPPED
        assert reason == "not in --channels list"


# --------------------------------------------------------------------------- #
# relay reconnect backoff
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def relay():
    spec = importlib.util.spec_from_file_location("subspace_relay", RELAY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_backoff_grows_then_caps_with_jitter(relay):
    for failures in range(14):
        nominal = min(
            relay.RECONNECT_MAX_SECONDS, relay.RECONNECT_INITIAL_SECONDS * 2**failures
        )
        samples = [relay._reconnect_delay(failures) for _ in range(2000)]
        assert min(samples) >= nominal * (1 - relay.RECONNECT_JITTER) - 1e-6
        assert max(samples) <= nominal * (1 + relay.RECONNECT_JITTER) + 1e-6


def test_backoff_steady_state_is_about_30_min(relay):
    assert relay.RECONNECT_MAX_SECONDS == 30 * 60
    samples = [relay._reconnect_delay(20) for _ in range(2000)]
    assert 24 * 60 <= min(samples)
    assert max(samples) <= 36 * 60


def test_wait_connected_nudges_reconnect_when_disconnected(relay):
    """The foreground path: a disconnected caller must NUDGE the reconnect loop
    (set _reconnect_now) so it breaks its idle-tuned backoff and retries at once,
    rather than passively waiting out the up-to-30-min sleep."""
    client = relay.DaemonClient()
    assert not client.connected  # fresh client, no socket

    async def scenario():
        return await client.wait_connected(timeout=0.2)  # no daemon → False, fast

    assert anyio.run(scenario) is False
    assert client._reconnect_now.is_set()  # the nudge was emitted


def test_detect_prefers_worktree_cwd_log(relay, tmp_path, monkeypatch):
    """A worktree session's MCP log is keyed by the session *cwd* (the worktree
    path), while CLAUDE_PROJECT_DIR still names the main repo root. Detection must
    probe the cwd first and find the real registration there, instead of reading
    the (markerless) main-repo log dir and downgrading to pull."""
    worktree = tmp_path / "proj" / ".claude" / "worktrees" / "wt"
    worktree.mkdir(parents=True)
    monkeypatch.chdir(worktree)
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", str(tmp_path / "proj"))
    cache = tmp_path / "cache"
    monkeypatch.setenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", str(cache))
    seed_channel_log(cache, registered=True, project_dir=str(worktree))

    client = relay.DaemonClient()
    anyio.run(client.detect_and_apply)
    assert client._mode == "push"


def test_detect_falls_back_to_project_dir_log(relay, tmp_path, monkeypatch):
    """When the cwd has no MCP log at all, the CLAUDE_PROJECT_DIR candidate must
    still be probed (the pre-worktree behavior)."""
    proj = tmp_path / "proj"
    elsewhere = tmp_path / "elsewhere"
    proj.mkdir()
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", str(proj))
    cache = tmp_path / "cache"
    monkeypatch.setenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", str(cache))
    seed_channel_log(cache, registered=True, project_dir=str(proj))

    client = relay.DaemonClient()
    anyio.run(client.detect_and_apply)
    assert client._mode == "push"


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _marker_line(marker: str, session_id: str, timestamp: str) -> str:
    return (
        json.dumps({"debug": marker, "sessionId": session_id, "timestamp": timestamp})
        + "\n"
    )


@pytest.fixture
def fast_detect(relay, tmp_path, monkeypatch):
    """Point detection at a tmp cache with short windows, for session id 'me'."""
    cache = tmp_path / "cache"
    monkeypatch.setenv("NOTIFICATIONS_MCP_LOG_CACHE_DIR", str(cache))
    monkeypatch.delenv("CLAUDE_PROJECT_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(relay.session_state, "known_session_ids", lambda: ["me"])
    monkeypatch.setattr(relay, "PROCESS_STARTED_AT", time.time())
    monkeypatch.setattr(relay, "CHANNEL_DETECT_POLL_SECONDS", 0.05)
    monkeypatch.setattr(relay, "CHANNEL_DETECT_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(relay, "CHANNEL_LATE_DETECT_POLL_SECONDS", 0.05)
    monkeypatch.setattr(relay, "CHANNEL_LATE_DETECT_SECONDS", 0.6)
    log_dir = (
        cache
        / "claude-cli-nodejs"
        / cd.encode_cwd(str(tmp_path))
        / "mcp-logs-plugin-subspace-subspace"
    )
    log_dir.mkdir(parents=True)
    return log_dir


def test_detect_ignores_resumed_sessions_stale_skip(relay, fast_detect):
    """Regression: a resumed session keeps its id. An earlier run's 'skipped'
    marker must not decide the mode before this run's 'registered' line arrives."""
    (fast_detect / "old.jsonl").write_text(
        _marker_line(
            "Channel notifications skipped: not in --channels list",
            "me",
            "2026-01-01T00:00:00.000Z",
        )
    )

    async def scenario():
        client = relay.DaemonClient()
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.detect_and_apply)
            await anyio.sleep(0.1)
            (fast_detect / "new.jsonl").write_text(
                _marker_line("Channel notifications registered", "me", _iso_now())
            )
        return client

    assert anyio.run(scenario)._mode == "push"


def test_detect_ignores_other_sessions_tagged_marker_in_cwd(relay, fast_detect):
    """The cwd fallback must not take a marker that another session in the same
    cwd tagged with its own id."""
    (fast_detect / "spare.jsonl").write_text(
        _marker_line("Channel notifications registered", "spare", _iso_now())
    )
    client = relay.DaemonClient()
    anyio.run(client.detect_and_apply)
    assert client._mode == "pull"


def test_detect_upgrades_on_late_marker(relay, fast_detect):
    """A marker that arrives after the detection timeout still switches to push."""

    async def scenario():
        client = relay.DaemonClient()
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.detect_and_apply)
            await anyio.sleep(0.4)  # past CHANNEL_DETECT_TIMEOUT_SECONDS
            assert client._mode == "pull"
            (fast_detect / "new.jsonl").write_text(
                _marker_line("Channel notifications registered", "me", _iso_now())
            )
        return client

    client = anyio.run(scenario)
    assert client._mode == "push"
    assert client._mode_detail is None


def test_catch_up_rechecks_pull_mode(relay, fast_detect):
    """catch_up in pull mode looks for the marker again and switches to push if it
    is there now; the held buffer moves to the push path."""
    client = relay.DaemonClient()
    client._mode = "pull"
    client._mode_detail = "no channel marker was found for this session"
    client._buffer["n1"] = {"content": "hello", "meta": {}}
    (fast_detect / "new.jsonl").write_text(
        _marker_line("Channel notifications registered", "me", _iso_now())
    )
    result = anyio.run(client.drain_buffer)
    assert client._mode == "push"
    assert "Live push is now active" in result
    assert not client._buffer
    assert [item["id"] for item in client._pending_debounce] == ["n1"]


def test_catch_up_stays_pull_without_marker(relay, fast_detect):
    client = relay.DaemonClient()
    client._mode = "pull"
    result = anyio.run(client.drain_buffer)
    assert client._mode == "pull"
    assert "Live push is disabled" in result


def test_wait_connected_does_not_nudge_when_already_connected(relay):
    """Already connected → nothing to reconnect, so no nudge is emitted."""
    client = relay.DaemonClient()
    client._ws = object()  # pretend connected

    async def scenario():
        return await client.wait_connected(timeout=0.2)

    assert anyio.run(scenario) is True
    assert not client._reconnect_now.is_set()


def test_run_survives_reconnect_exception_group(relay, monkeypatch):
    """Regression (side-door daemon-roll crash): an abnormal WS close bubbles out of
    _connect_once's task group as an anyio ExceptionGroup, which is neither an OSError
    nor a WebSocketException. run() must swallow it as a failed attempt and keep
    looping — NOT let it propagate and take the whole MCP server process down (every
    notifications tool vanished until a manual /mcp reconnect). Pre-fix, the first
    group escaped run() and this call raised instead of retrying."""
    client = relay.DaemonClient()
    calls = {"n": 0}

    async def boom():
        # Reproduce the real mechanism: _connect_once runs the recv/session tasks in
        # an anyio task group; a child raising on an abnormal close makes anyio 4
        # re-raise it wrapped in an ExceptionGroup (not an OSError/WebSocketException).
        calls["n"] += 1
        async with anyio.create_task_group() as tg:

            async def child():
                raise OSError("connection reset by peer")

            tg.start_soon(child)

    monkeypatch.setattr(relay, "STARTUP_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(relay, "_reconnect_delay", lambda failures: 0.01)
    monkeypatch.setattr(client, "_connect_once", boom)

    async def scenario():
        with anyio.move_on_after(0.1):  # let it iterate, then stop the infinite loop
            await client.run()

    anyio.run(scenario)  # must NOT raise
    assert calls["n"] >= 2  # kept retrying after the first ExceptionGroup


# --------------------------------------------------------------------------- #
# relay push-mode debounce / coalescing
# --------------------------------------------------------------------------- #


def _push_client(relay):
    """A push-mode DaemonClient with _deliver_channel/_ack replaced by recorders."""
    client = relay.DaemonClient()
    client._mode = "push"
    delivered: list[tuple[str, dict | None]] = []
    acked: list = []

    async def fake_deliver(content, meta):
        delivered.append((content, meta))

    async def fake_ack(notification_id):
        acked.append(notification_id)
        return True

    client._deliver_channel = fake_deliver
    client._ack = fake_ack
    return client, delivered, acked


def test_flush_coalesces_multiple_into_one_event(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_DEBOUNCE_SECONDS", "1")
    client, delivered, acked = _push_client(relay)

    async def scenario():
        await client._enqueue_push(
            "id1", "first", {"severity": "info", "kind": "pr_check"}
        )
        await client._enqueue_push(
            "id2", "second", {"severity": "high", "kind": "pr_review"}
        )
        await client._enqueue_push(
            "id3", "third", {"severity": "info", "kind": "pr_comment"}
        )
        assert delivered == []  # buffered, nothing pushed yet
        await client._flush_debounce()

    anyio.run(scenario)

    assert len(delivered) == 1  # one coalesced channel event
    content, meta = delivered[0]
    assert content == "first\n\nsecond\n\nthird"  # blank line between each
    assert meta == {"severity": "high", "kind": "batch", "count": "3"}
    assert acked == ["id1", "id2", "id3"]  # every underlying id acked
    assert client._pending_debounce == []


def test_flush_single_item_keeps_its_kind(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_DEBOUNCE_SECONDS", "1")
    client, delivered, acked = _push_client(relay)

    async def scenario():
        await client._enqueue_push(
            "only", "solo", {"severity": "info", "kind": "pr_comment"}
        )
        await client._flush_debounce()

    anyio.run(scenario)
    assert delivered == [
        ("solo", {"severity": "info", "kind": "pr_comment", "count": "1"})
    ]
    assert acked == ["only"]


def test_delivery_hint_is_mode_aware(relay):
    client = relay.DaemonClient()

    client._mode = "push"
    push_hint = client.delivery_hint()
    assert "<channel>" in push_hint and "catch_up" not in push_hint

    client._mode = "pull"
    pull_hint = client.delivery_hint()
    assert "catch_up" in pull_hint  # pull mode must point the agent at catch_up
    assert "<channel>" not in pull_hint  # and must not promise channel events

    client._mode = None  # detection unresolved: cover both outcomes
    detecting_hint = client.delivery_hint()
    assert "<channel>" in detecting_hint and "catch_up" in detecting_hint


def test_debounce_disabled_delivers_immediately(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_DEBOUNCE_SECONDS", "0")
    client, delivered, acked = _push_client(relay)

    async def scenario():
        await client._enqueue_push(
            "id1", "now", {"severity": "info", "kind": "pr_check"}
        )
        # delivered immediately with the original meta passed straight through
        assert delivered == [("now", {"severity": "info", "kind": "pr_check"})]
        assert acked == ["id1"]
        assert client._pending_debounce == []

    anyio.run(scenario)


def test_debounce_loop_flushes_quiet_burst(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_DEBOUNCE_SECONDS", "0.2")
    client, delivered, acked = _push_client(relay)

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.debounce_loop)
            await client._enqueue_push("a", "alpha", {"severity": "info", "kind": "x"})
            await client._enqueue_push("b", "beta", {"severity": "info", "kind": "y"})
            with anyio.move_on_after(3):  # let the quiet window elapse and flush
                while not delivered:
                    await anyio.sleep(0.05)
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert len(delivered) == 1
    content, meta = delivered[0]
    assert content == "alpha\n\nbeta"
    assert meta["kind"] == "batch" and meta["count"] == "2"
    assert acked == ["a", "b"]


# --------------------------------------------------------------------------- #
# bounded catch_up (drain_buffer cap)
# --------------------------------------------------------------------------- #


def _pull_client(relay):
    """A pull-mode DaemonClient with _ack replaced by a recorder (no ws)."""
    client = relay.DaemonClient()
    client._mode = "pull"
    acked: list = []

    async def fake_ack(notification_id):
        acked.append(notification_id)
        return True

    client._ack = fake_ack
    return client, acked


def test_drain_buffer_caps_at_max_messages(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_CATCHUP_MAX_MESSAGES", "2")
    client, acked = _pull_client(relay)
    client._buffer = {
        "id0": {"content": "alpha", "meta": {}},
        "id1": {"content": "bravo", "meta": {}},
        "id2": {"content": "charlie", "meta": {}},
    }
    out = anyio.run(client.drain_buffer)
    assert "alpha" in out and "bravo" in out and "charlie" not in out
    assert "capped batch" in out and "1 more pending" in out
    assert acked == ["id0", "id1"]
    assert list(client._buffer) == ["id2"]  # remainder kept, unacked, for next call


def test_drain_buffer_no_footer_when_all_fit(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_CATCHUP_MAX_MESSAGES", "5")
    client, acked = _pull_client(relay)
    client._buffer = {
        "id0": {"content": "alpha", "meta": {}},
        "id1": {"content": "bravo", "meta": {}},
    }
    out = anyio.run(client.drain_buffer)
    assert "alpha" in out and "bravo" in out and "capped batch" not in out
    assert client._buffer == {} and acked == ["id0", "id1"]


def test_drain_buffer_char_cap_always_drains_at_least_one(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_CATCHUP_MAX_CHARS", "10")
    monkeypatch.setenv("NOTIFICATIONS_CATCHUP_MAX_MESSAGES", "100")
    client, acked = _pull_client(relay)
    big = "x" * 50  # a single item over the whole char budget
    client._buffer = {
        "id0": {"content": big, "meta": {}},
        "id1": {"content": "next", "meta": {}},
    }
    out = anyio.run(client.drain_buffer)
    # The first item always drains (an oversized message can't wedge the drain); the
    # budget is then blown, so the second is held back.
    assert big in out and "next" not in out and "1 more pending" in out
    assert acked == ["id0"] and list(client._buffer) == ["id1"]


def test_drain_buffer_budget_spans_buffer_then_held(relay, monkeypatch):
    monkeypatch.setenv("NOTIFICATIONS_CATCHUP_MAX_MESSAGES", "3")
    client, acked = _pull_client(relay)
    client._buffer = {"b0": {"content": "buf0", "meta": {}}}
    client._held = [
        {"id": "h0", "content": "held0", "meta": {}},
        {"id": "h1", "content": "held1", "meta": {}},
        {"id": "h2", "content": "held2", "meta": {}},
    ]
    out = anyio.run(client.drain_buffer)
    # Budget 3 spans both buffers oldest-first: buf0 + held0 + held1; held2 held back.
    assert "buf0" in out and "held0" in out and "held1" in out and "held2" not in out
    assert acked == ["b0", "h0", "h1"]
    assert [i["id"] for i in client._held] == ["h2"]


# --------------------------------------------------------------------------- #
# message_status wedge hint (_wedge_annotation)
# --------------------------------------------------------------------------- #


def test_wedge_annotation_flags_stale_connected_recipient(relay):
    info = {"connected": True, "last_acked": 1000.0}
    out = relay._wedge_annotation("worker", info, threshold=600.0, now=2000.0)
    assert out is not None
    assert "worker" in out and "possibly context-wedged" in out


def test_wedge_annotation_none_when_recently_acked(relay):
    info = {"connected": True, "last_acked": 1900.0}  # acked 100s ago, < 600 threshold
    assert relay._wedge_annotation("worker", info, threshold=600.0, now=2000.0) is None


def test_wedge_annotation_none_when_never_acked(relay):
    # A never-acked (fresh/quiet) session isn't a wedge, even connected with a pending
    # message — guards against flagging brand-new sessions.
    assert relay._wedge_annotation("w", {"connected": True}, 600.0, 2000.0) is None
    assert relay._wedge_annotation("w", {"last_acked": 0.0}, 600.0, 2000.0) is None


# --------------------------------------------------------------------------- #
# delivery at resume: hold, claim, decline
# --------------------------------------------------------------------------- #


def _message(nid: str, content: str, level: str, threshold: str = "direct") -> dict:
    return {
        "id": nid,
        "content": content,
        "meta": {"kind": "message", "level": level, "threshold": threshold},
    }


class _FakeWs:
    """Records the frames the relay sends to the daemon."""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))


def _holding_client(relay, monkeypatch):
    """A push-mode client with a fake daemon that answers whoami at once."""
    monkeypatch.setenv("NOTIFICATIONS_DEBOUNCE_SECONDS", "0")
    client, delivered, acked = _push_client(relay)
    client._ws = _FakeWs()
    client._registered_session = "sid"

    async def fake_request(payload, *, timeout=0.0):
        client._ws.sent.append(payload)
        return {"type": wsproto.WHOAMI_RESULT, "session_id": "sid"}

    client.request = fake_request
    client._start_hold()
    return client, delivered, acked


def test_hold_keeps_push_back_until_released(relay, monkeypatch):
    client, delivered, acked = _holding_client(relay, monkeypatch)

    async def scenario():
        await client._enqueue_push("a", "first", {"kind": "pr_check"})
        await client._flush_debounce()
        assert delivered == []  # held, with debounce disabled too
        client._release_hold()
        await client._flush_debounce()

    anyio.run(scenario)
    assert [content for content, _ in delivered] == ["first"]
    assert acked == ["a"]


def test_hold_ends_on_its_own(relay, monkeypatch):
    monkeypatch.setattr(relay, "HOLD_SECONDS", 0.2)
    client, delivered, _acked = _holding_client(relay, monkeypatch)

    async def scenario():
        await client._enqueue_push("a", "first", {"kind": "pr_check"})
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.debounce_loop)
            with anyio.move_on_after(2.0):
                while not delivered:
                    await anyio.sleep(0.05)
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert [content for content, _ in delivered] == ["first"]


def test_claim_takes_surfacing_messages_and_acks_on_confirm(relay, monkeypatch):
    client, delivered, acked = _holding_client(relay, monkeypatch)
    # A surfacing message waits in the debounce buffer, a quiet one in the held
    # buffer, and a PR event (no level) in the debounce buffer.
    client._pending_debounce = [
        _message("m1", "@here all hands", "urgent"),
        {"id": "p1", "content": "pr event", "meta": {"kind": "pr_check"}},
    ]
    client._held = [_message("m2", "fyi thing", "ambient")]
    client._buffer = {"n1": {"content": "notice", "meta": {"level": "direct"}}}

    async def scenario():
        items = await client.claim("sid")
        assert [item["id"] for item in items] == ["m1", "n1"]
        assert client._ws.sent == [{"type": wsproto.WHOAMI}]  # same id: no register
        assert [item["id"] for item in client._pending_debounce] == ["p1"]
        assert [item["id"] for item in client._held] == ["m2"]
        assert client.holding
        await client.confirm_claim()
        await client._flush_debounce()

    anyio.run(scenario)
    assert acked == ["m1", "n1", "p1"]
    assert [content for content, _ in delivered] == ["pr event"]  # messages not pushed


def test_claim_registers_a_new_session_and_discards_old_items(relay, monkeypatch):
    client, _delivered, acked = _holding_client(relay, monkeypatch)
    client._registered_session = "old"
    client._hold_deadline = None
    client._pending_debounce = [_message("old1", "stale", "urgent")]
    client._held = [_message("old2", "stale quiet", "ambient")]

    async def scenario():
        items = await client.claim("new")
        assert items == []  # the old session's items are not this session's
        assert client._ws.sent == [
            {"type": wsproto.REGISTER, "session_id": "new"},
            {"type": wsproto.WHOAMI},
        ]
        assert client._registered_session == "new"
        assert client.holding
        assert client._held == [] and client._pending_debounce == []

    anyio.run(scenario)
    assert acked == []


def test_abandoned_claim_returns_messages_to_channel_delivery(relay, monkeypatch):
    client, delivered, acked = _holding_client(relay, monkeypatch)
    client._pending_debounce = [_message("m1", "@here all hands", "urgent")]

    async def scenario():
        items = await client.claim("sid")
        assert [item["id"] for item in items] == ["m1"]
        await client.abandon_claim()
        assert not client.holding
        await client._flush_debounce()

    anyio.run(scenario)
    assert [content for content, _ in delivered] == ["@here all hands"]
    assert acked == ["m1"]


def test_claim_without_daemon_takes_nothing(relay, monkeypatch):
    monkeypatch.setattr(relay, "CLAIM_CONNECT_SECONDS", 0.2)
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._ws = None
    client._pending_debounce = [_message("m1", "@here all hands", "urgent")]
    assert anyio.run(client.claim, "sid") == []
    assert [item["id"] for item in client._pending_debounce] == ["m1"]


def test_session_watch_change_starts_a_hold_and_discards_items(relay, monkeypatch):
    monkeypatch.setattr(relay, "SESSION_POLL_SECONDS", 0.05)
    monkeypatch.setattr(
        relay.session_state, "effective_session_id", lambda: ("new", "state file")
    )
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._registered_session = "old"
    client._hold_deadline = None
    client._pending_debounce = [_message("old1", "stale", "urgent")]

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client._session_watch, client._ws, tg)
            with anyio.move_on_after(2.0):
                while client._registered_session != "new":
                    await anyio.sleep(0.02)
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert client._ws.sent == [{"type": wsproto.REGISTER, "session_id": "new"}]
    assert client.holding
    assert client._pending_debounce == []


def test_first_registration_does_not_start_a_hold(relay, monkeypatch):
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._registered_session = None
    client._hold_deadline = None
    anyio.run(client._register, client._ws, "sid")
    assert not client.holding
    assert client._registered_session == "sid"


@pytest.fixture
def ipc_socket(relay, monkeypatch):
    """A short socket path (Unix socket paths are limited to about 100 bytes)."""
    directory = Path(tempfile.mkdtemp(prefix="nt-"))
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(directory))  # a private state dir
    path = directory / "1.sock"
    monkeypatch.setattr(relay, "_ipc_path", lambda: path)
    monkeypatch.setattr(relay, "_hook_expected", lambda: True)
    yield path
    shutil.rmtree(directory, ignore_errors=True)


def test_relay_refuses_a_state_dir_that_is_not_private(
    relay, monkeypatch, ipc_socket, capsys
):
    monkeypatch.setattr(relay.session_state, "secure_state_dir", lambda: None)
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.serve_ipc)
            await anyio.sleep(0.2)
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert not ipc_socket.exists()
    assert not client.holding
    assert "not private" in capsys.readouterr().err


async def _hook_exchange(path: Path, request: dict, *, done: bool) -> dict:
    """Speak the hook side of the IPC: send `request`, read the reply, and send
    "done" (or close without it)."""
    async with await anyio.connect_unix(str(path)) as stream:
        await stream.send(json.dumps(request).encode() + b"\n")
        if request["type"] != "claim":
            return {}
        lines = BufferedByteReceiveStream(stream)
        reply = json.loads(await lines.receive_until(b"\n", 65536))
        if done and reply.get("count"):  # as the hook: no "done" for nothing
            await stream.send(b'{"type": "done"}\n')
            await anyio.sleep(0.1)  # let the relay read it before the close
        return reply


async def _wait_for(path: Path) -> None:
    with anyio.fail_after(5.0):
        while not path.exists():
            await anyio.sleep(0.02)


def test_ipc_claim_then_done_acks(relay, monkeypatch, ipc_socket):
    client, delivered, acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None  # serve_ipc starts the hold itself
    client._pending_debounce = [
        _message("m1", "@here all hands", "urgent"),
        _message("m2", "@you direct", "direct"),
    ]
    client._held = [_message("m3", "fyi thing", "ambient")]

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.serve_ipc)
            await _wait_for(ipc_socket)
            assert client.holding
            reply = await _hook_exchange(
                ipc_socket,
                {"type": "claim", "session_id": "sid", "source": "resume"},
                done=True,
            )
            assert reply == {
                "type": "claimed",
                "count": 2,
                "content": "@here all hands\n\n@you direct",
            }
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert acked == ["m1", "m2"]
    assert not client.holding
    assert delivered == []
    assert [item["id"] for item in client._held] == ["m3"]
    assert not ipc_socket.exists()  # removed on shutdown


def test_ipc_close_before_done_keeps_messages_for_the_channel(
    relay, monkeypatch, ipc_socket
):
    client, delivered, acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None
    client._pending_debounce = [_message("m1", "@here all hands", "urgent")]

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.serve_ipc)
            await _wait_for(ipc_socket)
            reply = await _hook_exchange(
                ipc_socket,
                {"type": "claim", "session_id": "sid", "source": "resume"},
                done=False,
            )
            assert reply["count"] == 1
            with anyio.fail_after(5.0):
                while client.holding or client._claimed:
                    await anyio.sleep(0.02)
            await client._flush_debounce()
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert [content for content, _ in delivered] == ["@here all hands"]
    assert acked == ["m1"]


def test_ipc_decline_releases_the_hold(relay, monkeypatch, ipc_socket):
    client, delivered, _acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None
    client._pending_debounce = [_message("m1", "@here all hands", "urgent")]

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.serve_ipc)
            await _wait_for(ipc_socket)
            assert client.holding
            await _hook_exchange(
                ipc_socket,
                {"type": "decline", "session_id": "sid", "source": "startup"},
                done=False,
            )
            with anyio.fail_after(5.0):
                while client.holding:
                    await anyio.sleep(0.02)
            await client._flush_debounce()
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert [content for content, _ in delivered] == ["@here all hands"]
    assert client._pending_debounce == []


def test_relay_in_an_old_session_does_not_hold_at_start(relay, monkeypatch, ipc_socket):
    """An MCP reconnect restarts the relay long after the hook ran: no hook is
    coming, so the start hold would only delay delivery."""
    monkeypatch.setattr(relay, "_hook_expected", lambda: False)
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.serve_ipc)
            await _wait_for(ipc_socket)
            assert not client.holding
            tg.cancel_scope.cancel()

    anyio.run(scenario)


def test_hook_expected_follows_the_state_file_age(relay, monkeypatch, tmp_path):
    """The hook writes the state file just before it claims. An old process
    (a hot-spare worker) with no file, or a fresh file, means a hook is coming;
    an old file means the relay restarted in a running session."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setattr(relay.session_state, "resolve_claude_pid", lambda: 4242)
    monkeypatch.setattr(relay.session_state, "_proc_start_time", lambda pid: 111)
    assert relay._hook_expected()  # no file yet: the hook has not run
    path = session_state.write_session(4242, "sid", "resume")
    assert relay._hook_expected()  # just written
    old = time.time() - 3600
    os.utime(path, (old, old))
    assert not relay._hook_expected()  # written long ago: no hook is coming
    # A file of a recycled pid is not this process's file.
    monkeypatch.setattr(relay.session_state, "_proc_start_time", lambda pid: 222)
    assert relay._hook_expected()
    monkeypatch.setattr(relay.session_state, "resolve_claude_pid", lambda: None)
    assert not relay._hook_expected()


def test_claim_holds_before_it_waits_for_the_connection(relay, monkeypatch):
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None
    seen: list[bool] = []

    async def fake_wait_connected(timeout: float) -> bool:
        seen.append(client.holding)
        return False

    client.wait_connected = fake_wait_connected
    assert anyio.run(client.claim, "sid") == []
    assert seen == [True]


def test_replay_of_a_claimed_message_is_not_delivered_twice(relay, monkeypatch):
    """A reconnect while the hook holds a claim: the daemon sends the unacked
    ids again. The copies are dropped; confirm acks once, abandon delivers once."""
    client, delivered, acked = _holding_client(relay, monkeypatch)
    client._pending_debounce = [_message("m1", "@here all hands", "urgent")]

    async def scenario():
        await client.claim("sid")
        client._held = []  # what the reconnect cleanup does
        client._pending_debounce = []
        await client._dispatch_notify("m1", "@here all hands", _message_meta())
        assert client._pending_debounce == [] and client._held == []
        await client.confirm_claim()
        await client._flush_debounce()
        assert delivered == [] and acked == ["m1"]

        client._pending_debounce = [_message("m2", "@you direct", "direct")]
        await client.claim("sid")
        client._pending_debounce = []
        await client._dispatch_notify("m2", "@you direct", _message_meta("direct"))
        await client.abandon_claim()
        await client._flush_debounce()

    anyio.run(scenario)
    assert [content for content, _ in delivered] == ["@you direct"]
    assert acked == ["m1", "m2"]


def _message_meta(level: str = "urgent") -> dict:
    return {"kind": "message", "level": level, "threshold": "direct"}


class _FrameWs:
    """A daemon connection that sends the given frames, then closes."""

    def __init__(self, frames: list[dict]) -> None:
        self.frames = frames

    async def __aiter__(self):
        for frame in self.frames:
            yield json.dumps(frame)


def test_notify_of_an_earlier_session_is_dropped_on_receipt(relay, monkeypatch):
    """After a switch, an item of the earlier session can still be on the wire.
    Its session tag tells it apart; a frame without a tag (an older daemon)
    is accepted as before."""
    client, _delivered, acked = _holding_client(relay, monkeypatch)
    client._registered_session = "new"
    frames = [
        {
            "type": wsproto.NOTIFY,
            "id": "old1",
            "content": "late",
            "meta": _message_meta(),
            "session_id": "old",
        },
        {
            "type": wsproto.NOTIFY,
            "id": "new1",
            "content": "mine",
            "meta": _message_meta(),
            "session_id": "new",
        },
        {
            "type": wsproto.NOTIFY,
            "id": "untagged",
            "content": "old daemon",
            "meta": _message_meta(),
        },
    ]

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client._recv_loop, _FrameWs(frames), tg)

    anyio.run(scenario)
    assert [i["id"] for i in client._pending_debounce] == ["new1", "untagged"]
    assert acked == []


def test_confirm_while_disconnected_acks_on_the_next_registration(relay, monkeypatch):
    """The hook confirms after the daemon connection dropped: the ids stay
    suppressed, a replay is not delivered, and the ack goes out once the same
    session is registered again. A session change drops them."""
    client, delivered, acked = _holding_client(relay, monkeypatch)
    client._pending_debounce = [_message("m1", "@here all hands", "urgent")]

    async def fake_ack(notification_id):
        if client._ws is None:
            return False
        acked.append(notification_id)
        return True

    client._ack = fake_ack

    async def scenario():
        await client.claim("sid")
        ws, client._ws = client._ws, None  # the connection drops
        await client.confirm_claim()
        assert acked == [] and client._ack_pending == {"m1"}
        # Reconnect: REGISTER, then the pending acks, then the daemon's replay
        # (sent on REGISTER, read once the receive loop runs).
        client._ws = ws
        await client._register(ws, "sid")
        await client._send_pending_acks()
        assert acked == ["m1"] and client._ack_pending == set()
        await client._dispatch_notify("m1", "@here all hands", _message_meta())
        await client._flush_debounce()
        assert delivered == []  # the replay is dropped

        client._pending_debounce = [_message("m2", "@you", "direct")]
        await client.claim("sid")
        client._ws = None  # drops again before the hook confirms
        await client.confirm_claim()
        assert client._ack_pending == {"m2"}
        client._ws = ws
        client._registered_session = "other"
        await client._send_pending_acks()
        assert acked == ["m1"] and client._ack_pending == set()
        assert "m2" not in client._confirmed

    anyio.run(scenario)


def test_reconnect_under_a_new_session_is_a_session_change(relay, monkeypatch):
    """The session switched while the daemon connection was down: the reconnect
    registers the new id, so the earlier session's claim, confirmed ids and
    buffers go, and the new session's own copy of a shared message is kept."""
    client, delivered, acked = _holding_client(relay, monkeypatch)
    client._registered_session = "A"
    client._hold_deadline = None
    client._claimed = [_message("c1", "claimed in A", "urgent")]
    client._claimed_session = "A"
    client._confirmed = {"m1": "A"}
    client._ack_pending = {"m1"}
    client._buffer = {"n1": {"content": "notice", "meta": {"level": "direct"}}}

    async def scenario():
        client._ws.sent.clear()
        await client._register(client._ws, "B")  # what the reconnect does
        assert client._ws.sent == [{"type": wsproto.REGISTER, "session_id": "B"}]
        assert client.holding
        assert client._claimed == [] and client._confirmed == {}
        assert client._ack_pending == set() and client._buffer == {}
        await client._send_pending_acks()
        # B's own copy of m1 (a shared channel) is not suppressed.
        await client._dispatch_notify("m1", "@here all hands", _message_meta())
        assert [i["id"] for i in client._pending_debounce] == ["m1"]
        # An outstanding claim of A that the hook now confirms is not acked under B.
        client._claimed = [_message("c2", "claimed in A", "urgent")]
        client._claimed_session = "A"
        await client.confirm_claim()

    anyio.run(scenario)
    assert acked == []
    assert delivered == []


def test_decline_with_a_new_session_registers_it_first(relay, monkeypatch):
    """After /clear the hook declines with the new id. The relay switches at
    once, so the watcher's poll sees no change and starts no hold."""
    client, delivered, _acked = _holding_client(relay, monkeypatch)
    client._registered_session = "old"
    client._pending_debounce = [_message("old1", "stale", "urgent")]

    async def scenario():
        await client.decline("new")
        assert client._registered_session == "new"
        assert not client.holding
        assert client._pending_debounce == []
        assert client._ws.sent == [{"type": wsproto.REGISTER, "session_id": "new"}]
        await client._flush_debounce()

    anyio.run(scenario)
    assert delivered == []


def test_ipc_junk_does_not_kill_the_listener(relay, monkeypatch, ipc_socket):
    client, _delivered, _acked = _holding_client(relay, monkeypatch)
    client._hold_deadline = None

    async def scenario():
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.serve_ipc)
            await _wait_for(ipc_socket)
            async with await anyio.connect_unix(str(ipc_socket)) as stream:
                await stream.send(b"not json\n")
            await anyio.sleep(0.1)
            # The listener still answers a later hook.
            reply = await _hook_exchange(
                ipc_socket,
                {"type": "claim", "session_id": "sid", "source": "resume"},
                done=True,
            )
            assert reply == {"type": "claimed", "count": 0, "content": ""}
            tg.cancel_scope.cancel()

    anyio.run(scenario)
    assert not client.holding


# --------------------------------------------------------------------------- #
# relay ref parsing
# --------------------------------------------------------------------------- #


class TestRefParsing:
    def test_bare_ref_is_default_instance(self, relay):
        assert relay._parse_forgejo_ref("owner/repo#4") == ("", "owner", "repo", 4)

    def test_aliased_ref(self, relay):
        assert relay._parse_forgejo_ref("external:owner/repo#12") == (
            "external",
            "owner",
            "repo",
            12,
        )

    def test_kebab_alias(self, relay):
        assert relay._parse_forgejo_ref("self-hosted:o/r#1") == (
            "self-hosted",
            "o",
            "r",
            1,
        )

    def test_full_host_url_rejected(self, relay):
        err = relay._parse_forgejo_ref("https://fj.example.com/o/r#4")
        assert isinstance(err, str) and "Invalid PR reference" in err

    def test_dotted_host_prefix_rejected(self, relay):
        err = relay._parse_forgejo_ref("fj.example.com:o/r#4")
        assert isinstance(err, str) and "Full-host" in err

    def test_host_port_prefix_rejected(self, relay):
        # host:8080:o/r#4 — the first colon must not peel a host-like alias and leave a
        # colon-bearing owner; the extra colon in the remainder is what rejects it.
        err = relay._parse_forgejo_ref("host:8080:o/r#4")
        assert isinstance(err, str) and "Full-host" in err

    def test_uppercase_alias_rejected(self, relay):
        err = relay._parse_forgejo_ref("EXTERNAL:o/r#4")
        assert isinstance(err, str) and "kebab-case" in err

    def test_garbage_ref_rejected(self, relay):
        assert isinstance(relay._parse_forgejo_ref("not-a-ref"), str)

    def test_display_helpers(self, relay):
        assert relay._forgejo_ref_display("", "o", "r", 4) == "o/r#4"
        assert relay._forgejo_ref_display("ext", "o", "r", 4) == "ext:o/r#4"


# --------------------------------------------------------------------------- #
# relay version-skew echo-back cleanup
# --------------------------------------------------------------------------- #


class TestSkewCleanup:
    """The relay's belt: sent-non-default + no-echo == failure.

    We drive subscribe_forgejo_pr with a stubbed _forgejo_daemon_request that models an
    OLD daemon (no `instance` echo) and a session-state stub, then assert the tool treats
    it as failure and either unsubscribes the stray sub (this-session-created) or warns
    without destroying (mirrored-repo / pre-existing default)."""

    def _install_stubs(self, relay, monkeypatch, reply):
        monkeypatch.setattr(
            relay.session_state, "effective_session_id", lambda: ("sidA", "hook")
        )
        calls = []

        async def fake_request(payload):
            calls.append(payload)
            return dict(reply)

        monkeypatch.setattr(relay, "_forgejo_daemon_request", fake_request)
        return calls

    def test_old_daemon_no_echo_this_session_created_unsubscribes_stray(
        self, relay, monkeypatch
    ):
        relay._SESSION_CREATED_SUBS.clear()
        # OLD daemon: subscribes on its default, replies subscribed but WITHOUT an instance.
        calls = self._install_stubs(
            relay,
            monkeypatch,
            {"type": relay.wsproto.SUBSCRIBED, "pr": "o/r#1", "summary": "open"},
        )
        out = anyio.run(lambda: relay.subscribe_forgejo_pr("external:o/r#1"))
        assert "predates multi-instance" in out
        assert "has been undone" in out  # cleaned up the stray default sub
        # the follow-up request is the UNSUBSCRIBE the tool issued to undo it
        assert any(c["type"] == relay.wsproto.UNSUBSCRIBE_FORGEJO_PR for c in calls)
        assert (("external", "o/r#1")) not in relay._SESSION_CREATED_SUBS

    def test_old_daemon_no_echo_not_created_warns_without_destroy(
        self, relay, monkeypatch
    ):
        relay._SESSION_CREATED_SUBS.clear()
        # Simulate a pre-existing default sub the relay did NOT create this session by
        # pre-seeding the reply but making the tool believe it created it, then removing
        # the created-marker before the reply is processed. Simplest: model the
        # mirrored-repo case by clearing the created-set right before the skew check.
        calls = []

        monkeypatch.setattr(
            relay.session_state, "effective_session_id", lambda: ("sidA", "hook")
        )

        async def fake_request(payload):
            calls.append(payload)
            # Between the tool recording (instance,ref) and the reply, drop the marker so
            # the skew path sees "I did NOT create this" (the mirrored-repo case).
            relay._SESSION_CREATED_SUBS.discard(("external", "o/r#1"))
            return {"type": relay.wsproto.SUBSCRIBED, "pr": "o/r#1", "summary": "open"}

        monkeypatch.setattr(relay, "_forgejo_daemon_request", fake_request)
        out = anyio.run(lambda: relay.subscribe_forgejo_pr("external:o/r#1"))
        assert "predates multi-instance" in out
        assert "may have been created" in out  # warn-without-destroy
        assert "check list_forgejo_pr_subscriptions" in out
        # It must NOT have issued an unsubscribe (would destroy a legit default sub).
        assert all(c["type"] != relay.wsproto.UNSUBSCRIBE_FORGEJO_PR for c in calls)

    def test_new_daemon_echoes_instance_success(self, relay, monkeypatch):
        relay._SESSION_CREATED_SUBS.clear()
        self._install_stubs(
            relay,
            monkeypatch,
            {
                "type": relay.wsproto.SUBSCRIBED,
                "pr": "o/r#1",
                "instance": "external",
                "summary": "open, checks 0 pass",
            },
        )
        out = anyio.run(lambda: relay.subscribe_forgejo_pr("external:o/r#1"))
        assert "Subscribed to external:o/r#1 (Forgejo: external)" in out
        assert "predates" not in out
        # A clean success discards the in-flight marker (no stray to undo), so it can't
        # linger to a later re-subscribe and defeat the mirrored-repo destroy-safety.
        assert ("external", "o/r#1") not in relay._SESSION_CREATED_SUBS

    def test_default_subscribe_renders_exactly_v1(self, relay, monkeypatch):
        relay._SESSION_CREATED_SUBS.clear()
        self._install_stubs(
            relay,
            monkeypatch,
            {
                "type": relay.wsproto.SUBSCRIBED,
                "pr": "o/r#1",
                "instance": "",
                "summary": "open",
            },
        )
        out = anyio.run(lambda: relay.subscribe_forgejo_pr("o/r#1"))
        # v1 wording: "Subscribed to o/r#1 (Forgejo). Current status: ..."
        assert "Subscribed to o/r#1 (Forgejo)." in out
        assert "Forgejo:" not in out  # default carries no alias tag

    def test_default_subscribe_old_daemon_no_skew(self, relay, monkeypatch):
        # A DEFAULT subscribe against an old daemon (no echo) is NOT a skew — we sent no
        # instance, so the missing echo is expected and it renders as a plain success.
        relay._SESSION_CREATED_SUBS.clear()
        self._install_stubs(
            relay,
            monkeypatch,
            {"type": relay.wsproto.SUBSCRIBED, "pr": "o/r#1", "summary": "open"},
        )
        out = anyio.run(lambda: relay.subscribe_forgejo_pr("o/r#1"))
        assert "Subscribed to o/r#1 (Forgejo)." in out
        assert "predates" not in out


# --------------------------------------------------------------------------- #
# relay list rendering with alias tags
# --------------------------------------------------------------------------- #


class TestListRendering:
    def test_named_and_default_list_lines(self, relay, monkeypatch):
        monkeypatch.setattr(
            relay.session_state, "effective_session_id", lambda: ("sidA", "hook")
        )

        async def fake_request(payload):
            return {
                "type": relay.wsproto.SUBSCRIPTIONS_RESULT,
                "items": [
                    {"pr": "o/r#1", "instance": "", "merged": False, "pending": 0},
                    {
                        "pr": "o/r#2",
                        "instance": "external",
                        "merged": False,
                        "pending": 3,
                    },
                ],
            }

        monkeypatch.setattr(relay, "_forgejo_daemon_request", fake_request)
        out = anyio.run(relay.list_forgejo_pr_subscriptions)
        assert "  o/r#1" in out  # default: bare
        assert "external:o/r#2" in out  # named: alias-prefixed
        assert "pending=3" in out
