# vim: filetype=python
"""Unit tests for the scheduled-callback store, the server settings, and the
server's warm-retention reaper."""

import asyncio
import importlib.util

import accounts
import sessions
import time
from pathlib import Path

import pytest

import pr_monitor
import scheduler
import settings

# --------------------------------------------------------------------------- #
# scheduler
# --------------------------------------------------------------------------- #


def test_scheduler_due_and_delete(tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    callback_id = scheduler.schedule("sid", time.time() - 1, kind="demo")
    due = scheduler.due_callbacks("sid", time.time())
    assert [e["id"] for e in due] == [callback_id]
    assert [e["id"] for e in scheduler.pending("sid")] == [callback_id]
    scheduler.delete("sid", callback_id)
    assert scheduler.pending("sid") == []


def test_scheduler_not_yet_due(tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    scheduler.schedule("sid", time.time() + 1000)
    assert scheduler.due_callbacks("sid", time.time()) == []
    assert len(scheduler.pending("sid")) == 1


# --------------------------------------------------------------------------- #
# shared token
# --------------------------------------------------------------------------- #


def test_token_autocreates_stable_and_0600(tmp_path, monkeypatch):
    monkeypatch.delenv("SUBSPACE_TOKEN", raising=False)
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    first = settings.token()
    assert first
    token_file = tmp_path / "token"
    assert token_file.read_text().strip() == first
    assert (token_file.stat().st_mode & 0o777) == 0o600
    assert settings.token() == first  # stable on repeated calls


def test_token_env_overrides_file(tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SUBSPACE_TOKEN", "explicit-secret")
    assert settings.token() == "explicit-secret"
    assert not (tmp_path / "token").exists()  # env path never touches the file


def test_data_dir_defaults_to_xdg(tmp_path, monkeypatch):
    monkeypatch.delenv("SUBSPACE_DATA_DIR", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    assert settings.data_dir() == tmp_path / "subspace"
    monkeypatch.delenv("XDG_DATA_HOME")
    assert settings.data_dir() == Path.home() / ".local" / "share" / "subspace"
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path / "explicit"))
    assert settings.data_dir() == tmp_path / "explicit"


# --------------------------------------------------------------------------- #
# server warm-retention TTL + reaper
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def server():
    path = Path(__file__).resolve().parent.parent / "server" / "subspace-server.py"
    spec = importlib.util.spec_from_file_location("subspace_server", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("change", ["remove", "replace", "transfer"])
def test_wake_retry_resolves_current_target(server, tmp_path, monkeypatch, change):
    registry = sessions.SessionRegistry(tmp_path)
    record, _ = registry.register_handle(
        "bots/one", "runner", now=1, is_live=lambda _: False, idle=60
    )
    old = accounts.Webhook("runner", "https://old.invalid/wake", key="old")
    new = accounts.Webhook("runner", "https://new.invalid/wake", key="new")
    account = accounts.Account(id="bots", webhooks=(old,))
    monkeypatch.setattr(server, "SESSIONS", registry, raising=False)
    monkeypatch.setattr(server, "_scope", lambda _: account)
    monkeypatch.setattr(server, "_live", lambda _: False)
    monkeypatch.setattr(server, "_wake_retry_delays", lambda: [0.001])
    items = [(1, {"id": "message"})]
    monkeypatch.setattr(server, "_wake_items", lambda *_: items)
    posts = []

    async def post(url, bearer, body):
        nonlocal account
        posts.append((url, bearer))
        if len(posts) == 1:
            if change == "transfer":
                registry.register_handle(
                    "bots/two",
                    "runner",
                    now=2,
                    is_live=lambda _: False,
                    idle=60,
                    takeover=True,
                )
            else:
                account = accounts.Account(
                    id="bots", webhooks=(new,) if change == "replace" else ()
                )
            return 503
        return 200

    monkeypatch.setattr(server, "_post_webhook", post)
    asyncio.run(server._send_wake(record.key, account, old, items))
    assert posts == [(old.url, old.key)] + (
        [(new.url, new.key)] if change == "replace" else []
    )


def test_reactions_do_not_wake_at_all_threshold(server, tmp_path, monkeypatch):
    from message_topic import MessageTopic

    registry = sessions.SessionRegistry(tmp_path)
    record = registry.ensure("bots/one", now=1)
    record.default_threshold = "all"
    topic = MessageTopic("chan:room", "channel")
    topic.join(record.key, now=1)
    normal = topic.post("bots/two", now=2, body="hello", ordinal=1)
    reaction = topic.post(
        "bots/two",
        now=3,
        body="yes",
        ordinal=2,
        intent="reaction",
        target=normal.id,
    )
    monkeypatch.setattr(server, "SESSIONS", registry, raising=False)
    monkeypatch.setattr(server, "TOPICS", {topic.key: topic})
    monkeypatch.setattr(
        server, "_scope", lambda _: accounts.Account(id="bots", channels=None)
    )
    assert [ordinal for ordinal, _ in server._wake_items(record.key, 0)] == [1]
    # Wake filtering does not acknowledge or remove ordinary reaction delivery.
    assert reaction in topic.messages
    assert reaction.id not in topic.acked[record.key]


def _make_tracker(
    server, key="o/r#1", *, subscribers=(), terminal=False, idle_since=None
):
    t = pr_monitor.PRTracker("o", "r", 1, None)
    t.key = key
    t.subscribers = set(subscribers)
    t.terminal = terminal
    t.idle_since = idle_since
    return t


def test_warm_ttl_seconds_default_and_overrides(server, monkeypatch):
    monkeypatch.delenv("SUBSPACE_PR_WARM_TTL_SECONDS", raising=False)
    assert server._warm_ttl_seconds() == 1800.0
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "0")
    assert server._warm_ttl_seconds() == 0.0
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "5")
    assert server._warm_ttl_seconds() == 5.0
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "garbage")
    assert server._warm_ttl_seconds() == 1800.0


def test_reaper_removes_expired_tracker(server, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "10")
    now = time.time()
    t = _make_tracker(server, idle_since=now - 100)
    monkeypatch.setattr(server, "TRACKERS", {t.key: t})
    removed = server._reap_idle_trackers(now)
    assert removed == [t.key]
    assert t.key not in server.TRACKERS


def test_reaper_keeps_recently_idle_tracker(server, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "10")
    now = time.time()
    t = _make_tracker(server, idle_since=now - 1)
    monkeypatch.setattr(server, "TRACKERS", {t.key: t})
    assert server._reap_idle_trackers(now) == []
    assert t.key in server.TRACKERS


def test_reaper_skips_subscribed_and_terminal(server, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "10")
    now = time.time()
    subbed = _make_tracker(
        server, key="o/r#1", subscribers=("sid",), idle_since=now - 100
    )
    term = _make_tracker(server, key="o/r#2", terminal=True, idle_since=now - 100)
    monkeypatch.setattr(server, "TRACKERS", {subbed.key: subbed, term.key: term})
    assert server._reap_idle_trackers(now) == []
    assert subbed.key in server.TRACKERS
    assert term.key in server.TRACKERS


def test_reaper_self_heals_missing_idle_marker(server, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "10")
    now = time.time()
    t = _make_tracker(server, idle_since=None)
    monkeypatch.setattr(server, "TRACKERS", {t.key: t})
    assert server._reap_idle_trackers(now) == []  # clock just started, not removed
    assert t.key in server.TRACKERS
    assert t.idle_since == pytest.approx(now)


def test_reaper_disabled_keeps_everything(server, tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SUBSPACE_PR_WARM_TTL_SECONDS", "0")
    t = _make_tracker(server, idle_since=time.time() - 10_000)
    monkeypatch.setattr(server, "TRACKERS", {t.key: t})
    assert server._reap_idle_trackers(time.time()) == []
    assert t.key in server.TRACKERS
