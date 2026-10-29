# vim: filetype=python
"""Unit tests for the scheduled-callback store, the server settings, and the
server's warm-retention reaper."""

import asyncio
import importlib.util
import json
from types import ModuleType

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
            return 503, ""
        return 200, ""

    monkeypatch.setattr(server, "_post_webhook", post)
    asyncio.run(server._send_wake(record.key, account, old, items))
    assert posts == [(old.url, old.key)] + (
        [(new.url, new.key)] if change == "replace" else []
    )


def test_account_webhook_wakes_a_session_that_holds_a_handle(
    server: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = sessions.SessionRegistry(tmp_path)
    one, _ = registry.register_handle(
        "bots/one", "runner", now=1, is_live=lambda _: False, idle=60
    )
    hook = accounts.Webhook("", "https://account.invalid/wake", key="k")
    named = accounts.Webhook("named", "https://named.invalid/wake", key="k")
    account = accounts.Account(id="bots", webhooks=(hook, named))
    monkeypatch.setattr(server, "SESSIONS", registry, raising=False)
    monkeypatch.setattr(server, "WAKE_TASKS", {})
    monkeypatch.setattr(server, "_scope", lambda _: account)
    monkeypatch.setattr(server, "_live", lambda _: False)
    monkeypatch.setattr(server, "_wake_retry_delays", list)
    items = [(1, {"id": "message"})]
    monkeypatch.setattr(server, "_wake_items", lambda *_: items)
    bodies: list[dict] = []

    async def post(url: str, bearer: str, body: dict) -> tuple[int, str]:
        bodies.append(body)
        return 200, "verdict 'queued'"

    monkeypatch.setattr(server, "_post_webhook", post)
    assert server._message_wake_target(one.key) == (account, hook)
    asyncio.run(server._send_wake(one.key, account, hook, items))
    [body] = bodies
    # The session's own handle, not the handle of the entry (none).
    assert body["session"] == "one" and body["handle"] == "runner"

    # An entry for the handle takes precedence.
    registry.register_handle(
        "bots/one", "named", now=2, is_live=lambda _: False, idle=60
    )
    assert server._message_wake_target(one.key) == (account, named)

    # A session that lost its handle is not woken for messages, though the
    # account's webhook still applies to it.
    registry.register_handle(
        "bots/two", "named", now=3, is_live=lambda _: False, idle=60, takeover=True
    )
    assert one.handle == ""
    assert server._webhook_for(one.key) == (account, hook)
    assert server._message_wake_target(one.key) is None
    server._poke_webhook(one.key)
    assert server.WAKE_TASKS == {}
    asyncio.run(server._send_wake(one.key, account, hook, items))
    assert len(bodies) == 1


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


def test_topic_truncation_delivered_once_until_ack_and_persisted(
    server, tmp_path, monkeypatch
):
    import message_topic

    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(message_topic, "MAX_CACHED_MESSAGES", 1)
    registry = sessions.SessionRegistry(tmp_path)
    key = registry.ensure("bots/one", now=1).key
    topic = message_topic.MessageTopic("chan:room", "channel")
    topic.join(key, now=1)
    for i in range(3):
        message = topic.post("bots/two", now=i + 2, body=str(i))
        message_topic.append_messages(tmp_path, topic, [message])
    message_topic.save_state(tmp_path, topic)
    topic = message_topic.load_all_topics(tmp_path)[0]
    assert topic.missed[key] == 2
    monkeypatch.setattr(server, "SESSIONS", registry, raising=False)
    monkeypatch.setattr(server, "TOPICS", {topic.key: topic})
    monkeypatch.setattr(
        server, "_scope", lambda _: accounts.Account(id="bots", channels=None)
    )
    conn = server.Connection(None, "bots", "credential")
    conn.session_key = key
    sent = []

    async def send(conn, key, payload):
        sent.append(payload)
        return True

    monkeypatch.setattr(server, "_send_notify", send)
    asyncio.run(server._deliver_messages(conn, key))
    assert sent[0]["id"].startswith("trunc:msg:chan:room:")
    assert sent[1]["id"] == topic.messages[0].id
    assert sent[0]["meta"]["kind"] == "message_truncated"
    asyncio.run(server._deliver_messages(conn, key))
    assert len(sent) == 2
    # Reconnect without acknowledging: the persisted loss is replayed.
    conn = server.Connection(None, "bots", "credential")
    conn.session_key = key
    server._handle_ack(conn, {"id": sent[0]["id"]})
    server._handle_ack(conn, {"id": "trunc:msg:chan:room"})
    assert topic.missed[key] == 2
    asyncio.run(server._deliver_messages(conn, key))
    notice = sent[2]
    assert notice["id"] != sent[0]["id"]
    assert "2 earlier message(s)" in notice["content"]

    # More history is compacted while the delivered notice awaits its ACK.
    message = topic.post("bots/two", now=5, body="more")
    message_topic.append_messages(tmp_path, topic, [message])
    assert topic.missed[key] == 3
    server._handle_ack(conn, {"id": notice["id"]})
    assert notice["id"] not in conn.inflight
    assert message_topic.load_all_topics(tmp_path)[0].missed[key] == 1
    server._handle_ack(conn, {"id": notice["id"]})
    assert topic.missed[key] == 1
    asyncio.run(server._deliver_messages(conn, key))
    next_notice = sent[4]
    assert "1 earlier message(s)" in next_notice["content"]
    assert next_notice["id"] != notice["id"]
    # Even after another notice is sent, an old duplicate cannot clear it.
    server._handle_ack(conn, {"id": notice["id"]})
    assert topic.missed[key] == 1
    server._handle_ack(conn, {"id": next_notice["id"]})
    assert message_topic.load_all_topics(tmp_path)[0].missed[key] == 0
    count = len(sent)
    asyncio.run(server._deliver_messages(conn, key))
    assert len(sent) == count


def test_notify_carries_registered_session_id(server, monkeypatch):
    class Ws:
        def __init__(self) -> None:
            self.frames: list[dict] = []

        async def send(self, raw: str) -> None:
            self.frames.append(json.loads(raw))

    ws = Ws()
    conn = server.Connection(ws, "bots", "credential")
    conn.session_key = "bots/s2"
    conn.session_id = "@runner"
    monkeypatch.setattr(server, "CONNECTIONS", {"bots/s2": [conn]})
    payload = {"type": "notify", "id": "n", "content": "c", "meta": {}}
    asyncio.run(server._send_notify(conn, "bots/s2", payload))
    # A frame for a session that the connection left gets that session's id.
    asyncio.run(server._send_notify(conn, "bots/s1", payload))
    assert [f["session_id"] for f in ws.frames] == ["@runner", "s1"]


@pytest.mark.parametrize("same_pr", [True, False])
def test_concurrent_subscriptions_keep_one_tracker_and_enforce_quota(
    server, tmp_path, monkeypatch, same_pr
):
    from types import SimpleNamespace

    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(server, "TRACKERS", {})
    monkeypatch.setattr(server, "_pr_client", lambda *_: None)
    monkeypatch.setattr(server, "_wake", lambda *_: None)
    monkeypatch.setattr(server, "_session_key_for", lambda conn, msg: "bots/one")
    monkeypatch.setattr(pr_monitor, "save_state", lambda *_: None)
    monkeypatch.setattr(pr_monitor, "save_subscriber", lambda *_: None)
    monkeypatch.setattr(pr_monitor, "summarize", lambda *_: "summary")
    account = accounts.Account(id="bots", limits=accounts.Limits(pr_subscriptions=1))
    conn = SimpleNamespace(account=account)
    sent = []
    polls = []
    tasks = []

    async def send(ws, kind, msg, **fields):
        sent.append(kind)

    async def tracker_loop(tracker):
        tasks.append(tracker)

    async def run():
        monkeypatch.setattr(server, "_SUBSCRIBE_LOCKS", {})
        entered, release = asyncio.Event(), asyncio.Event()

        async def initial_poll(tracker):
            polls.append(tracker)
            if tracker.number == 1:
                entered.set()
                await release.wait()
            return "summary"

        monkeypatch.setattr(pr_monitor.PRTracker, "initial_poll", initial_poll)
        msg = {"owner": "org", "repo": "repo", "number": 1}
        first = asyncio.create_task(server._handle_subscribe(None, conn, msg))
        await entered.wait()
        second = asyncio.create_task(
            server._handle_subscribe(None, conn, {**msg, "number": 1 if same_pr else 2})
        )
        await asyncio.sleep(0)
        if same_pr:
            assert not second.done()
            assert len(polls) == 1
        else:
            # Independent PR finishes while the first fetch remains blocked,
            # consuming the last slot before the first's post-fetch check.
            await asyncio.wait_for(second, timeout=1)
            assert not first.done()
            assert len(server.TRACKERS) == 1
            assert next(iter(server.TRACKERS.values())).number == 2
        release.set()
        await asyncio.gather(first, second)
        await asyncio.sleep(0)

    monkeypatch.setattr(server, "_send", send)
    monkeypatch.setattr(server, "_tracker_loop", tracker_loop)
    asyncio.run(run())
    assert len(polls) == (1 if same_pr else 2)
    assert len(tasks) == len(server.TRACKERS) == 1
    tracker = next(iter(server.TRACKERS.values()))
    assert tracker is polls[0 if same_pr else 1] is tasks[0]
    assert tracker.subscribers == {"bots/one"}
    assert sent == [
        server.proto.SUBSCRIBED,
        server.proto.SUBSCRIBED if same_pr else server.proto.ERROR,
    ]


def test_reaper_continues_after_cleanup_failure(server, monkeypatch):
    calls = []
    sleeps = 0

    async def sleep(_):
        nonlocal sleeps
        sleeps += 1
        if sleeps == 3:
            raise asyncio.CancelledError

    def sessions_reap(_):
        calls.append("sessions")
        raise OSError("disk unavailable")

    monkeypatch.setattr(server.asyncio, "sleep", sleep)
    monkeypatch.setattr(server, "_reap_expired_sessions", sessions_reap)
    monkeypatch.setattr(
        server, "_reap_idle_trackers", lambda _: calls.append("trackers")
    )
    monkeypatch.setattr(server, "_reap_idle_topics", lambda _: calls.append("topics"))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(server._reaper_loop())
    assert calls == ["sessions", "trackers", "topics"] * 2


def test_session_failed_remove_stays_retryable(tmp_path, monkeypatch):
    registry = sessions.SessionRegistry(tmp_path)
    record = registry.ensure("bots/one", now=1)
    original = Path.unlink

    def fail(path, *args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(Path, "unlink", fail)
    with pytest.raises(OSError):
        registry.remove(record.key)
    assert registry.get(record.key) is record
    monkeypatch.setattr(Path, "unlink", original)
    registry.remove(record.key)
    assert registry.get(record.key) is None


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
