# vim: filetype=python
"""Unit tests for the session registry: session keys, handles, the handle
ownership rules, validation, expiry, and persistence. The clock is injected."""

import json
from pathlib import Path
from urllib.parse import quote

import pytest

import scheduler
import sessions as s
import storage


def _never_live(_key: str) -> bool:
    return False


def _always_live(_key: str) -> bool:
    return True


IDLE = 900.0


def _register(reg: s.SessionRegistry, key: str, handle: str, **kwargs: object):
    defaults: dict[str, object] = {"now": 1000.0, "is_live": _never_live, "idle": IDLE}
    defaults.update(kwargs)
    return reg.register_handle(key, handle, **defaults)


# --- keys and validation -----------------------------------------------------


def test_session_key_round_trip() -> None:
    key = s.session_key("default", "a/b:c")
    assert key == "default/a/b:c"
    assert s.split_key(key) == ("default", "a/b:c")


@pytest.mark.parametrize("good", ["x", "@reviewer", "a" * 128, "9c1e-uuid:1/2"])
def test_valid_session_ids(good: str) -> None:
    s.validate_session_id(good)


@pytest.mark.parametrize("bad", ["", "a" * 129, "has space", "tab\t", "é"])
def test_invalid_session_ids(bad: str) -> None:
    with pytest.raises(s.InvalidSessionId):
        s.validate_session_id(bad)


def test_parse_handle_ref() -> None:
    assert s.parse_handle_ref("reviewer", "default") == ("default", "reviewer")
    assert s.parse_handle_ref("reviewer@ci", "default") == ("ci", "reviewer")
    with pytest.raises(s.InvalidName):
        s.parse_handle_ref("Bad@ci", "default")
    with pytest.raises(s.InvalidName):
        s.parse_handle_ref("ok@Bad", "default")


def test_display_handle() -> None:
    record = s.SessionRecord(key="ci/run-1", handle="reviewer")
    assert s.display_handle(record, "ci") == "reviewer"
    assert s.display_handle(record, "default") == "reviewer@ci"
    assert s.display_handle(s.SessionRecord(key="ci/x"), "ci") == ""


def test_key_filename_is_injective() -> None:
    keys = ["d/a/b", "d/a_b", "d/a%2Fb", "d/@x", "d/_x", "d/a:b", "d/a#b"]
    names = {storage.key_filename(k) for k in keys}
    assert len(names) == len(keys)
    assert storage.key_filename("default/abc-1.2_3") == "default%2Fabc-1.2_3"


# --- handles -----------------------------------------------------------------


def test_register_handle_happy_path(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    record, displaced = _register(
        reg,
        "default/s1",
        "frontend",
        description="UI work",
        capabilities="react",
        default_threshold="urgent",
    )
    assert displaced is None
    assert record.handle == "frontend"
    assert record.account == "default" and record.session_id == "s1"
    assert record.description == "UI work"
    assert record.default_threshold == "urgent"
    assert record.registered_at == 1000.0
    assert reg.by_handle("default", "frontend") == record


def test_handles_are_per_account(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer")
    record, displaced = _register(reg, "ci/s1", "reviewer", is_live=_always_live)
    assert displaced is None
    assert reg.by_handle("ci", "reviewer") == record
    assert reg.by_handle("default", "reviewer").key == "default/s1"


def test_live_holder_keeps_handle(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer")
    with pytest.raises(s.HandleTaken):
        _register(reg, "default/s2", "reviewer", now=99999.0, is_live=_always_live)


def test_recent_offline_holder_keeps_handle(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer", now=1000.0)
    with pytest.raises(s.HandleTaken):
        _register(reg, "default/s2", "reviewer", now=1000.0 + IDLE)


def test_idle_holder_loses_handle_without_takeover(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer", now=1000.0)
    record, displaced = _register(reg, "default/s2", "reviewer", now=1001.0 + IDLE)
    assert displaced == "default/s1"
    assert record.key == "default/s2"
    assert reg.get("default/s1").handle == ""


def test_takeover_moves_handle_from_live_holder(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer", description="old")
    record, displaced = _register(
        reg, "default/s2", "reviewer", is_live=_always_live, takeover=True
    )
    assert displaced == "default/s1"
    old = reg.get("default/s1")
    assert old.handle == "" and old.description == ""
    assert reg.by_handle("default", "reviewer") == record


def test_new_handle_releases_previous(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "first")
    _register(reg, "default/s1", "second", now=2000.0)
    assert reg.by_handle("default", "first") is None
    record = reg.by_handle("default", "second")
    assert record.key == "default/s1" and record.registered_at == 2000.0


def test_same_handle_again_keeps_registered_at(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer", now=1000.0, default_threshold="all")
    record, _ = _register(reg, "default/s1", "reviewer", now=5000.0)
    assert record.registered_at == 1000.0
    assert record.default_threshold == "all"  # omitted -> kept
    assert record.last_seen == 5000.0


def test_register_validation(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    with pytest.raises(s.InvalidName):
        _register(reg, "default/s1", "Not-Kebab")
    with pytest.raises(s.InvalidName):
        _register(reg, "default/s1", "a@b")
    with pytest.raises(s.InvalidThreshold):
        _register(reg, "default/s1", "ok", default_threshold="loud")


def test_release_handle(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/s1", "reviewer", description="d")
    before = reg.release_handle("default/s1")
    assert before is not None and before.handle == "reviewer"
    assert reg.by_handle("default", "reviewer") is None
    assert reg.get("default/s1").handle == ""
    assert reg.release_handle("default/s1") is None


def test_set_threshold_needs_handle(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    reg.ensure("default/s1", 1.0)
    with pytest.raises(s.NotRegistered):
        reg.set_threshold("default/s1", "all")
    _register(reg, "default/s1", "reviewer")
    assert reg.set_threshold("default/s1", "all").default_threshold == "all"


# --- persistence and expiry --------------------------------------------------


def test_persistence_round_trip(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "ci/@reviewer", "reviewer", description="d")
    reg.ensure("default/plain", 5.0)
    reg.mark_acked("ci/@reviewer", 7.0)
    reg.touch("ci/@reviewer", 8.0, persist=True)
    loaded = s.SessionRegistry(tmp_path)
    record = loaded.by_handle("ci", "reviewer")
    assert record is not None and record.key == "ci/@reviewer"
    assert record.last_acked == 7.0 and record.last_seen == 8.0
    assert loaded.get("default/plain").handle == ""


def test_touch_without_persist_stays_in_memory(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    reg.ensure("default/s1", 1.0)
    reg.touch("default/s1", 50.0, persist=False)
    assert reg.get("default/s1").last_seen == 50.0
    assert s.SessionRegistry(tmp_path).get("default/s1").last_seen == 1.0


def test_expired_and_remove(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    _register(reg, "default/old", "old", now=100.0)
    reg.ensure("default/new", 900.0)
    reg.ensure("default/live", 100.0)
    expired = reg.expired(1000.0, 500.0, lambda k: k == "default/live")
    assert expired == ["default/old"]
    reg.remove("default/old")
    assert reg.get("default/old") is None
    assert reg.by_handle("default", "old") is None
    assert s.SessionRegistry(tmp_path).get("default/old") is None


def test_failed_creation_does_not_publish_record(tmp_path, monkeypatch) -> None:
    reg = s.SessionRegistry(tmp_path)

    def fail(*args):
        raise OSError("disk full")

    monkeypatch.setattr(storage, "atomic_write", fail)
    with pytest.raises(OSError):
        reg.ensure("default/new", 1.0)
    assert reg.get("default/new") is None


def test_long_session_ids_persist_and_remove(tmp_path: Path) -> None:
    reg = s.SessionRegistry(tmp_path)
    key = s.session_key("a" * 64, ":" * 128)
    s.validate_session_id(s.split_key(key)[1])
    reg.ensure(key, 1.0)
    assert s.SessionRegistry(tmp_path).get(key) is not None
    reg.remove(key)
    assert s.SessionRegistry(tmp_path).get(key) is None


def test_existing_long_session_filename_is_kept(tmp_path: Path) -> None:
    key = "bots/" + "!" * 70
    directory = tmp_path / "sessions"
    directory.mkdir()
    existing = directory / (quote(key, safe="") + ".json")
    existing.write_text(json.dumps(s.SessionRecord(key=key, last_seen=1).to_dict()))
    reg = s.SessionRegistry(tmp_path)
    reg.touch(key, now=2, persist=True)
    assert list(directory.glob("*.json")) == [existing]
    assert s.SessionRegistry(tmp_path).get(key).last_seen == 2
    reg.remove(key)
    assert not existing.exists()


def test_key_names_change_only_when_too_long(tmp_path: Path) -> None:
    # atomic_write writes "<name>.tmp", so a file name has 4 bytes less.
    fits = "a" * (storage.NAME_MAX - len(".json.tmp"))
    assert storage.key_path(tmp_path, fits).name == fits + ".json"
    name = storage.key_path(tmp_path, fits + "a").name
    assert name.startswith("sha256:") and name.endswith(".json")
    sub = storage.key_path(tmp_path, fits + "a", prefix="sub-").name
    assert sub == "sub-" + name
    assert storage.key_filename("a" * storage.NAME_MAX) == "a" * storage.NAME_MAX
    assert storage.key_filename("a" * (storage.NAME_MAX + 1)).startswith("sha256:")


def test_scheduler_long_session_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    key = s.session_key("a" * 64, ":" * 128)
    callback_id = scheduler.schedule(key, 0.0)
    assert [e["id"] for e in scheduler.pending(key)] == [callback_id]
    scheduler.delete(key, callback_id)
    assert scheduler.pending(key) == []


def test_scheduler_delete_ignores_ids_it_did_not_make(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
    victim = tmp_path / "sessions" / "victim.json"
    victim.parent.mkdir(parents=True)
    victim.write_text("{}")
    callback_id = scheduler.schedule("ci/x", 0.0)
    scheduler.delete("ci/x", "../../sessions/victim")
    assert victim.exists()
    scheduler.delete("ci/x", callback_id)
    assert scheduler.pending("ci/x") == []
