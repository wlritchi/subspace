# vim: filetype=python
"""Unit tests for the identity migration, against a data directory in the format
that the name-keyed server wrote."""

import json
from pathlib import Path

import message_topic as mt
import migrate_identity
import sessions
import storage


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _old_data_dir(root: Path) -> None:
    for name, sid, threshold in [
        ("frontend", "sid-1", "all"),
        ("backend", "sid-2", "direct"),
    ]:
        _write_json(
            root / "agents" / f"{name}.json",
            {
                "name": name,
                "session_id": sid,
                "description": f"{name} work",
                "capabilities": "",
                "working_dir": "/repo",
                "default_threshold": threshold,
                "registered_at": 10.0,
                "last_seen": 20.0,
                "reclaim_key_hash": "abc",
                "generation": 3,
                "last_acked": 15.0,
            },
        )
    chan = root / "msg" / "chan_ops"
    _write_json(
        chan / "state.json",
        {
            "key": "chan:ops",
            "kind": "channel",
            "topic": "ops talk",
            "members": ["frontend", "backend", "ghost"],
            "next_seq": 2,
            "last_activity": 5.0,
        },
    )
    (chan / "messages.jsonl").write_text(
        json.dumps(
            {
                "id": "msg:chan:ops:0",
                "seq": 0,
                "sender": "frontend",
                "body": "hi",
                "mentions": ["backend", "nobody"],
                "ordinal": 1,
            }
        )
        + "\n"
        + json.dumps(
            {"id": "msg:chan:ops:1", "seq": 1, "sender": "system", "body": "note"}
        )
        + "\n"
    )
    _write_json(
        chan / "sub-frontend.json",
        {"member": "frontend", "acked": ["msg:chan:ops:0"], "threshold": "urgent"},
    )
    _write_json(chan / "sub-backend.json", {"member": "backend", "acked": []})
    _write_json(chan / "sub-ghost.json", {"member": "ghost", "acked": []})
    dm = root / "msg" / "dm_backend_frontend"
    _write_json(
        dm / "state.json",
        {
            "key": "dm:backend,frontend",
            "kind": "dm",
            "members": ["backend", "frontend"],
        },
    )
    _write_json(dm / "sub-backend.json", {"member": "backend", "acked": []})
    _write_json(dm / "sub-frontend.json", {"member": "frontend", "acked": []})
    pr = root / "pr" / "o_r#1"
    _write_json(
        pr / "sub-sid-1.json", {"session_id": "sid-1", "acked": ["e1"], "missed": 2}
    )
    _write_json(pr / "sub-sid-9.json", {"session_id": "sid-9", "acked": []})
    _write_json(
        root / "scheduled" / "sid-1" / "cb1.json",
        {"id": "cb1", "session_id": "sid-1", "due_at": 100.0, "kind": "scheduled"},
    )


def test_migrates_old_data_dir(tmp_path: Path) -> None:
    _old_data_dir(tmp_path)
    assert migrate_identity.migrate(tmp_path, "default")

    assert (tmp_path / "identity-version").read_text().strip() == "2"
    assert not (tmp_path / "agents").exists()
    assert (tmp_path / "agents.v1" / "frontend.json").exists()

    registry = sessions.SessionRegistry(tmp_path)
    frontend = registry.by_handle("default", "frontend")
    assert frontend is not None and frontend.key == "default/sid-1"
    assert frontend.default_threshold == "all"
    assert frontend.description == "frontend work"
    assert frontend.last_acked == 15.0
    # A PR subscriber with no agent record gets a session record, and no handle.
    orphan = registry.get("default/sid-9")
    assert orphan is not None and orphan.handle == ""

    chan = mt.load_topic(tmp_path / "msg" / "chan_ops")
    assert chan is not None
    assert chan.members == {"default/sid-1", "default/sid-2"}
    assert chan.thresholds == {"default/sid-1": "urgent"}
    assert chan.acked["default/sid-1"] == {"msg:chan:ops:0"}
    first, second = chan.messages
    assert first.sender == "default/sid-1" and first.sender_handle == "frontend"
    assert first.mentions == ("default/sid-2", "nobody")
    assert second.sender == "system"
    assert sorted(p.name for p in (tmp_path / "msg" / "chan_ops").glob("sub-*")) == [
        f"sub-{storage.key_filename('default/sid-1')}.json",
        f"sub-{storage.key_filename('default/sid-2')}.json",
    ]

    dm = mt.load_topic(tmp_path / "msg" / "dm_backend_frontend")
    assert dm is not None and dm.key == "dm:backend,frontend"
    assert dm.members == {"default/sid-1", "default/sid-2"}

    pr = tmp_path / "pr" / "o_r#1"
    assert not (pr / "sub-sid-1.json").exists()
    moved = json.loads(
        (pr / f"sub-{storage.key_filename('default/sid-1')}.json").read_text()
    )
    assert moved == {"session_id": "default/sid-1", "acked": ["e1"], "missed": 2}

    callback = (
        tmp_path / "scheduled" / storage.key_filename("default/sid-1") / "cb1.json"
    )
    assert json.loads(callback.read_text())["session_id"] == "default/sid-1"
    assert not (tmp_path / "scheduled" / "sid-1").exists()


def test_runs_once(tmp_path: Path) -> None:
    _old_data_dir(tmp_path)
    assert migrate_identity.migrate(tmp_path, "default")
    assert not migrate_identity.migrate(tmp_path, "default")


def test_rerun_after_partial_run_keeps_migrated_values(tmp_path: Path) -> None:
    _old_data_dir(tmp_path)
    assert migrate_identity.migrate(tmp_path, "default")
    # Simulate a run that stopped before the marker and the agents rename.
    (tmp_path / "identity-version").unlink()
    (tmp_path / "agents.v1").rename(tmp_path / "agents")
    assert migrate_identity.migrate(tmp_path, "default")
    chan = mt.load_topic(tmp_path / "msg" / "chan_ops")
    assert chan is not None
    assert chan.members == {"default/sid-1", "default/sid-2"}
    assert chan.messages[0].sender == "default/sid-1"


def test_empty_data_dir(tmp_path: Path) -> None:
    assert migrate_identity.migrate(tmp_path, "home")
    assert (tmp_path / "identity-version").exists()
    assert not migrate_identity.migrate(tmp_path, "home")
