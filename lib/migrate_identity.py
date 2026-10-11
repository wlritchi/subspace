# vim: filetype=python
"""One-time migration of the data directory to session keys.

Before docs/specs/2026-10-10-identity.md, agent records were keyed by name, topic
members and message senders were agent names, and PR subscribers and callbacks
were plain session ids. After it, all of these are session keys
(``"<account>/<session id>"``) in the default account, and records are session
records with a handle.

The migration runs at startup, before the server loads its state. It writes
``<data_dir>/identity-version`` when it is done, and does nothing if that file
says it is done. Each step skips values that are already session keys, so a run
that stopped part of the way can run again. The old agent records stay in
``agents.v1/`` as a backup.
"""

import json
import sys
import time
from pathlib import Path

import message_topic
import sessions
import storage

VERSION = 2
_MARKER = "identity-version"


def _done(data_dir: Path) -> bool:
    try:
        return int((data_dir / _MARKER).read_text().strip()) >= VERSION
    except (OSError, ValueError):
        return False


def migrate(data_dir: Path, default_account: str) -> bool:
    """Migrate ``data_dir`` if necessary. Returns True if it changed anything."""
    data_dir = Path(data_dir)
    if _done(data_dir):
        return False
    prefix = f"{default_account}/"
    name_to_key = _migrate_agents(data_dir, prefix)
    _migrate_topics(data_dir, prefix, name_to_key)
    registry = sessions.SessionRegistry(data_dir)
    now = time.time()
    for key in _migrate_pr_subscribers(data_dir, prefix) | _migrate_callbacks(
        data_dir, prefix
    ):
        registry.ensure(key, now)
    agents_dir = data_dir / "agents"
    if agents_dir.is_dir():
        agents_dir.rename(data_dir / "agents.v1")
    storage.atomic_write(data_dir / _MARKER, f"{VERSION}\n")
    print(
        f"subspace: migrated {data_dir} to session keys "
        f"({len(name_to_key)} agent records)",
        file=sys.stderr,
    )
    return True


def _migrate_agents(data_dir: Path, prefix: str) -> dict[str, str]:
    """Agent records become session records with the name as the handle.
    Returns agent name -> session key."""
    registry = sessions.SessionRegistry(data_dir)
    name_to_key: dict[str, str] = {}
    for data in storage.load_json_dir(data_dir / "agents"):
        name = data.get("name")
        session_id = data.get("session_id")
        if not isinstance(name, str) or not isinstance(session_id, str):
            continue
        key = prefix + session_id
        name_to_key[name] = key
        registry.persist(
            sessions.SessionRecord(
                key=key,
                handle=name,
                description=data.get("description", ""),
                capabilities=data.get("capabilities", ""),
                default_threshold=data.get(
                    "default_threshold", sessions.DEFAULT_THRESHOLD
                ),
                registered_at=float(data.get("registered_at", 0.0)),
                last_seen=float(data.get("last_seen", 0.0)),
                last_acked=float(data.get("last_acked", 0.0)),
            )
        )
    return name_to_key


def _migrate_topics(data_dir: Path, prefix: str, name_to_key: dict[str, str]) -> None:
    """Topic members become session keys. A member name with no record is
    dropped. Message senders and mentions that resolve become session keys."""
    root = data_dir / "msg"
    if not root.is_dir():
        return
    for directory in sorted(root.iterdir()):
        topic = message_topic.load_topic(directory)
        if topic is None:
            continue
        for member in sorted(topic.members):
            if member.startswith(prefix):
                continue
            key = name_to_key.get(member)
            if key is not None:
                topic.rekey_member(member, key)
            else:
                topic.leave(member)
        for message in topic.messages:
            if "/" not in message.sender and message.sender in name_to_key:
                message.sender_handle = message.sender
                message.sender = name_to_key[message.sender]
            message.mentions = tuple(
                name_to_key.get(m, m) if "/" not in m else m for m in message.mentions
            )
        for sub_path in directory.glob("sub-*.json"):
            sub_path.unlink()
        for member in topic.members:
            message_topic.save_subscriber(data_dir, topic, member)
        storage.atomic_write(
            directory / "messages.jsonl",
            "".join(json.dumps(m.to_dict()) + "\n" for m in topic.messages),
        )
        message_topic.save_state(data_dir, topic)


def _migrate_pr_subscribers(data_dir: Path, prefix: str) -> set[str]:
    """PR subscriber files move to session keys. Returns the keys."""
    keys: set[str] = set()
    root = data_dir / "pr"
    if not root.is_dir():
        return keys
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        for sub_path in sorted(directory.glob("sub-*.json")):
            try:
                sub = json.loads(sub_path.read_text())
            except (OSError, ValueError):
                continue
            session_id = sub.get("session_id")
            if not isinstance(session_id, str) or not session_id:
                continue
            key = session_id if session_id.startswith(prefix) else prefix + session_id
            keys.add(key)
            new_path = directory / f"sub-{storage.key_filename(key)}.json"
            if new_path == sub_path:
                continue
            storage.atomic_write(new_path, json.dumps({**sub, "session_id": key}))
            sub_path.unlink()
    return keys


def _migrate_callbacks(data_dir: Path, prefix: str) -> set[str]:
    """Scheduled callbacks move to directories named by session key. Returns the
    keys."""
    keys: set[str] = set()
    root = data_dir / "scheduled"
    if not root.is_dir():
        return keys
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        for path in sorted(directory.glob("*.json")):
            try:
                entry = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            session_id = entry.get("session_id")
            if not isinstance(session_id, str) or not session_id:
                continue
            key = session_id if session_id.startswith(prefix) else prefix + session_id
            keys.add(key)
            new_path = root / storage.key_filename(key) / path.name
            if new_path == path:
                continue
            storage.atomic_write(new_path, json.dumps({**entry, "session_id": key}))
            path.unlink()
        if directory.exists() and not any(directory.iterdir()):
            directory.rmdir()
    return keys
