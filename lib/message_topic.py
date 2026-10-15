# vim: filetype=python
"""Message topics: pure membership/posting logic plus per-topic persistence.

The ``PRTracker`` analog for Phase B of the agent-messaging design. A topic is a
unified primitive backing both channels (``kind == "channel"``) and DMs
(``kind == "dm"``, a degenerate channel auto-named from its participant set). Like
the PR tracker it has no server/WebSocket dependency: the clock is injected
(``now: float``) and every op is referentially transparent, so membership,
ordering, retention, and the per-subscriber acked sets are fully testable without
spawning anything.

Member keys are opaque strings owned by the caller. The server uses session keys
(``"<account>/<session id>"``, docs/specs/2026-10-10-identity.md). A message's
``sender`` is the sender's session key (or ``"system"``), and ``sender_handle`` is
the handle the sender held when it posted.

Persistence mirrors ``pr_monitor``'s split layout under
``<data_dir>/msg/<safe_key>/``: ``state.json`` (kind/topic/members/next_seq/
last_activity), an append-only ``messages.jsonl``, and one ``sub-<member>.json`` per
member (acked id set, missed count, optional per-topic threshold override). The
message log compacts past ``MAX_CACHED_MESSAGES``, bumping each subscriber's
``missed`` by the count of dropped messages it had not yet acked — exactly the PR
event-log compaction.
"""

import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import storage

MAX_CACHED_MESSAGES = 200


@dataclass
class Message:
    id: str
    seq: int
    # The sender's session key, or "system".
    sender: str
    body: str
    intent: str = "fyi"
    severity: str = "normal"
    mentions: tuple[str, ...] = ()
    created_at: float = 0.0
    target: str = ""  # reacted-to message id (Phase C); empty for normal messages
    # Server-global, monotonic message handle (Phase C addendum). 0 == unset/legacy;
    # the server allocates and passes it in, the lib stays counter-agnostic.
    ordinal: int = 0
    # The handle the sender held when it posted.
    sender_handle: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "seq": self.seq,
            "sender": self.sender,
            "body": self.body,
            "intent": self.intent,
            "severity": self.severity,
            "mentions": list(self.mentions),
            "created_at": self.created_at,
            "target": self.target,
            "ordinal": self.ordinal,
            "sender_handle": self.sender_handle,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        return cls(
            id=data["id"],
            seq=int(data["seq"]),
            sender=data["sender"],
            body=data["body"],
            intent=data.get("intent", "fyi"),
            severity=data.get("severity", "normal"),
            mentions=tuple(data.get("mentions", ())),
            created_at=float(data.get("created_at", 0.0)),
            target=data.get("target", ""),
            ordinal=int(data.get("ordinal", 0)),
            sender_handle=data.get("sender_handle", ""),
        )


class MessageTopic:
    """Mutable per-topic state. The server owns delivery/persistence around it."""

    def __init__(self, key: str, kind: str, topic: str = "") -> None:
        self.key: str = key
        self.kind: str = kind
        self.topic: str = topic
        self.members: set[str] = set()
        self.acked: dict[str, set[str]] = {}  # member key -> set of acked message ids
        self.missed: dict[str, int] = {}  # member key -> dropped-while-unacked count
        self.thresholds: dict[str, str] = {}  # member key -> per-topic override
        self.messages: list[Message] = []
        self.next_seq: int = 0
        self.last_activity: float = 0.0

    def join(self, sid: str, *, now: float, threshold: str | None = None) -> None:
        """Add ``sid`` as a member, seeding its acked set to every current message
        id so a joiner gets no replay and no wake. Records a per-topic threshold
        override when given. Joining is not activity — ``last_activity`` is left to
        ``post`` (``now`` is accepted for signature symmetry with the other ops)."""
        if sid not in self.members:
            self.acked[sid] = {m.id for m in self.messages}
            self.missed[sid] = 0
        self.members.add(sid)
        if threshold is not None:
            self.thresholds[sid] = threshold

    def leave(self, sid: str) -> None:
        """Drop ``sid`` from membership and forget its acked/missed/threshold state."""
        self.members.discard(sid)
        self.acked.pop(sid, None)
        self.missed.pop(sid, None)
        self.thresholds.pop(sid, None)

    def post(
        self,
        sender: str,
        *,
        now: float,
        body: str,
        sender_handle: str = "",
        intent: str = "fyi",
        severity: str = "normal",
        mentions: tuple[str, ...] = (),
        target: str = "",
        ordinal: int = 0,
    ) -> Message:
        """Author a message: assign the next monotonic ``seq`` and the derived id
        ``msg:<key>:<seq>``, append it, and bump ``next_seq`` and ``last_activity``.

        ``target`` carries the reacted-to message id for a reaction (Phase C,
        ``intent == "reaction"``); it is empty for an ordinary message. ``ordinal`` is
        the server-allocated global message handle (0 leaves it unset — the lib does
        not own the counter)."""
        seq = self.next_seq
        message = Message(
            id=f"msg:{self.key}:{seq}",
            seq=seq,
            sender=sender,
            body=body,
            intent=intent,
            severity=severity,
            mentions=tuple(mentions),
            created_at=now,
            target=target,
            ordinal=ordinal,
            sender_handle=sender_handle,
        )
        self.messages.append(message)
        self.next_seq = seq + 1
        self.last_activity = now
        return message

    def history_tail(self, n: int) -> list[Message]:
        """The last ``n`` messages by seq (messages are kept in seq order)."""
        if n <= 0:
            return []
        return self.messages[-n:]

    def delivery_status(self, message_id: str) -> tuple[list[str], list[str]]:
        """Partition current members into ``(delivered_sids, pending_sids)`` by
        whether they have acked ``message_id``. ``acked`` reflects ack-on-surface,
        so a member with a held (sub-threshold) message reads as pending until it
        drains. Both lists are sorted for determinism. Pure: a bogus ``message_id``
        nobody has acked simply yields everyone as pending."""
        delivered: list[str] = []
        pending: list[str] = []
        for sid in self.members:
            if message_id in self.acked.get(sid, set()):
                delivered.append(sid)
            else:
                pending.append(sid)
        return sorted(delivered), sorted(pending)

    def reactions_for(self, message_id: str) -> list[tuple[str, str]]:
        """``(sender, body)`` for every logged reaction targeting ``message_id``,
        in seq order (messages are already kept in seq order)."""
        return [
            (msg.sender, msg.body)
            for msg in self.messages
            if msg.intent == "reaction" and msg.target == message_id
        ]

    def rekey_member(self, old: str, new: str) -> None:
        """Move one member's state (membership, acked set, missed count, threshold
        override) from key ``old`` to key ``new``. If ``new`` is already a member,
        its state wins and ``old``'s is discarded (the live entry is fresher by
        construction). The caller persists (save_subscriber under ``new``,
        delete_subscriber of ``old``, save_state)."""
        if old not in self.members or old == new:
            return
        self.members.discard(old)
        acked = self.acked.pop(old, set())
        missed = self.missed.pop(old, 0)
        threshold = self.thresholds.pop(old, None)
        if new in self.members:
            return
        self.members.add(new)
        self.acked[new] = acked
        self.missed[new] = missed
        if threshold is not None:
            self.thresholds[new] = threshold

    def reapable(self, *, now: float, ttl: float) -> bool:
        """True once the topic has been both memberless and silent for ``ttl``
        seconds. ``ttl <= 0`` makes it reapable as soon as it is memberless and
        silent (``now`` has reached ``last_activity``)."""
        return not self.members and (now - self.last_activity) >= ttl


# --------------------------------------------------------------------------- #
# persistence (mirrors pr_monitor's split layout, via lib/storage.py)
# --------------------------------------------------------------------------- #
# Storage layout per topic (under <data_dir>/msg/<safe key>/):
#   state.json       kind/topic/members/next_seq/last_activity — tmp+rename per post
#   messages.jsonl   append-only message log — appended to, rewritten on compaction
#   sub-<sid>.json   one per member (acked id set, missed, threshold?) — tmp+rename
# This keeps write amplification low: a post appends a line and rewrites a small
# state file; an ack rewrites one small subscriber file and never touches the log.


def _msg_root(data_dir: Path) -> Path:
    return Path(data_dir) / "msg"


def _topic_dir(data_dir: Path, key: str) -> Path:
    return _msg_root(data_dir) / storage.safe_name(key)


def save_state(data_dir: Path, topic: MessageTopic) -> None:
    storage.atomic_write(
        _topic_dir(data_dir, topic.key) / "state.json",
        json.dumps(
            {
                "key": topic.key,
                "kind": topic.kind,
                "topic": topic.topic,
                "members": sorted(topic.members),
                "next_seq": topic.next_seq,
                "last_activity": topic.last_activity,
            }
        ),
    )


def append_messages(
    data_dir: Path, topic: MessageTopic, messages: list[Message]
) -> None:
    """Append ``messages`` to the JSONL log; compact + prune if it grows large."""
    if not messages:
        return
    directory = _topic_dir(data_dir, topic.key)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "messages.jsonl").open("a") as f:
        for m in messages:
            f.write(json.dumps(m.to_dict()) + "\n")
    if len(topic.messages) > MAX_CACHED_MESSAGES:
        # Before trimming, record per-member how many soon-to-be-dropped messages
        # were never acked by that member — those are genuinely lost, so the server
        # can surface a "history truncated" notice on (re)connect.
        dropped_ids = {m.id for m in topic.messages[:-MAX_CACHED_MESSAGES]}
        for sid in list(topic.acked):
            topic.missed[sid] = topic.missed.get(sid, 0) + len(
                dropped_ids - topic.acked.get(sid, set())
            )
        topic.messages = topic.messages[-MAX_CACHED_MESSAGES:]
        remaining = {m.id for m in topic.messages}
        storage.atomic_write(
            directory / "messages.jsonl",
            "".join(json.dumps(m.to_dict()) + "\n" for m in topic.messages),
        )
        for sid in list(topic.acked):  # drop acked ids that fell out of the log
            topic.acked[sid] &= remaining
            save_subscriber(data_dir, topic, sid)  # persists the bumped missed too


def save_subscriber(data_dir: Path, topic: MessageTopic, member: str) -> None:
    # "member" is the current field; "session_id" is read for back-compat with sub
    # files written before the name-keyed identity model (see load_topic).
    record: dict = {
        "member": member,
        "acked": sorted(topic.acked.get(member, set())),
        "missed": topic.missed.get(member, 0),
    }
    override = topic.thresholds.get(member)
    if override is not None:
        record["threshold"] = override
    storage.atomic_write(
        _topic_dir(data_dir, topic.key) / f"sub-{storage.key_filename(member)}.json",
        json.dumps(record),
    )


def delete_subscriber(data_dir: Path, topic: MessageTopic, member: str) -> None:
    path = _topic_dir(data_dir, topic.key) / f"sub-{storage.key_filename(member)}.json"
    path.unlink(missing_ok=True)


def delete_topic(data_dir: Path, key: str) -> None:
    shutil.rmtree(_topic_dir(data_dir, key), ignore_errors=True)


def load_topic(directory: Path) -> MessageTopic | None:
    """Reconstruct a single topic from its on-disk directory, or None if absent."""
    state_path = directory / "state.json"
    if not directory.is_dir() or not state_path.exists():
        return None
    try:
        state = json.loads(state_path.read_text())
    except (OSError, ValueError):
        return None
    topic = MessageTopic(
        key=state["key"],
        kind=state.get("kind", "channel"),
        topic=state.get("topic", ""),
    )
    topic.next_seq = int(state.get("next_seq", 0))
    topic.last_activity = float(state.get("last_activity", 0.0))
    topic.members = set(state.get("members", []))

    messages_path = directory / "messages.jsonl"
    if messages_path.exists():
        seen: set[str] = set()
        for line in messages_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except ValueError:
                continue
            mid = data.get("id")
            if not mid or mid in seen:
                continue
            seen.add(mid)
            topic.messages.append(Message.from_dict(data))
        topic.messages.sort(key=lambda m: m.seq)
        # The append-only log can survive a crash before state.json is replaced.
        # Never reuse an id or reap a topic based on that stale snapshot.
        if topic.messages:
            topic.next_seq = max(topic.next_seq, topic.messages[-1].seq + 1)
            topic.last_activity = max(
                topic.last_activity, max(m.created_at for m in topic.messages)
            )
        if len(topic.messages) > MAX_CACHED_MESSAGES:
            topic.messages = topic.messages[-MAX_CACHED_MESSAGES:]

    for sub_path in sorted(directory.glob("sub-*.json")):
        try:
            sub = json.loads(sub_path.read_text())
        except (OSError, ValueError):
            continue
        # "member" is the current field; pre-name-keyed sub files wrote "session_id".
        member = sub.get("member") or sub.get("session_id")
        if not member:
            continue
        topic.members.add(member)
        topic.acked[member] = set(sub.get("acked", []))
        topic.missed[member] = int(sub.get("missed", 0))
        threshold = sub.get("threshold")
        if threshold is not None:
            topic.thresholds[member] = threshold
    return topic


def load_all_topics(data_dir: Path) -> list[MessageTopic]:
    root = _msg_root(data_dir)
    if not root.is_dir():
        return []
    topics: list[MessageTopic] = []
    for directory in sorted(root.iterdir()):
        topic = load_topic(directory)
        if topic is not None:
            topics.append(topic)
    return topics
