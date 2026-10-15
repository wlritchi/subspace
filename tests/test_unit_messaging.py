# vim: filetype=python
"""Unit tests for the Phase B message core: the attention-model helpers
(``messaging``) and the topic primitive plus persistence (``message_topic``).

The clock is injected via explicit ``now`` values and storage goes through
``tmp_path``; nothing here spawns a server, touches wall-clock time, or mocks.
"""

from pathlib import Path

import pytest

import message_topic as mt
import messaging as m
from message_topic import Message, MessageTopic


def test_load_recovers_sequence_and_activity_after_log_only_append(
    tmp_path: Path,
) -> None:
    topic = MessageTopic("chan:room", "channel")
    mt.save_state(tmp_path, topic)
    first = topic.post("alice", now=42, body="durable log")
    mt.append_messages(tmp_path, topic, [first])
    loaded = mt.load_all_topics(tmp_path)[0]
    assert loaded.next_seq == first.seq + 1
    assert loaded.last_activity == 42
    assert not loaded.reapable(now=43, ttl=10)
    second = loaded.post("alice", now=44, body="next")
    assert second.id != first.id
    mt.append_messages(tmp_path, loaded, [second])
    assert len(mt.load_all_topics(tmp_path)[0].messages) == 2


def test_rejoin_preserves_pending_and_missed_and_updates_threshold(
    tmp_path: Path,
) -> None:
    topic = MessageTopic("chan:room", "channel")
    topic.join("alice", now=1, threshold="direct")
    pending = topic.post("bob", now=2, body="pending")
    topic.missed["alice"] = 3
    topic.join("alice", now=3, threshold="all")
    assert pending.id not in topic.acked["alice"]
    assert topic.missed["alice"] == 3
    assert topic.thresholds["alice"] == "all"
    mt.save_state(tmp_path, topic)
    mt.save_subscriber(tmp_path, topic, "alice")
    loaded = mt.load_all_topics(tmp_path)[0]
    assert loaded.acked["alice"] == set()
    assert loaded.missed["alice"] == 3
    assert loaded.thresholds["alice"] == "all"


# --------------------------------------------------------------------------- #
# messaging: levels / thresholds / surfacing
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "severity, addressed, expected",
    [
        ("high", True, "urgent"),  # @here always wins
        ("high", False, "urgent"),
        ("normal", True, "direct"),  # addressed but not loud
        ("normal", False, "ambient"),  # neither
        ("low", True, "direct"),  # only "high" is urgent
        ("low", False, "ambient"),
    ],
)
def test_compute_level_matrix(severity: str, addressed: bool, expected: str) -> None:
    assert m.compute_level(severity=severity, addressed=addressed) == expected


def test_compute_level_default_intent_matches_explicit_fyi() -> None:
    # The intent="fyi" default leaves every existing caller's result unchanged.
    for severity in ("low", "normal", "high"):
        for addressed in (True, False):
            assert m.compute_level(
                severity=severity, addressed=addressed
            ) == m.compute_level(severity=severity, addressed=addressed, intent="fyi")


@pytest.mark.parametrize(
    "severity, addressed",
    [
        ("high", True),  # reaction beats @here...
        ("high", False),
        ("normal", True),  # ...and beats addressing
        ("normal", False),
        ("low", False),
    ],
)
def test_compute_level_reaction_is_always_ambient(
    severity: str, addressed: bool
) -> None:
    # A reaction never wakes anyone: it is ambient even when it would otherwise be
    # urgent (high severity) or direct (addressed).
    assert (
        m.compute_level(severity=severity, addressed=addressed, intent="reaction")
        == "ambient"
    )


def test_should_surface_full_matrix() -> None:
    levels = {"ambient": 0, "direct": 1, "urgent": 2}
    thresholds = {"all": 0, "direct": 1, "urgent": 2}
    for level, lo in levels.items():
        for threshold, to in thresholds.items():
            assert m.should_surface(level, threshold) is (lo >= to), (
                level,
                threshold,
            )


def test_should_surface_edges() -> None:
    # `all` admits everything; `urgent` admits only an urgent message.
    assert all(m.should_surface(lv, "all") for lv in ("ambient", "direct", "urgent"))
    assert m.should_surface("urgent", "urgent") is True
    assert m.should_surface("direct", "urgent") is False
    assert m.should_surface("ambient", "urgent") is False


@pytest.mark.parametrize(
    "override, default, expected",
    [
        (None, "direct", "direct"),
        ("urgent", "direct", "urgent"),
        ("all", "urgent", "all"),
        ("", "direct", "direct"),  # empty override falls through to default
    ],
)
def test_effective_threshold(override: str | None, default: str, expected: str) -> None:
    assert m.effective_threshold(override, default) == expected


def test_intents_set() -> None:
    assert m.INTENTS == frozenset({"fyi", "question", "request", "reply"})
    assert "reaction" not in m.INTENTS  # Phase C


# --------------------------------------------------------------------------- #
# messaging: keys + validation
# --------------------------------------------------------------------------- #


def test_dm_key_sorts_dedups_and_is_order_independent() -> None:
    assert m.dm_key(["b", "a"]) == "dm:a,b"
    assert m.dm_key(["a", "b", "a"]) == "dm:a,b"  # dedupe
    assert m.dm_key(["b", "a"]) == m.dm_key(["a", "b"])  # order independent
    assert m.dm_key(["solo"]) == "dm:solo"


def test_channel_key() -> None:
    assert m.channel_key("general") == "chan:general"
    with pytest.raises(m.InvalidChannelName):
        m.channel_key("Bad Name")


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("#147", 147),  # canonical handle
        ("147", 147),  # bare ordinal
        ("#1", 1),
        ("  #147  ", 147),  # surrounding whitespace tolerated
        ("msg:chan:room:3", None),  # a full id is not a handle
        ("", None),
        ("#", None),
        ("#0", None),  # zero is non-positive
        ("0", None),
        ("-5", None),  # negatives are not ordinals
        ("abc", None),
        ("12x", None),  # not purely numeric
        ("#12.5", None),
    ],
)
def test_parse_handle(raw: str, expected: int | None) -> None:
    assert m.parse_handle(raw) == expected


@pytest.mark.parametrize("good", ["ab", "a1", "my-chan", "a-b-c", "x" * 64])
def test_valid_channel_names(good: str) -> None:
    m.validate_channel_name(good)  # must not raise


@pytest.mark.parametrize(
    "bad",
    ["", "a", "-x", "x-", "UP", "has space", "naïve", "x" * 65, "a_b"],
)
def test_invalid_channel_names(bad: str) -> None:
    with pytest.raises(m.InvalidChannelName):
        m.validate_channel_name(bad)


@pytest.mark.parametrize("good", ["all", "direct", "urgent"])
def test_valid_thresholds(good: str) -> None:
    m.validate_threshold(good)  # must not raise


@pytest.mark.parametrize("bad", ["loud", "ALL", "", "high", "ambient"])
def test_invalid_thresholds(bad: str) -> None:
    with pytest.raises(m.InvalidThreshold):
        m.validate_threshold(bad)


@pytest.mark.parametrize("good", ["👍", "ok", "ack", "x", "y" * m.REACTION_MAX])
def test_valid_reactions(good: str) -> None:
    m.validate_reaction(good)  # must not raise


@pytest.mark.parametrize(
    "bad",
    [
        "",  # empty
        "   ",  # whitespace-only
        "\t",  # whitespace-only
        "x" * (m.REACTION_MAX + 1),  # too long
        "a\nb",  # contains a newline
        "\n",  # newline only
        "ok\u2028done",  # Unicode line separator
        "ok\x85",  # next line (NEL)
        "a\rb",  # carriage return
    ],
)
def test_invalid_reactions(bad: str) -> None:
    with pytest.raises(m.InvalidReaction):
        m.validate_reaction(bad)


def test_exception_hierarchy() -> None:
    for exc in (m.InvalidChannelName, m.InvalidThreshold, m.InvalidReaction):
        assert issubclass(exc, m.MessagingError)
    assert issubclass(m.MessagingError, ValueError)


# --------------------------------------------------------------------------- #
# Message dataclass
# --------------------------------------------------------------------------- #


def test_message_round_trip_and_mentions_as_list() -> None:
    msg = Message(
        id="msg:chan:x:0",
        seq=0,
        sender="s1",
        body="hi",
        intent="question",
        severity="high",
        mentions=("a", "b"),
        created_at=12.5,
    )
    d = msg.to_dict()
    assert isinstance(d["mentions"], list) and d["mentions"] == ["a", "b"]
    back = Message.from_dict(d)
    assert back == msg
    assert isinstance(back.mentions, tuple)  # tuple in memory


def test_message_defaults() -> None:
    msg = Message(id="i", seq=3, sender="s", body="b")
    assert msg.intent == "fyi"
    assert msg.severity == "normal"
    assert msg.mentions == ()
    assert msg.created_at == 0.0
    assert msg.target == ""  # normal messages carry no reaction target
    assert msg.ordinal == 0  # 0 == unset/legacy; the server allocates the handle


def test_message_ordinal_round_trips() -> None:
    msg = Message(id="msg:chan:x:0", seq=0, sender="s", body="b", ordinal=147)
    d = msg.to_dict()
    assert d["ordinal"] == 147
    assert Message.from_dict(d) == msg

    # The field is always present in to_dict and defaults to 0; from_dict tolerates a
    # legacy dict written before ordinals existed.
    plain = Message(id="i", seq=0, sender="s", body="b")
    assert plain.to_dict()["ordinal"] == 0
    legacy = {k: v for k, v in plain.to_dict().items() if k != "ordinal"}
    assert Message.from_dict(legacy).ordinal == 0


def test_message_target_round_trips() -> None:
    # A reaction message: target points at the reacted-to message id.
    reaction = Message(
        id="msg:chan:x:5",
        seq=5,
        sender="s2",
        body="👍",
        intent="reaction",
        target="msg:chan:x:2",
    )
    d = reaction.to_dict()
    assert d["target"] == "msg:chan:x:2"
    assert Message.from_dict(d) == reaction

    # The field is always present in to_dict and defaults to "" for normal
    # messages; from_dict tolerates a legacy dict without it.
    plain = Message(id="i", seq=0, sender="s", body="b")
    assert plain.to_dict()["target"] == ""
    legacy = {k: v for k, v in plain.to_dict().items() if k != "target"}
    assert Message.from_dict(legacy).target == ""


# --------------------------------------------------------------------------- #
# MessageTopic: membership / posting / history / reaping
# --------------------------------------------------------------------------- #


def _post(topic: MessageTopic, sender: str, now: float, body: str = "x") -> Message:
    return topic.post(
        sender, now=now, body=body, intent="fyi", severity="normal", mentions=()
    )


def test_join_seeds_acked_to_all_current_ids_no_replay() -> None:
    topic = MessageTopic("chan:general", "channel")
    a = _post(topic, "s1", now=1.0)
    b = _post(topic, "s1", now=2.0)
    topic.join("reader", now=3.0)
    # Everything that existed at join time is pre-acked: no replay, no wake.
    assert topic.acked["reader"] == {a.id, b.id}
    assert "reader" in topic.members
    assert topic.missed["reader"] == 0

    # A message posted *after* the join is genuinely unacked for the joiner.
    c = _post(topic, "s1", now=4.0)
    unacked = [msg for msg in topic.messages if msg.id not in topic.acked["reader"]]
    assert unacked == [c]


def test_join_records_threshold_override() -> None:
    topic = MessageTopic("chan:general", "channel")
    topic.join("s1", now=1.0, threshold="urgent")
    topic.join("s2", now=1.0)
    assert topic.thresholds == {"s1": "urgent"}


def test_leave_drops_all_member_state() -> None:
    topic = MessageTopic("chan:general", "channel")
    topic.join("s1", now=1.0, threshold="urgent")
    _post(topic, "s1", now=2.0)
    topic.acked["s1"].add("msg:chan:general:0")
    topic.missed["s1"] = 3
    topic.leave("s1")
    assert "s1" not in topic.members
    assert "s1" not in topic.acked
    assert "s1" not in topic.missed
    assert "s1" not in topic.thresholds


def test_post_assigns_increasing_seq_id_and_bumps_activity() -> None:
    topic = MessageTopic("chan:general", "channel")
    a = topic.post(
        "s1", now=10.0, body="first", intent="fyi", severity="normal", mentions=()
    )
    b = topic.post(
        "s2",
        now=20.0,
        body="second",
        intent="reply",
        severity="high",
        mentions=("s1",),
    )
    assert (a.seq, a.id) == (0, "msg:chan:general:0")
    assert (b.seq, b.id) == (1, "msg:chan:general:1")
    assert topic.next_seq == 2
    assert topic.last_activity == 20.0
    assert b.sender == "s2"
    assert b.mentions == ("s1",)
    assert b.created_at == 20.0


def test_post_stores_passed_ordinal() -> None:
    topic = MessageTopic("chan:general", "channel")
    msg = topic.post("s1", now=1.0, body="hi", ordinal=147)
    assert msg.ordinal == 147
    # The lib does not own the counter: omitting the param leaves the ordinal unset.
    plain = topic.post("s1", now=2.0, body="ho")
    assert plain.ordinal == 0


def test_history_tail() -> None:
    topic = MessageTopic("chan:general", "channel")
    msgs = [_post(topic, "s1", now=float(i)) for i in range(5)]
    assert topic.history_tail(2) == msgs[-2:]
    assert topic.history_tail(0) == []
    assert topic.history_tail(10) == msgs  # n larger than count -> all


def test_delivery_status_partitions_members_by_acked() -> None:
    topic = MessageTopic("chan:general", "channel")
    topic.join("s-a", now=0.0)
    topic.join("s-b", now=0.0)
    topic.join("s-c", now=0.0)
    msg = _post(topic, "s-a", now=1.0)  # post-join, so unacked for everyone
    # Two members have surfaced/acked it; s-b is still holding it (sub-threshold).
    topic.acked["s-a"].add(msg.id)
    topic.acked["s-c"].add(msg.id)

    delivered, pending = topic.delivery_status(msg.id)
    assert delivered == ["s-a", "s-c"]  # sorted
    assert pending == ["s-b"]

    # A message id nobody has acked -> everyone pending; result stays sorted.
    delivered, pending = topic.delivery_status("msg:chan:general:999")
    assert delivered == []
    assert pending == ["s-a", "s-b", "s-c"]


def test_reactions_for_filters_by_target_and_intent_in_order() -> None:
    topic = MessageTopic("chan:general", "channel")
    target_x = _post(topic, "s1", now=1.0, body="hello")  # the reacted-to message
    target_y = _post(topic, "s2", now=2.0, body="other")

    def _react(sender: str, body: str, target: str, now: float) -> None:
        seq = topic.next_seq
        topic.messages.append(
            Message(
                id=f"msg:{topic.key}:{seq}",
                seq=seq,
                sender=sender,
                body=body,
                intent="reaction",
                target=target,
                created_at=now,
            )
        )
        topic.next_seq = seq + 1

    _react("s2", "👍", target_x.id, now=3.0)
    _post(topic, "s3", now=4.0, body="a normal message, not a reaction")
    _react("s3", "🎉", target_y.id, now=5.0)  # reacts to Y, must be excluded
    _react("s4", "ack", target_x.id, now=6.0)

    assert topic.reactions_for(target_x.id) == [("s2", "👍"), ("s4", "ack")]
    assert topic.reactions_for(target_y.id) == [("s3", "🎉")]
    assert topic.reactions_for("msg:chan:general:404") == []


def test_reapable_member_present_is_false() -> None:
    topic = MessageTopic("chan:x", "channel")
    topic.join("s1", now=0.0)
    _post(topic, "s1", now=100.0)
    assert topic.reapable(now=1_000_000.0, ttl=10.0) is False


def test_reapable_memberless_and_silent() -> None:
    topic = MessageTopic("chan:x", "channel")
    topic.join("s1", now=0.0)
    _post(topic, "s1", now=100.0)
    topic.leave("s1")
    assert topic.reapable(now=105.0, ttl=10.0) is False  # silent 5s < ttl
    assert topic.reapable(now=110.0, ttl=10.0) is True  # silent exactly ttl


def test_reapable_zero_ttl_edge() -> None:
    topic = MessageTopic("chan:x", "channel")
    _post(topic, "s1", now=50.0)  # never joined -> memberless
    assert topic.reapable(now=50.0, ttl=0.0) is True
    assert topic.reapable(now=49.9, ttl=0.0) is False  # before activity


# --------------------------------------------------------------------------- #
# MessageTopic: compaction
# --------------------------------------------------------------------------- #


def test_compaction_bumps_missed_for_unacked(tmp_path: Path) -> None:
    topic = MessageTopic("chan:big", "channel")
    topic.join("reader", now=0.0)  # acked seeded empty (no messages yet)
    overflow = 5
    for i in range(mt.MAX_CACHED_MESSAGES + overflow):
        msg = _post(topic, "s1", now=float(i + 1))
        mt.append_messages(tmp_path, topic, [msg])
    # Exactly `overflow` of the oldest messages fell out of the cache, none of
    # which the reader had acked -> they count as missed.
    assert len(topic.messages) == mt.MAX_CACHED_MESSAGES
    assert topic.missed["reader"] == overflow
    # Dropped ids no longer linger in the (empty) acked set.
    assert topic.acked["reader"] == set()
    # The bumped count was persisted by the compaction path's save_subscriber.
    mt.save_state(tmp_path, topic)
    sub = mt.load_topic(tmp_path / "msg" / "chan_big")
    assert sub is not None
    assert sub.missed["reader"] == overflow


# --------------------------------------------------------------------------- #
# MessageTopic: persistence round-trip
# --------------------------------------------------------------------------- #


def test_persistence_round_trip(tmp_path: Path) -> None:
    topic = MessageTopic("chan:general", "channel", topic="dev chat")
    topic.join("s1", now=1.0, threshold="urgent")
    topic.join("s2", now=1.0)
    a = topic.post(
        "s1", now=10.0, body="hello", intent="fyi", severity="normal", mentions=("s2",)
    )
    b = topic.post(
        "s2", now=11.0, body="hi back", intent="reply", severity="normal", mentions=()
    )
    mt.append_messages(tmp_path, topic, [a])
    mt.append_messages(tmp_path, topic, [b])
    topic.acked["s1"].add(a.id)  # simulate s1 having acked the first message

    mt.save_state(tmp_path, topic)
    mt.save_subscriber(tmp_path, topic, "s1")
    mt.save_subscriber(tmp_path, topic, "s2")

    topics = mt.load_all_topics(tmp_path)
    assert len(topics) == 1
    loaded = topics[0]

    assert loaded.key == "chan:general"
    assert loaded.kind == "channel"
    assert loaded.topic == "dev chat"
    assert loaded.members == {"s1", "s2"}
    assert loaded.next_seq == 2
    assert loaded.last_activity == 11.0

    assert [msg.id for msg in loaded.messages] == [a.id, b.id]
    assert [msg.seq for msg in loaded.messages] == [0, 1]
    assert [msg.body for msg in loaded.messages] == ["hello", "hi back"]
    assert loaded.messages[0].mentions == ("s2",)
    assert loaded.messages[0].intent == "fyi"
    assert loaded.messages[1].intent == "reply"

    assert loaded.acked["s1"] == {a.id}
    assert loaded.acked["s2"] == set()
    assert loaded.thresholds.get("s1") == "urgent"
    assert "s2" not in loaded.thresholds


def test_load_all_topics_empty_dir(tmp_path: Path) -> None:
    assert mt.load_all_topics(tmp_path) == []


def test_delete_topic_and_subscriber(tmp_path: Path) -> None:
    topic = MessageTopic("chan:gone", "channel")
    topic.join("s1", now=1.0)
    msg = _post(topic, "s1", now=2.0)
    mt.append_messages(tmp_path, topic, [msg])
    mt.save_state(tmp_path, topic)
    mt.save_subscriber(tmp_path, topic, "s1")

    mt.delete_subscriber(tmp_path, topic, "s1")
    reloaded = mt.load_topic(tmp_path / "msg" / "chan_gone")
    assert reloaded is not None
    assert "s1" not in reloaded.acked

    mt.delete_topic(tmp_path, "chan:gone")
    assert mt.load_all_topics(tmp_path) == []
    mt.delete_subscriber(tmp_path, topic, "s1")  # idempotent, must not raise


# --------------------------------------------------------------------------- #
# name-keyed membership: rekey_member + legacy sub-file compat
# --------------------------------------------------------------------------- #


def test_rekey_member_moves_all_state() -> None:
    topic = MessageTopic("chan:general", "channel")
    topic.join("sid-old", now=1.0, threshold="urgent")
    a = _post(topic, "someone", now=2.0)
    topic.acked["sid-old"].add(a.id)
    topic.missed["sid-old"] = 2

    topic.rekey_member("sid-old", "worker")
    assert topic.members == {"worker"}
    assert topic.acked["worker"] == {a.id}
    assert topic.missed["worker"] == 2
    assert topic.thresholds == {"worker": "urgent"}
    assert "sid-old" not in topic.acked
    assert "sid-old" not in topic.missed
    assert "sid-old" not in topic.thresholds


def test_rekey_member_existing_target_state_wins() -> None:
    topic = MessageTopic("chan:general", "channel")
    a = _post(topic, "someone", now=1.0)
    topic.join("worker", now=2.0)  # live name entry: pre-acks `a`
    topic.join("sid-old", now=2.0)
    topic.missed["sid-old"] = 5

    topic.rekey_member("sid-old", "worker")
    assert topic.members == {"worker"}
    # The existing name-keyed entry is fresher by construction; the old sid's
    # state (its missed count, its acked set) is discarded, not merged.
    assert topic.acked["worker"] == {a.id}
    assert topic.missed["worker"] == 0


def test_rekey_member_noops() -> None:
    topic = MessageTopic("chan:general", "channel")
    topic.join("worker", now=1.0)
    topic.rekey_member("absent", "worker")  # old key not a member
    topic.rekey_member("worker", "worker")  # old == new
    assert topic.members == {"worker"}
    assert topic.acked["worker"] == set()


def test_load_topic_reads_legacy_session_id_sub_field(tmp_path: Path) -> None:
    """Sub files written before the name-keyed identity model carry a
    ``session_id`` field instead of ``member``; loading honors both."""
    topic = MessageTopic("chan:general", "channel")
    topic.join("sid-legacy", now=1.0)
    mt.save_state(tmp_path, topic)
    directory = tmp_path / "msg" / "chan_general"
    (directory / "sub-sid-legacy.json").write_text(
        '{"session_id": "sid-legacy", "acked": ["x"], "missed": 4, "threshold": "all"}'
    )

    loaded = mt.load_topic(directory)
    assert loaded is not None
    assert loaded.members == {"sid-legacy"}
    assert loaded.acked["sid-legacy"] == {"x"}
    assert loaded.missed["sid-legacy"] == 4
    assert loaded.thresholds["sid-legacy"] == "all"


def test_save_subscriber_writes_member_field(tmp_path: Path) -> None:
    import json as _json

    topic = MessageTopic("chan:general", "channel")
    topic.join("worker", now=1.0)
    mt.save_state(tmp_path, topic)
    mt.save_subscriber(tmp_path, topic, "worker")
    data = _json.loads(
        (tmp_path / "msg" / "chan_general" / "sub-worker.json").read_text()
    )
    assert data["member"] == "worker"
    assert "session_id" not in data
