# vim: filetype=python
"""Unit tests for the relay's copy of the attention-model helpers (``messaging``)."""

import pytest

import messaging as m

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


def test_channel_name_rejects_trailing_newline() -> None:
    # `$` matches before a final newline; the server's rule uses `\Z`.
    with pytest.raises(m.InvalidChannelName):
        m.validate_channel_name("room\n")


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
    ],
)
def test_invalid_reactions(bad: str) -> None:
    with pytest.raises(m.InvalidReaction):
        m.validate_reaction(bad)


def test_exception_hierarchy() -> None:
    for exc in (m.InvalidChannelName, m.InvalidThreshold, m.InvalidReaction):
        assert issubclass(exc, m.MessagingError)
    assert issubclass(m.MessagingError, ValueError)
