# vim: filetype=python
"""Unit tests for the webhook wake-up rules."""

import pytest

import wake

GRACE = 60.0
COOLDOWN = 300.0


def _due(
    *,
    live: bool = False,
    has_new: bool = True,
    last_seen: float = 1000.0,
    sent_at: float = 0.0,
) -> float | None:
    return wake.due_at(
        live=live,
        has_new=has_new,
        last_seen=last_seen,
        sent_at=sent_at,
        grace=GRACE,
        cooldown=COOLDOWN,
    )


def test_no_wake_while_connected_or_with_nothing_new() -> None:
    assert _due(live=True) is None
    assert _due(has_new=False) is None


def test_first_wake_waits_for_the_grace_time() -> None:
    assert _due() == 1000.0 + GRACE


def test_grace_counts_from_the_core_start_at_the_earliest() -> None:
    due = wake.due_at(
        live=False,
        has_new=True,
        last_seen=1000.0,
        sent_at=0.0,
        grace=GRACE,
        cooldown=COOLDOWN,
        started_at=5000.0,
    )
    assert due == 5000.0 + GRACE


def test_cooldown_applies_until_the_session_connects() -> None:
    # Woken at 1100 and not connected since: wait for the cooldown.
    assert _due(sent_at=1100.0) == 1100.0 + COOLDOWN
    # Connected after the wake (last_seen moved past it): only the grace time.
    assert _due(sent_at=1100.0, last_seen=1200.0) == 1200.0 + GRACE


def test_grace_wins_when_it_ends_later_than_the_cooldown() -> None:
    assert _due(sent_at=900.0, last_seen=900.0) == max(900.0 + GRACE, 900.0 + COOLDOWN)
    assert (
        wake.due_at(
            live=False,
            has_new=True,
            last_seen=1000.0,
            sent_at=1000.0,
            grace=600.0,
            cooldown=10.0,
        )
        == 1600.0
    )


@pytest.mark.parametrize(
    ("status", "expected"),
    [(None, True), (429, True), (500, True), (503, True), (400, False), (404, False)],
)
def test_retryable(status: int | None, expected: bool) -> None:
    assert wake.retryable(status) is expected


def test_body_lists_at_most_max_items() -> None:
    items = [{"id": f"m{i}"} for i in range(wake.MAX_ITEMS + 3)]
    payload = wake.body("bots", "runner", items)
    assert payload["event"] == "wake"
    assert payload["account"] == "bots" and payload["handle"] == "runner"
    assert payload["pending"] == wake.MAX_ITEMS + 3
    assert payload["items"] == items[: wake.MAX_ITEMS]
