# vim: filetype=python
"""Polling cadence for GitHub PR monitoring.

Exponential backoff: base 4 minutes 30 seconds, doubling after every 2
consecutive no-update polls, capped at 8 hours. The base sits just under the
5-minute prompt-cache TTL of a subscribed agent: while a PR is active, one
extra wake with a warm cache is cheaper than a wake just after the cache goes
cold. To keep that guarantee, polls that have not backed off yet are capped at
the TTL after jitter (jitter can only shorten them, never push them past it). During business hours the interval is capped
to 1 hour instead. Business hours are 8am ET through 8pm PT, Monday-Friday;
since US Pacific is always 3 hours behind US Eastern, 8pm PT == 11pm ET, so the
window is exactly 08:00-23:00 in America/New_York (DST-safe). Outside business
hours, if the window opens before the next scheduled poll, the next poll is
pulled in to within ~1 hour of the open, so there is never more than a (jittered)
1-hour gap once business hours begin. Every interval is jittered.

Pure and deterministic given `now` and an injected RNG; unit-tested.
"""

import random
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

BASE_INTERVAL_SECONDS = 4 * 60 + 30
CACHE_TTL_CAP_SECONDS = 5 * 60  # prompt-cache TTL; hard ceiling for un-backed-off polls
MAX_INTERVAL_SECONDS = 8 * 60 * 60
BUSINESS_CAP_SECONDS = 60 * 60
JITTER = 0.15  # +/- 15%

_ET = ZoneInfo("America/New_York")
_BUSINESS_START = time(8, 0)  # 8am ET
_BUSINESS_END = time(23, 0)  # 8pm PT == 11pm ET

_DEFAULT_RNG = random.Random()


def base_interval_seconds(consecutive_no_update: int) -> float:
    """Backoff interval (pre-jitter, pre-business-cap) for the given run length."""
    doublings = min(max(0, consecutive_no_update) // 2, 30)
    return float(min(MAX_INTERVAL_SECONDS, BASE_INTERVAL_SECONDS * 2**doublings))


def _jitter(seconds: float, rng: random.Random) -> float:
    return seconds * rng.uniform(1.0 - JITTER, 1.0 + JITTER)


def in_business_hours(now: datetime) -> bool:
    et = now.astimezone(_ET)
    return et.weekday() < 5 and _BUSINESS_START <= et.time() < _BUSINESS_END


def _next_business_start(now: datetime) -> datetime | None:
    """The next business-window open strictly after `now` (None if in one now)."""
    et = now.astimezone(_ET)
    for offset in range(8):
        day = et + timedelta(days=offset)
        if day.weekday() < 5:
            start = day.replace(
                hour=_BUSINESS_START.hour, minute=0, second=0, microsecond=0
            )
            if start > now:
                return start
    return None


def compute_next_poll(
    now: datetime,
    consecutive_no_update: int,
    rng: random.Random | None = None,
) -> datetime:
    """Absolute (tz-aware) time of the next poll. `now` must be tz-aware."""
    r = rng if rng is not None else _DEFAULT_RNG
    base = base_interval_seconds(consecutive_no_update)
    interval = _jitter(base, r)
    if base <= BASE_INTERVAL_SECONDS:
        interval = min(interval, CACHE_TTL_CAP_SECONDS)
    nxt = now + timedelta(seconds=interval)
    if in_business_hours(now):
        nxt = min(nxt, now + timedelta(seconds=_jitter(BUSINESS_CAP_SECONDS, r)))
    else:
        start = _next_business_start(now)
        if start is not None and start < nxt:
            nxt = min(nxt, start + timedelta(seconds=_jitter(BUSINESS_CAP_SECONDS, r)))
    return nxt
