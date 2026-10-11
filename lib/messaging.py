# vim: filetype=python
"""Pure helpers for the agent-messaging attention model (Phase B).

This module is the home of the vocabulary shared by the server (sender-side
loudness) and the relay shim (receiver-side attention): the intent enum, the
``ambient < direct < urgent`` level scale and the ``all < direct < urgent``
threshold scale (one shared ordinal axis), the wake-gating predicates, and the
topic-key / name-validation helpers. Nothing here touches the clock, the
filesystem, or any WebSocket — it is all referentially transparent so both sides
of the server/shim split can compute the same surface decision.
"""

import re

INTENTS = frozenset({"fyi", "question", "request", "reply"})  # reaction = Phase C
# Severity is the sender's loudness knob: ``high`` is the ``@here`` that makes a
# message urgent for every recipient; ``normal`` is the default; ``low`` is a hint
# that the message is incidental (it never raises the level on its own).
SEVERITIES = frozenset({"low", "normal", "high"})

# Level (how loud the sender made a message *for this recipient*) and threshold
# (the recipient's bar) live on one shared ordinal axis so ``should_surface`` is
# a plain ``>=``. ``all`` (0) admits everything; ``urgent`` (2) admits only an
# urgent message.
_LEVELS = {"ambient": 0, "direct": 1, "urgent": 2}
_THRESHOLDS = {"all": 0, "direct": 1, "urgent": 2}
THRESHOLD_NAMES = frozenset(_THRESHOLDS)

# Channel names reuse the agent-name rule: lowercase kebab-case, 2-64 chars, no
# leading/trailing hyphen.
_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\Z")
_NAME_MIN = 2
_NAME_MAX = 64

# A reaction body is free-form but short (Phase C): a single emoji or a terse
# token, no newlines. The bound keeps reactions visually second-class to real
# messages and cheap to render inline.
REACTION_MAX = 64


class MessagingError(ValueError):
    """Base class for messaging errors; the server maps these to ERROR replies."""


class InvalidChannelName(MessagingError):
    """The channel name is not valid kebab-case within length bounds."""


class InvalidThreshold(MessagingError):
    """The threshold is not one of {all, direct, urgent}."""


class InvalidIntent(MessagingError):
    """The intent is not one of the known message intents."""


class InvalidSeverity(MessagingError):
    """The severity is not one of {low, normal, high}."""


class InvalidReaction(MessagingError):
    """The reaction body is empty, too long, or contains a newline."""


def compute_level(*, severity: str, addressed: bool, intent: str = "fyi") -> str:
    """Sender-side loudness for one recipient.

    A ``reaction`` is always ``ambient`` — terminal and second-class, it never
    wakes anyone, not even an addressed recipient or the target's author, and not
    even at ``high`` severity. This is checked first. Otherwise: ``urgent`` when
    the message is an ``@here`` (``severity == "high"``); else ``direct`` when the
    recipient is addressed (a DM recipient or ``@``-mentioned); else ``ambient``.
    Intent is otherwise orthogonal — it carries reply/display semantics, not
    loudness.
    """
    if intent == "reaction":
        return "ambient"
    if severity == "high":
        return "urgent"
    if addressed:
        return "direct"
    return "ambient"


def effective_threshold(override: str | None, default: str) -> str:
    """The recipient's bar for a topic: the per-topic override if set, else the
    agent record's global default."""
    return override or default


def should_surface(level: str, threshold: str) -> bool:
    """True iff a message at ``level`` clears ``threshold`` on the shared scale."""
    return _LEVELS[level] >= _THRESHOLDS[threshold]


def parse_handle(s: str) -> int | None:
    """Parse an agent-facing message handle to its global ordinal.

    Accepts ``"#147"`` or ``"147"`` (an optional leading ``#`` then a positive
    integer) and returns the ordinal. Returns ``None`` for a full ``"msg:"`` id, an
    empty/blank string, ``"#"``, ``"#0"`` / non-positive values, or anything
    non-numeric — the caller then treats the input as a full message id. Pure.
    """
    s = s.strip()
    if s.startswith("#"):
        s = s[1:]
    if not (s.isascii() and s.isdigit()):  # isascii guards against e.g. superscripts
        return None
    n = int(s)
    return n if n > 0 else None


def dm_key(names: list[str]) -> str:
    """Stable topic key for a DM among ``names``.

    Deduped and sorted so a given participant set always maps to one thread,
    regardless of who initiated or the order ``to`` was given in.
    """
    return "dm:" + ",".join(sorted(set(names)))


def channel_key(name: str) -> str:
    """Topic key for a named channel (validates ``name`` first)."""
    validate_channel_name(name)
    return "chan:" + name


def validate_channel_name(name: str) -> None:
    if not (_NAME_MIN <= len(name) <= _NAME_MAX) or _NAME_RE.match(name) is None:
        raise InvalidChannelName(
            f"invalid channel name {name!r}: must be {_NAME_MIN}-{_NAME_MAX} "
            "chars, lowercase kebab-case (a-z, 0-9, hyphens; no leading/trailing hyphen)"
        )


def validate_threshold(threshold: str) -> None:
    if threshold not in _THRESHOLDS:
        raise InvalidThreshold(
            f"invalid threshold {threshold!r}: must be one of "
            f"{', '.join(sorted(_THRESHOLDS))}"
        )


def validate_intent(intent: str) -> None:
    if intent not in INTENTS:
        raise InvalidIntent(
            f"invalid intent {intent!r}: must be one of {', '.join(sorted(INTENTS))}"
        )


def validate_severity(severity: str) -> None:
    if severity not in SEVERITIES:
        raise InvalidSeverity(
            f"invalid severity {severity!r}: must be one of "
            f"{', '.join(sorted(SEVERITIES))}"
        )


def validate_reaction(reaction: str) -> None:
    """Accept a short, single-line reaction body (1-``REACTION_MAX`` chars).

    Rejects empty/whitespace-only bodies, anything longer than ``REACTION_MAX``,
    and any body containing a newline — reactions are terse, inline tokens.
    """
    if not reaction.strip():
        raise InvalidReaction("invalid reaction: must not be empty or whitespace-only")
    if len(reaction) > REACTION_MAX:
        raise InvalidReaction(
            f"invalid reaction: must be at most {REACTION_MAX} chars "
            f"(got {len(reaction)})"
        )
    if "\n" in reaction or "\r" in reaction:
        raise InvalidReaction("invalid reaction: must not contain a newline")
