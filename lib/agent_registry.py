# vim: filetype=python
"""Agent directory: pure registration/identity logic plus per-agent persistence.

Phase A of the agent-messaging design. This module has no server/WebSocket
dependency: the clock is injected (``now: float``) and session liveness is
supplied as a callable, so the registration rules are fully testable without
spawning anything. Records persist one JSON file per agent under
``<data_dir>/agents/<safe_name>.json``.
"""

import hashlib
import hmac
import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

import storage

DEFAULT_THRESHOLD = "direct"
_THRESHOLDS = frozenset({"all", "direct", "urgent"})
_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_NAME_MIN = 2
_NAME_MAX = 64
_RECLAIM_KEY_MAX = 256


class AgentRegistryError(ValueError):
    """Base class for registry errors; the server maps these to ERROR replies."""


class NameTaken(AgentRegistryError):
    """The requested name is held by a different, still-claimed session."""


class InvalidName(AgentRegistryError):
    """The requested name is not valid kebab-case within length bounds."""


class InvalidThreshold(AgentRegistryError):
    """The requested wake threshold is not one of {all, direct, urgent}."""


class NotRegistered(AgentRegistryError):
    """The session owns no agent record."""


class InvalidReclaimKey(AgentRegistryError):
    """The supplied reclaim key is empty/whitespace-only or too long."""


@dataclass
class AgentRecord:
    name: str
    session_id: str
    description: str = ""
    capabilities: str = ""
    working_dir: str = ""
    default_threshold: str = DEFAULT_THRESHOLD
    registered_at: float = 0.0
    last_seen: float = 0.0
    reclaim_key_hash: str = ""
    # Which holder of this name this record represents (name = service, session =
    # process; docs/specs/2026-07-09). Bumps every time the name transfers to a
    # DIFFERENT session; a same-session re-register keeps it. Audit + relay-side
    # supersession detection; never part of message addressing.
    generation: int = 1
    # Last time this session ACKed a delivery — i.e. last SURFACED a notification into
    # a runnable context (distinct from last_seen, the WS heartbeat). A connected
    # session whose last_acked has gone stale while a message is pending is the
    # context-wedge fingerprint (docs/specs/2026-07-14). Stamped in-memory on each ack;
    # rides along when the record is persisted for other reasons.
    last_acked: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AgentRecord":
        return cls(
            name=data["name"],
            session_id=data["session_id"],
            description=data.get("description", ""),
            capabilities=data.get("capabilities", ""),
            working_dir=data.get("working_dir", ""),
            default_threshold=data.get("default_threshold", DEFAULT_THRESHOLD),
            registered_at=data.get("registered_at", 0.0),
            last_seen=data.get("last_seen", 0.0),
            reclaim_key_hash=data.get("reclaim_key_hash", ""),
            generation=int(data.get("generation", 1)),
            last_acked=data.get("last_acked", 0.0),
        )


def _validate_name(name: str) -> None:
    if not (_NAME_MIN <= len(name) <= _NAME_MAX) or _NAME_RE.match(name) is None:
        raise InvalidName(
            f"invalid agent name {name!r}: must be {_NAME_MIN}-{_NAME_MAX} "
            "chars, lowercase kebab-case (a-z, 0-9, hyphens; no leading/trailing hyphen)"
        )


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _validate_reclaim_key(key: str) -> None:
    stripped = key.strip()
    if not stripped:
        raise InvalidReclaimKey("reclaim key must not be empty or whitespace-only")
    if len(stripped) > _RECLAIM_KEY_MAX:
        raise InvalidReclaimKey(
            f"reclaim key too long: max {_RECLAIM_KEY_MAX} characters"
        )


def _validate_threshold(threshold: str) -> None:
    if threshold not in _THRESHOLDS:
        raise InvalidThreshold(
            f"invalid threshold {threshold!r}: must be one of "
            f"{', '.join(sorted(_THRESHOLDS))}"
        )


class AgentRegistry:
    def __init__(self, data_dir: Path) -> None:
        self._dir: Path = Path(data_dir) / "agents"
        self._by_name: dict[str, AgentRecord] = {}
        for data in storage.load_json_dir(self._dir):
            try:
                record = AgentRecord.from_dict(data)
            except (KeyError, TypeError):
                continue
            self._by_name[record.name] = record

    def _path(self, name: str) -> Path:
        return self._dir / f"{storage.safe_name(name)}.json"

    def _persist(self, record: AgentRecord) -> None:
        self._by_name[record.name] = record
        storage.atomic_write(self._path(record.name), json.dumps(record.to_dict()))

    def _remove(self, record: AgentRecord) -> None:
        self._by_name.pop(record.name, None)
        self._path(record.name).unlink(missing_ok=True)

    def get_by_session(self, session_id: str) -> AgentRecord | None:
        for record in self._by_name.values():
            if record.session_id == session_id:
                return record
        return None

    def list(self) -> list[AgentRecord]:
        return list(self._by_name.values())

    def register(
        self,
        session_id: str,
        name: str,
        *,
        now: float,
        is_session_live: Callable[[str], bool],
        ttl: float,
        settle: float = 0.0,
        description: str = "",
        capabilities: str = "",
        working_dir: str = "",
        default_threshold: str | None = None,
        reclaim_key: str | None = None,
    ) -> AgentRecord:
        _validate_name(name)
        if default_threshold is not None:
            _validate_threshold(default_threshold)
        if reclaim_key is not None:
            _validate_reclaim_key(reclaim_key)
        succeeds_generation: int | None = None  # set when displacing an offline holder

        # Collision: the desired name is held by a *different* session. A live
        # owner is never displaced. An offline owner may be displaced by a
        # registrant presenting the owner's reclaim key once the owner has been
        # quiet for at least `settle` (see below); otherwise the reclaim-grace
        # window applies, after which the stale holder is reclaimed (same slug,
        # overwritten below).
        holder = self._by_name.get(name)
        if holder is not None and holder.session_id != session_id:
            if is_session_live(holder.session_id):
                raise NameTaken(f"name {name!r} is already taken")
            key_matches = (
                reclaim_key is not None
                and holder.reclaim_key_hash != ""
                and hmac.compare_digest(holder.reclaim_key_hash, _hash_key(reclaim_key))
            )
            offline_for = now - holder.last_seen
            within_grace = offline_for < ttl
            # A matching reclaim key normally bypasses the grace so a resumed agent
            # retakes its name immediately. But a holder that went quiet only moments
            # ago may be a live agent whose WebSocket briefly dropped during a roll —
            # and a same-key sibling (e.g. a bg spare cloned with the same pod-wide
            # reclaim key) would otherwise hijack it in that blip. Require the holder to
            # have been offline at least `settle` before the key may bypass the grace.
            # STOPGAP for the spare-collision race (pending a holistic reclaim-key
            # redesign): closes the race for blips up to `settle`, not for rolls that
            # keep the holder offline longer.
            reclaimable = key_matches and offline_for >= settle
            if within_grace and not reclaimable:
                raise NameTaken(f"name {name!r} is already taken")
            # The name transfers to a new session: the successor's record continues
            # the holder's generation sequence (name = service, session = process).
            succeeds_generation = holder.generation
            self._remove(holder)

        # One identity per session: if this session already owns a *different*
        # name, release that record (its slug differs, so delete the old file).
        owned = self.get_by_session(session_id)
        if owned is not None and owned.name != name:
            self._remove(owned)

        # Whatever remains under ``name`` is now owned by this session, if
        # anything: an idempotent self-update that preserves registered_at.
        prior = self._by_name.get(name)
        if prior is not None and prior.session_id == session_id:
            registered_at = prior.registered_at
            threshold = (
                default_threshold
                if default_threshold is not None
                else prior.default_threshold
            )
            key_hash = (
                _hash_key(reclaim_key)
                if reclaim_key is not None
                else prior.reclaim_key_hash
            )
            generation = prior.generation  # same holder: not a succession
        else:
            registered_at = now
            threshold = (
                default_threshold
                if default_threshold is not None
                else DEFAULT_THRESHOLD
            )
            key_hash = _hash_key(reclaim_key) if reclaim_key is not None else ""
            generation = (
                succeeds_generation + 1 if succeeds_generation is not None else 1
            )

        record = AgentRecord(
            name=name,
            session_id=session_id,
            description=description,
            capabilities=capabilities,
            working_dir=working_dir,
            default_threshold=threshold,
            registered_at=registered_at,
            last_seen=now,
            reclaim_key_hash=key_hash,
            generation=generation,
        )
        self._persist(record)
        return record

    def unregister(self, session_id: str) -> AgentRecord | None:
        record = self.get_by_session(session_id)
        if record is None:
            return None
        self._remove(record)
        return record

    def set_availability(self, session_id: str, default_threshold: str) -> AgentRecord:
        _validate_threshold(default_threshold)
        record = self.get_by_session(session_id)
        if record is None:
            raise NotRegistered("this session is not registered as an agent")
        record.default_threshold = default_threshold
        self._persist(record)
        return record

    def touch(self, session_id: str, now: float) -> None:
        record = self.get_by_session(session_id)
        if record is None:
            return
        record.last_seen = now
        self._persist(record)

    def mark_acked(self, session_id: str, now: float) -> None:
        """Record that this session just ACKed a delivery (surfaced a notification).
        In-memory only — acks are frequent, so we avoid a disk write per ack; the value
        rides along whenever the record is next persisted for another reason (touch/
        register). Lost on server restart (0.0), which the wedge heuristic treats as
        'no ack signal yet' rather than a false wedge."""
        record = self.get_by_session(session_id)
        if record is not None:
            record.last_acked = now
