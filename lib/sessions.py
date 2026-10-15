# vim: filetype=python
"""Session registry: session records, handles, and the handle ownership rules.

A session is identified by its key, ``"<account>/<session id>"``. The session id
comes from the client and is not trusted for anything except routing. A handle is
a kebab-case name that a session registers so that other sessions can address it.
Handles are unique inside an account. See docs/specs/2026-10-10-identity.md.

The clock and session liveness are injected, so the rules are testable without a
server. Records persist one JSON file per session under ``<data_dir>/sessions/``.
"""

import json
import re
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

import storage

DEFAULT_THRESHOLD = "direct"
_THRESHOLDS = frozenset({"all", "direct", "urgent"})
_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\Z")
_NAME_MIN = 2
_NAME_MAX = 64
_SESSION_ID_RE = re.compile(r"^[\x21-\x7e]{1,128}\Z")

# A session id with this prefix names a session that attached by handle.
HANDLE_SESSION_PREFIX = "@"


class SessionError(ValueError):
    """Base class for registry errors. The server sends them as ERROR replies."""


class InvalidSessionId(SessionError):
    """The session id is empty, too long, or has characters outside 0x21-0x7E."""


class InvalidName(SessionError):
    """A handle or account id is not kebab-case within the length limits."""


class InvalidThreshold(SessionError):
    """The wake threshold is not one of all, direct, urgent."""


class HandleTaken(SessionError):
    """Another session in the account holds the handle."""


class NotRegistered(SessionError):
    """The session holds no handle."""


@dataclass
class SessionRecord:
    key: str
    handle: str = ""
    description: str = ""
    capabilities: str = ""
    default_threshold: str = DEFAULT_THRESHOLD
    # When the session took its current handle. 0.0 if it holds none.
    registered_at: float = 0.0
    last_seen: float = 0.0
    # Last time the session acked a delivery. 0.0 if it has not acked since the
    # server started.
    last_acked: float = 0.0
    # Webhook wake-up: when the last wake was sent, and the highest message
    # ordinal that a wake covered.
    wake_sent_at: float = 0.0
    wake_covered: int = 0

    @property
    def account(self) -> str:
        return split_key(self.key)[0]

    @property
    def session_id(self) -> str:
        return split_key(self.key)[1]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SessionRecord":
        return cls(
            key=data["key"],
            handle=data.get("handle", ""),
            description=data.get("description", ""),
            capabilities=data.get("capabilities", ""),
            default_threshold=data.get("default_threshold", DEFAULT_THRESHOLD),
            registered_at=float(data.get("registered_at", 0.0)),
            last_seen=float(data.get("last_seen", 0.0)),
            last_acked=float(data.get("last_acked", 0.0)),
            wake_sent_at=float(data.get("wake_sent_at", 0.0)),
            wake_covered=int(data.get("wake_covered", 0)),
        )


def session_key(account: str, session_id: str) -> str:
    return f"{account}/{session_id}"


def split_key(key: str) -> tuple[str, str]:
    """Split a session key into (account, session id). Account ids have no ``/``."""
    account, _, session_id = key.partition("/")
    return account, session_id


def validate_session_id(session_id: str) -> None:
    if _SESSION_ID_RE.match(session_id) is None:
        raise InvalidSessionId(
            f"invalid session id {session_id!r}: must be 1-128 visible ASCII characters"
        )


def validate_name(name: str, what: str = "agent name") -> None:
    if not (_NAME_MIN <= len(name) <= _NAME_MAX) or _NAME_RE.match(name) is None:
        raise InvalidName(
            f"invalid {what} {name!r}: must be {_NAME_MIN}-{_NAME_MAX} chars, "
            "lowercase kebab-case (a-z, 0-9, hyphens; no leading/trailing hyphen)"
        )


def validate_threshold(threshold: str) -> None:
    if threshold not in _THRESHOLDS:
        raise InvalidThreshold(
            f"invalid threshold {threshold!r}: must be one of "
            f"{', '.join(sorted(_THRESHOLDS))}"
        )


def parse_handle_ref(ref: str, own_account: str) -> tuple[str, str]:
    """Parse ``handle`` or ``handle@account`` into (account, handle).

    A bare handle is in ``own_account``. Raises InvalidName for a bad part.
    """
    handle, sep, account = ref.partition("@")
    if not sep:
        account = own_account
    validate_name(handle)
    validate_name(account, "account id")
    return account, handle


def display_handle(record: SessionRecord, reader_account: str) -> str:
    """The handle as the reader sees it: bare in the reader's account, else
    ``handle@account``. Empty if the session holds no handle."""
    if not record.handle:
        return ""
    if record.account == reader_account:
        return record.handle
    return f"{record.handle}@{record.account}"


class SessionRegistry:
    def __init__(self, data_dir: Path) -> None:
        self._dir: Path = Path(data_dir) / "sessions"
        self._by_key: dict[str, SessionRecord] = {}
        self._by_handle: dict[tuple[str, str], str] = {}
        for data in storage.load_json_dir(self._dir):
            try:
                record = SessionRecord.from_dict(data)
            except (KeyError, TypeError, ValueError):
                continue
            self._by_key[record.key] = record
            if record.handle:
                self._by_handle[(record.account, record.handle)] = record.key

    def _path(self, key: str) -> Path:
        return storage.key_path(self._dir, key)

    def persist(self, record: SessionRecord) -> None:
        storage.atomic_write(self._path(record.key), json.dumps(record.to_dict()))
        self._by_key[record.key] = record

    def get(self, key: str) -> SessionRecord | None:
        return self._by_key.get(key)

    def records(self) -> list[SessionRecord]:
        return list(self._by_key.values())

    def by_handle(self, account: str, handle: str) -> SessionRecord | None:
        key = self._by_handle.get((account, handle))
        return self._by_key.get(key) if key is not None else None

    def ensure(self, key: str, now: float) -> SessionRecord:
        """Return the record for ``key``, and create and persist it if it is new."""
        record = self._by_key.get(key)
        if record is None:
            record = SessionRecord(key=key, last_seen=now)
            self.persist(record)
        return record

    def register_handle(
        self,
        key: str,
        handle: str,
        *,
        now: float,
        is_live: Callable[[str], bool],
        idle: float,
        takeover: bool = False,
        description: str = "",
        capabilities: str = "",
        default_threshold: str | None = None,
    ) -> tuple[SessionRecord, str | None]:
        """Give ``handle`` to session ``key``.

        Returns the record and the key of the session that lost the handle, or
        None. The handle moves from another session only with ``takeover``, or
        when that session has no connection and was last seen more than
        ``idle`` seconds ago. State does not move with the handle.
        """
        validate_name(handle)
        if default_threshold is not None:
            validate_threshold(default_threshold)
        record = self.ensure(key, now)
        displaced: str | None = None
        holder = self.by_handle(record.account, handle)
        if holder is not None and holder.key != key:
            idle_out = not is_live(holder.key) and now - holder.last_seen > idle
            if not (takeover or idle_out):
                raise HandleTaken(f"name {handle!r} is already taken")
            self._clear_handle(holder)
            self.persist(holder)
            displaced = holder.key
        if record.handle != handle:
            if record.handle:
                self._by_handle.pop((record.account, record.handle), None)
            record.handle = handle
            record.registered_at = now
            self._by_handle[(record.account, handle)] = key
        record.description = description
        record.capabilities = capabilities
        if default_threshold is not None:
            record.default_threshold = default_threshold
        record.last_seen = now
        self.persist(record)
        return record, displaced

    def _clear_handle(self, record: SessionRecord) -> None:
        if record.handle:
            self._by_handle.pop((record.account, record.handle), None)
        record.handle = ""
        record.description = ""
        record.capabilities = ""
        record.registered_at = 0.0

    def release_handle(self, key: str) -> SessionRecord | None:
        """Release the session's handle. Returns the record as it was before, or
        None if the session held no handle."""
        record = self._by_key.get(key)
        if record is None or not record.handle:
            return None
        before = SessionRecord.from_dict(record.to_dict())
        self._clear_handle(record)
        self.persist(record)
        return before

    def set_threshold(self, key: str, threshold: str) -> SessionRecord:
        validate_threshold(threshold)
        record = self._by_key.get(key)
        if record is None or not record.handle:
            raise NotRegistered("this session is not registered as an agent")
        record.default_threshold = threshold
        self.persist(record)
        return record

    def touch(self, key: str, now: float, *, persist: bool) -> None:
        """Set ``last_seen``. Write it to disk only if ``persist``."""
        record = self._by_key.get(key)
        if record is None:
            return
        record.last_seen = now
        if persist:
            self.persist(record)

    def mark_acked(self, key: str, now: float) -> None:
        """Record an ack in memory. Acks are frequent, so the value goes to disk
        only with the next write for another reason."""
        record = self._by_key.get(key)
        if record is not None:
            record.last_acked = now

    def remove(self, key: str) -> None:
        # Keep failed removals registered so the next reaper pass can retry.
        self._path(key).unlink(missing_ok=True)
        record = self._by_key.pop(key, None)
        if record is not None and record.handle:
            self._by_handle.pop((record.account, record.handle), None)

    def expired(
        self, now: float, ttl: float, is_live: Callable[[str], bool]
    ) -> list[str]:
        """Keys of sessions with no connection that were last seen ``ttl`` or
        more seconds ago."""
        return [
            r.key
            for r in self._by_key.values()
            if not is_live(r.key) and now - r.last_seen >= ttl
        ]
