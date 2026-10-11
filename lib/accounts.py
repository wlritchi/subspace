# vim: filetype=python
"""Accounts and credentials: the accounts file, tokens, and authentication.

An account is the authentication and authorization boundary. Each credential
gives access to one account. The master token always gives access to the default
account. See docs/specs/2026-10-10-identity.md and
docs/specs/2026-10-10-remote-agent-mcp.md.

stdlib only, so scripts/new-credential.py can import it without dependencies.
"""

import hashlib
import hmac
import os
import re
import secrets
import sys
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

PERMISSIONS = frozenset({"messaging", "schedule", "pr"})
DEFAULT_PERMISSIONS = frozenset({"messaging"})
_SEVERITY_ORDER = ("low", "normal", "high")
DEFAULT_ACCOUNT_ID = "default"
MASTER_CREDENTIAL_ID = "master"

_ID_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_ID_MAX = 64
_CHANNEL_PATTERN_RE = re.compile(r"^(?:\*|[a-z0-9][a-z0-9-]*\*?)$")
_CREDENTIAL_ID_RE = re.compile(r"^[a-z2-7]{12}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
# ssp_<12 base32 chars>_<base64url secret>. The secret can contain "_", so the
# fixed-length id is what separates the parts.
_TOKEN_RE = re.compile(r"^ssp_([a-z2-7]{12})_([A-Za-z0-9_-]{20,})$")
_BEARER = "Bearer "


class AccountsError(ValueError):
    """The accounts file is not valid."""


@dataclass(frozen=True)
class Limits:
    """Limits for each session of an account. None means no limit."""

    posts_per_minute: int | None = 30
    channels: int | None = 10
    pr_subscriptions: int | None = 0
    callbacks: int | None = 0


UNLIMITED = Limits(
    posts_per_minute=None, channels=None, pr_subscriptions=None, callbacks=None
)
_LIMIT_KEYS = frozenset(Limits.__dataclass_fields__)


@dataclass(frozen=True)
class Credential:
    id: str
    secret_sha256: str
    expires_at: float | None = None

    def expired(self, now: float) -> bool:
        return self.expires_at is not None and now >= self.expires_at


@dataclass(frozen=True)
class Account:
    id: str
    label: str = ""
    # Channel name patterns. None means every channel.
    channels: tuple[str, ...] | None = None
    permissions: frozenset[str] = DEFAULT_PERMISSIONS
    max_severity: str = "normal"
    limits: Limits = field(default_factory=Limits)
    credentials: tuple[Credential, ...] = ()

    def allows_channel(self, name: str) -> bool:
        if self.channels is None:
            return True
        for pattern in self.channels:
            if pattern == "*" or pattern == name:
                return True
            if pattern.endswith("*") and name.startswith(pattern[:-1]):
                return True
        return False

    def cap_severity(self, severity: str) -> str:
        """Lower ``severity`` to this account's maximum."""
        if _SEVERITY_ORDER.index(severity) > _SEVERITY_ORDER.index(self.max_severity):
            return self.max_severity
        return severity


def builtin_default(account_id: str) -> Account:
    """The default account when the accounts file does not define it."""
    return Account(
        id=account_id,
        label="default account",
        channels=None,
        permissions=PERMISSIONS,
        max_severity="high",
        limits=UNLIMITED,
    )


def validate_account_id(account_id: str) -> None:
    if not (2 <= len(account_id) <= _ID_MAX) or _ID_RE.match(account_id) is None:
        raise AccountsError(
            f"invalid account id {account_id!r}: must be 2-{_ID_MAX} chars, "
            "lowercase kebab-case"
        )


def default_account_id() -> str:
    """The default account's id: SUBSPACE_DEFAULT_ACCOUNT, else "default"."""
    value = os.environ.get("SUBSPACE_DEFAULT_ACCOUNT") or DEFAULT_ACCOUNT_ID
    validate_account_id(value)
    return value


def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def parse_token(token: str) -> tuple[str, str] | None:
    """Split an ``ssp_`` token into (credential id, secret), or None."""
    m = _TOKEN_RE.match(token)
    return (m.group(1), m.group(2)) if m else None


def new_token() -> tuple[str, str, str]:
    """Make a new credential: (token, credential id, sha256 of the secret)."""
    alphabet = "abcdefghijklmnopqrstuvwxyz234567"
    credential_id = "".join(secrets.choice(alphabet) for _ in range(12))
    secret = secrets.token_urlsafe(32)
    return f"ssp_{credential_id}_{secret}", credential_id, hash_secret(secret)


def _timestamp(value: object, where: str) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    raise AccountsError(f"{where}: expires_at must be a TOML date-time")


def _check_keys(table: dict, allowed: set[str], where: str) -> None:
    unknown = set(table) - allowed
    if unknown:
        raise AccountsError(f"{where}: unknown keys {sorted(unknown)}")


def _parse_limits(raw: object, where: str) -> Limits:
    if raw is None:
        return Limits()
    if not isinstance(raw, dict):
        raise AccountsError(f"{where}: limits must be a table")
    _check_keys(raw, set(_LIMIT_KEYS), f"{where}.limits")
    values: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise AccountsError(f"{where}.limits.{key} must be an integer >= 0")
        values[key] = value
    return Limits(**values)


def _parse_credential(raw: object, where: str) -> Credential:
    if not isinstance(raw, dict):
        raise AccountsError(f"{where} must be a table")
    _check_keys(raw, {"id", "secret_sha256", "expires_at"}, where)
    credential_id = raw.get("id")
    digest = raw.get("secret_sha256")
    if not isinstance(credential_id, str) or not _CREDENTIAL_ID_RE.match(credential_id):
        raise AccountsError(f"{where}: id must be 12 characters of a-z and 2-7")
    if not isinstance(digest, str) or not _SHA256_RE.match(digest):
        raise AccountsError(f"{where}: secret_sha256 must be 64 lowercase hex digits")
    expires = raw.get("expires_at")
    return Credential(
        id=credential_id,
        secret_sha256=digest,
        expires_at=None if expires is None else _timestamp(expires, where),
    )


def _parse_account(raw: object, index: int) -> Account:
    where = f"account[{index}]"
    if not isinstance(raw, dict):
        raise AccountsError(f"{where} must be a table")
    _check_keys(
        raw,
        {
            "id",
            "label",
            "channels",
            "permissions",
            "max_severity",
            "limits",
            "credential",
        },
        where,
    )
    account_id = raw.get("id")
    if not isinstance(account_id, str):
        raise AccountsError(f"{where}: id is required")
    validate_account_id(account_id)
    where = f"account {account_id!r}"
    label = raw.get("label", "")
    if not isinstance(label, str):
        raise AccountsError(f"{where}: label must be a string")
    channels: tuple[str, ...] | None = None
    if "channels" in raw:
        patterns = raw["channels"]
        if not isinstance(patterns, list) or not all(
            isinstance(p, str) and _CHANNEL_PATTERN_RE.match(p) for p in patterns
        ):
            raise AccountsError(
                f"{where}: channels must be a list of names or name prefixes "
                "ending in '*'"
            )
        channels = tuple(patterns)
    permissions = raw.get("permissions", sorted(DEFAULT_PERMISSIONS))
    if not isinstance(permissions, list) or not set(permissions) <= PERMISSIONS:
        raise AccountsError(
            f"{where}: permissions must be a list of {sorted(PERMISSIONS)}"
        )
    max_severity = raw.get("max_severity", "normal")
    if max_severity not in _SEVERITY_ORDER:
        raise AccountsError(
            f"{where}: max_severity must be one of {list(_SEVERITY_ORDER)}"
        )
    credentials = raw.get("credential", [])
    if not isinstance(credentials, list):
        raise AccountsError(f"{where}: credential must be an array of tables")
    return Account(
        id=account_id,
        label=label,
        channels=channels,
        permissions=frozenset(permissions),
        max_severity=max_severity,
        limits=_parse_limits(raw.get("limits"), where),
        credentials=tuple(
            _parse_credential(c, f"{where} credential[{i}]")
            for i, c in enumerate(credentials)
        ),
    )


def parse(text: str, default_id: str) -> dict[str, Account]:
    """Parse the accounts file. The default account is always in the result."""
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise AccountsError(f"not valid TOML: {exc}") from exc
    _check_keys(data, {"account"}, "accounts file")
    raw_accounts = data.get("account", [])
    if not isinstance(raw_accounts, list):
        raise AccountsError("account must be an array of tables ([[account]])")
    accounts: dict[str, Account] = {}
    credential_ids: set[str] = set()
    for i, raw in enumerate(raw_accounts):
        account = _parse_account(raw, i)
        if account.id in accounts:
            raise AccountsError(f"account {account.id!r} is defined twice")
        for credential in account.credentials:
            if credential.id in credential_ids:
                raise AccountsError(f"credential {credential.id!r} is defined twice")
            credential_ids.add(credential.id)
        accounts[account.id] = account
    if default_id not in accounts:
        accounts[default_id] = builtin_default(default_id)
    return accounts


def accounts_path(data_dir: Path) -> Path:
    """SUBSPACE_ACCOUNTS_FILE, else <data dir>/accounts.toml."""
    override = os.environ.get("SUBSPACE_ACCOUNTS_FILE")
    return Path(override) if override else Path(data_dir) / "accounts.toml"


class AccountStore:
    """The current accounts, reloaded when the file changes."""

    def __init__(self, path: Path, master_token: str, default_id: str) -> None:
        self.path: Path = path
        self.default_id: str = default_id
        self._master: str = master_token
        self._stamp: tuple[int, int] | None = None
        self.accounts: dict[str, Account] = {default_id: builtin_default(default_id)}
        self._credentials: dict[str, tuple[str, Credential]] = {}
        # Load at startup. A file that is not valid stops the server here.
        self._load(strict=True)

    def _file_stamp(self) -> tuple[int, int] | None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return None
        return (stat.st_mtime_ns, stat.st_size)

    def _load(self, *, strict: bool) -> None:
        stamp = self._file_stamp()
        try:
            text = self.path.read_text() if stamp is not None else ""
            accounts = parse(text, self.default_id)
        except (OSError, AccountsError) as exc:
            if strict:
                raise
            print(
                f"subspace: {self.path}: {exc}; keeping the previous accounts",
                file=sys.stderr,
            )
            self._stamp = stamp
            return
        self.accounts = accounts
        self._credentials = {
            c.id: (a.id, c) for a in accounts.values() for c in a.credentials
        }
        self._stamp = stamp

    def reload_if_changed(self) -> bool:
        """Reload the file if it changed. Returns True if it changed."""
        if self._file_stamp() == self._stamp:
            return False
        self._load(strict=False)
        return True

    def get(self, account_id: str) -> Account | None:
        return self.accounts.get(account_id)

    def authenticate(self, header: str, now: float) -> tuple[str, str] | None:
        """Check an Authorization header value. Returns (account id, credential
        id), or None if the token is not valid."""
        if hmac.compare_digest(header.encode(), (_BEARER + self._master).encode()):
            return self.default_id, MASTER_CREDENTIAL_ID
        if not header.startswith(_BEARER):
            return None
        parsed = parse_token(header[len(_BEARER) :])
        if parsed is None:
            return None
        credential_id, secret = parsed
        entry = self._credentials.get(credential_id)
        if entry is None:
            return None
        account_id, credential = entry
        if not hmac.compare_digest(credential.secret_sha256, hash_secret(secret)):
            return None
        if credential.expired(now):
            return None
        return account_id, credential_id

    def credential_valid(self, account_id: str, credential_id: str, now: float) -> bool:
        """True if the credential still gives access to the account."""
        if credential_id == MASTER_CREDENTIAL_ID:
            return account_id == self.default_id
        entry = self._credentials.get(credential_id)
        if entry is None or entry[0] != account_id:
            return False
        return not entry[1].expired(now)
