# vim: filetype=python
"""Unit tests for accounts: the accounts file, tokens, authentication, reload,
channel patterns, and the severity cap."""

import os
import time
from pathlib import Path

import pytest

import accounts as a

MASTER = "master-secret"


def _credential_block(token_parts: tuple[str, str, str], expires: str = "") -> str:
    _token, credential_id, digest = token_parts
    extra = f"\nexpires_at = {expires}" if expires else ""
    return (
        f'[[account.credential]]\nid = "{credential_id}"\n'
        f'secret_sha256 = "{digest}"{extra}\n'
    )


def _write(path: Path, text: str) -> None:
    path.write_text(text)
    # Make sure the reload check sees a change even within one mtime tick.
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))


def test_new_token_parses() -> None:
    token, credential_id, digest = a.new_token()
    assert token.startswith(f"ssp_{credential_id}_")
    parsed = a.parse_token(token)
    assert parsed is not None and parsed[0] == credential_id
    assert a.hash_secret(parsed[1]) == digest


@pytest.mark.parametrize(
    "bad", ["", "ssp_short_secretsecretsecretsecret", "xyz_abcdefghijkl_" + "s" * 30]
)
def test_parse_token_rejects(bad: str) -> None:
    assert a.parse_token(bad) is None


def test_no_file_gives_builtin_default(tmp_path: Path) -> None:
    store = a.AccountStore(tmp_path / "accounts.toml", MASTER, "default")
    default = store.get("default")
    assert default is not None
    assert default.permissions == a.PERMISSIONS
    assert default.channels is None
    assert default.max_severity == "high"
    assert default.limits == a.UNLIMITED
    assert store.authenticate(f"Bearer {MASTER}", 0.0) == ("default", "master")
    assert store.authenticate("Bearer wrong", 0.0) is None
    assert store.authenticate("", 0.0) is None


def test_file_account_defaults_and_auth(tmp_path: Path) -> None:
    parts = a.new_token()
    path = tmp_path / "accounts.toml"
    _write(path, '[[account]]\nid = "ci"\n' + _credential_block(parts))
    store = a.AccountStore(path, MASTER, "default")
    ci = store.get("ci")
    assert ci is not None
    assert ci.permissions == {"messaging"}
    assert ci.max_severity == "normal"
    assert ci.limits == a.Limits()
    assert ci.channels is None
    assert store.authenticate(f"Bearer {parts[0]}", 0.0) == ("ci", parts[1])
    assert store.authenticate(f"Bearer {parts[0]}x", 0.0) is None
    # The default account is still there, from the built-in definition.
    assert store.get("default").permissions == a.PERMISSIONS


def test_file_can_redefine_default_account(tmp_path: Path) -> None:
    path = tmp_path / "accounts.toml"
    _write(path, '[[account]]\nid = "home"\npermissions = ["messaging", "pr"]\n')
    store = a.AccountStore(path, MASTER, "home")
    home = store.get("home")
    assert home.permissions == {"messaging", "pr"}
    assert store.authenticate(f"Bearer {MASTER}", 0.0) == ("home", "master")


def test_expired_credential(tmp_path: Path) -> None:
    parts = a.new_token()
    path = tmp_path / "accounts.toml"
    _write(
        path,
        '[[account]]\nid = "ci"\n' + _credential_block(parts, "2020-01-01T00:00:00Z"),
    )
    store = a.AccountStore(path, MASTER, "default")
    assert store.authenticate(f"Bearer {parts[0]}", time.time()) is None
    assert not store.credential_valid("ci", parts[1], time.time())


def test_reload_revokes(tmp_path: Path) -> None:
    parts = a.new_token()
    path = tmp_path / "accounts.toml"
    _write(path, '[[account]]\nid = "ci"\n' + _credential_block(parts))
    store = a.AccountStore(path, MASTER, "default")
    assert store.credential_valid("ci", parts[1], 0.0)
    assert not store.reload_if_changed()

    _write(path, '[[account]]\nid = "ci"\n')  # credential removed
    assert store.reload_if_changed()
    assert not store.credential_valid("ci", parts[1], 0.0)
    assert store.credential_valid("default", "master", 0.0)


def test_broken_file_at_reload_fails_closed(tmp_path: Path) -> None:
    parts = a.new_token()
    path = tmp_path / "accounts.toml"
    _write(path, '[[account]]\nid = "ci"\n' + _credential_block(parts))
    store = a.AccountStore(path, MASTER, "default")
    _write(path, "not = [valid")
    assert store.reload_if_changed()
    assert store.get("ci") is None
    assert not store.credential_valid("ci", parts[1], 0.0)
    assert store.authenticate(f"Bearer {parts[0]}", 0.0) is None
    assert store.authenticate(f"Bearer {MASTER}", 0.0) == ("default", "master")
    # Fixing the file brings the account back.
    _write(path, '[[account]]\nid = "ci"\n' + _credential_block(parts))
    assert store.reload_if_changed()
    assert store.credential_valid("ci", parts[1], 0.0)


def test_strict_load_at_startup(tmp_path: Path) -> None:
    path = tmp_path / "accounts.toml"
    _write(path, '[[account]]\nid = "ci"\npermissions = ["root"]\n')
    with pytest.raises(a.AccountsError):
        a.AccountStore(path, MASTER, "default")


@pytest.mark.parametrize(
    "text",
    [
        '[[account]]\nid = "Bad"\n',
        '[[account]]\nid = "ci"\nunknown = 1\n',
        '[[account]]\nid = "ci"\n[[account]]\nid = "ci"\n',
        '[[account]]\nid = "ci"\nchannels = ["a*b"]\n',
        '[[account]]\nid = "ci"\nmax_severity = "loud"\n',
        '[[account]]\nid = "ci"\n[account.limits]\nchannels = -1\n',
        '[[account]]\nid = "ci"\n[account.limits]\nbogus = 1\n',
        '[[account]]\nid = "ci"\n[[account.credential]]\nid = "x"\nsecret_sha256 = "y"\n',
    ],
)
def test_invalid_files(text: str) -> None:
    with pytest.raises(a.AccountsError):
        a.parse(text, "default")


def test_duplicate_credential_ids_across_accounts() -> None:
    _token, credential_id, digest = a.new_token()
    block = (
        f'[[account.credential]]\nid = "{credential_id}"\nsecret_sha256 = "{digest}"\n'
    )
    text = '[[account]]\nid = "one"\n' + block + '[[account]]\nid = "two"\n' + block
    with pytest.raises(a.AccountsError):
        a.parse(text, "default")


def test_channel_patterns() -> None:
    account = a.Account(id="ci", channels=("ops", "ci-*"))
    assert account.allows_channel("ops")
    assert account.allows_channel("ci-builds")
    assert not account.allows_channel("ci")
    assert not account.allows_channel("ops-2")
    assert a.Account(id="x", channels=("*",)).allows_channel("anything")
    assert a.Account(id="x").allows_channel("anything")
    assert not a.Account(id="x", channels=()).allows_channel("anything")


def test_cap_severity() -> None:
    account = a.Account(id="ci", max_severity="normal")
    assert account.cap_severity("high") == "normal"
    assert account.cap_severity("normal") == "normal"
    assert account.cap_severity("low") == "low"


def test_limits_parse() -> None:
    accounts = a.parse(
        '[[account]]\nid = "ci"\n[account.limits]\nposts_per_minute = 5\ncallbacks = 2\n',
        "default",
    )
    limits = accounts["ci"].limits
    assert limits.posts_per_minute == 5 and limits.callbacks == 2
    assert limits.channels == 10 and limits.pr_subscriptions == 0
    assert limits.sessions == 20


def test_default_account_id_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUBSPACE_DEFAULT_ACCOUNT", raising=False)
    assert a.default_account_id() == "default"
    monkeypatch.setenv("SUBSPACE_DEFAULT_ACCOUNT", "home")
    assert a.default_account_id() == "home"
    monkeypatch.setenv("SUBSPACE_DEFAULT_ACCOUNT", "Not Valid")
    with pytest.raises(a.AccountsError):
        a.default_account_id()


@pytest.mark.parametrize(
    "text",
    [
        '[[account]]\nid = "ci"\npermissions = [["messaging"]]\n',
        '[[account]]\nid = "ci"\nmax_severity = ["high"]\n',
    ],
)
def test_wrong_types_are_accounts_errors(text: str) -> None:
    with pytest.raises(a.AccountsError):
        a.parse(text, "default")


def test_webhook_parse() -> None:
    accounts = a.parse(
        '[[account]]\nid = "bots"\n'
        '[[account.webhook]]\nhandle = "runner"\n'
        'url = "https://hooks.example.com/run/1"\nkey = "s3cret"\n'
        "grace_seconds = 5\n"
        '[[account.webhook]]\nhandle = "local"\nurl = "http://127.0.0.1:9/hook"\n',
        "default",
    )
    account = accounts["bots"]
    runner = account.webhook("runner")
    assert runner is not None
    assert runner.url == "https://hooks.example.com/run/1"
    assert runner.key == "s3cret" and "s3cret" not in repr(runner)
    assert runner.grace_seconds == 5.0
    assert runner.cooldown_seconds == a.DEFAULT_WAKE_COOLDOWN_SECONDS
    local = account.webhook("local")
    assert local is not None and local.key == ""
    assert account.webhook("other") is None


@pytest.mark.parametrize(
    "webhook",
    [
        'handle = "runner"\nurl = "http://hooks.example.com/run"\n',
        'handle = "runner"\nurl = "ftp://hooks.example.com/run"\n',
        'handle = "runner"\nurl = "not a url"\n',
        'handle = "Runner"\nurl = "https://hooks.example.com/run"\n',
        'handle = "runner"\nurl = "https://hooks.example.com/run"\nkey = 1\n',
        'handle = "runner"\nurl = "https://hooks.example.com/run"\nkey = "k\u00e9y"\n',
        'handle = "runner"\nurl = "https://hooks.example.com/run"\nkey = "a b"\n',
        'handle = "runner"\nurl = "https://hooks.example.com/run"\n'
        "cooldown_seconds = -1\n",
        'handle = "runner"\nurl = "https://hooks.example.com/run"\nretries = 3\n',
    ],
)
def test_invalid_webhooks(webhook: str) -> None:
    with pytest.raises(a.AccountsError):
        a.parse(f'[[account]]\nid = "bots"\n[[account.webhook]]\n{webhook}', "default")


def test_one_webhook_per_handle() -> None:
    block = '[[account.webhook]]\nhandle = "runner"\nurl = "https://h.example.com/"\n'
    with pytest.raises(a.AccountsError):
        a.parse('[[account]]\nid = "bots"\n' + block + block, "default")


def test_account_webhook_and_precedence() -> None:
    accounts = a.parse(
        '[[account]]\nid = "bots"\n'
        '[[account.webhook]]\nurl = "https://h.example.com/account"\n'
        '[[account.webhook]]\nhandle = "runner"\nurl = "https://h.example.com/runner"\n',
        "default",
    )
    account = accounts["bots"]
    runner = account.webhook("runner")
    assert runner is not None and runner.url == "https://h.example.com/runner"
    other = account.webhook("other")
    assert other is not None and other.url == "https://h.example.com/account"
    assert other.handle == ""
    # A session with no handle also matches the account's webhook.
    assert account.webhook("") is other


def test_one_account_webhook() -> None:
    block = '[[account.webhook]]\nurl = "https://h.example.com/"\n'
    with pytest.raises(a.AccountsError, match="no handle"):
        a.parse('[[account]]\nid = "bots"\n' + block + block, "default")


def test_empty_handle_is_not_the_account_webhook() -> None:
    with pytest.raises(a.AccountsError, match="handle"):
        a.parse(
            '[[account]]\nid = "bots"\n[[account.webhook]]\nhandle = ""\n'
            'url = "https://h.example.com/"\n',
            "default",
        )


def test_webhook_key_file(tmp_path: Path) -> None:
    key_file = tmp_path / "runner.key"
    accounts = a.parse(
        '[[account]]\nid = "bots"\n[[account.webhook]]\nhandle = "runner"\n'
        f'url = "https://h.example.com/"\nkey_file = "{key_file}"\n',
        "default",
    )
    webhook = accounts["bots"].webhook("runner")
    assert webhook is not None
    with pytest.raises(a.AccountsError) as missing:
        webhook.bearer_key()
    assert str(key_file) in str(missing.value)
    key_file.write_text("first-key\n")
    assert webhook.bearer_key() == "first-key"
    # Read for each wake: a new key takes effect without a reload.
    key_file.write_text("second-key")
    assert webhook.bearer_key() == "second-key"
    key_file.write_text("has a space")
    with pytest.raises(a.AccountsError) as invalid:
        webhook.bearer_key()
    assert "space" not in str(invalid.value).replace("no spaces", "")


@pytest.mark.parametrize(
    "webhook",
    [
        'key_file = "relative/path"\n',
        'key = "k"\nkey_file = "/etc/subspace/k"\n',
        "key_file = 1\n",
    ],
)
def test_invalid_webhook_key_files(webhook: str) -> None:
    with pytest.raises(a.AccountsError):
        a.parse(
            '[[account]]\nid = "bots"\n[[account.webhook]]\nhandle = "runner"\n'
            f'url = "https://h.example.com/"\n{webhook}',
            "default",
        )
