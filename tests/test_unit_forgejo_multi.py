# vim: filetype=python
"""Unit tests for MULTI-INSTANCE Forgejo PR notifications (docs/specs/2026-07-11).

Covers the additive multi-instance seam, keeping the default instance byte-identical:

  - storage_key alias dimension (default unprefixed, named prefixed)
  - server config parsing of FORGEJO_<ALIAS>_* pairs + the canonicalization config-error
  - load_trackers instance round-trip + the base_url mismatch-skip guard
  - the server echoes `instance` back in its replies
  - the auth-error message naming the right per-instance token env var
  - per-instance client selection (default vs named vs unknown)

The server is imported by path (its filename is a hyphenated script), the same idiom as
test_unit_pr / test_unit_core."""

import json

import anyio
import pytest
from _harness import load_server

import forgejo_client as fc
import pr_monitor as pm


# --------------------------------------------------------------------------- #
# module loaders (hyphenated script filenames)
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    return load_server(tmp_path_factory.mktemp("server"))


# --------------------------------------------------------------------------- #
# storage_key alias dimension
# --------------------------------------------------------------------------- #


class TestStorageKeyAlias:
    def test_default_forgejo_unprefixed_by_alias(self):
        # Default instance (no alias) is byte-identical to v1: forgejo:owner/repo#n.
        assert pm.storage_key("forgejo", "o", "r", 1) == "forgejo:o/r#1"
        assert pm.storage_key("forgejo", "o", "r", 1, "") == "forgejo:o/r#1"
        assert pm.storage_key("forgejo", "o", "r", 1, None) == "forgejo:o/r#1"

    def test_named_instance_folds_alias(self):
        assert (
            pm.storage_key("forgejo", "o", "r", 1, "external")
            == "forgejo:external:o/r#1"
        )

    def test_github_ignores_alias(self):
        # github has no multi-instance dimension in v1; the alias is ignored and the key
        # stays unprefixed (byte-identical).
        assert pm.storage_key("github", "o", "r", 1) == "o/r#1"
        assert pm.storage_key("github", "o", "r", 1, "external") == "o/r#1"

    def test_named_and_default_and_github_all_distinct(self):
        keys = {
            pm.storage_key("github", "o", "r", 1),
            pm.storage_key("forgejo", "o", "r", 1),
            pm.storage_key("forgejo", "o", "r", 1, "external"),
        }
        assert len(keys) == 3  # one owner/repo#n cannot collide across the three


# --------------------------------------------------------------------------- #
# server config parsing + canonicalization
# --------------------------------------------------------------------------- #


class TestConfigParsing:
    def test_parses_aliased_pairs(self, server):
        env = {
            "FORGEJO_EXTERNAL_API_URL": "https://ext.example",
            "FORGEJO_EXTERNAL_TOKEN": "e",
            "FORGEJO_SELF_HOSTED_API_URL": "https://self.example/",  # underscore -> kebab
            "FORGEJO_SELF_HOSTED_TOKEN": "s",
        }
        inst = server._build_forgejo_instances(
            env, default_url="https://fj.example/api/v1"
        )
        assert sorted(inst) == ["external", "self-hosted"]
        assert inst["external"].base_url == "https://ext.example/api/v1"
        assert inst["self-hosted"].base_url == "https://self.example/api/v1"
        # token threaded onto the right client (Gitea `token <PAT>` scheme)
        assert inst["external"]._headers()["Authorization"] == "token e"

    def test_bare_forgejo_api_url_is_not_an_instance(self, server):
        # The unaliased FORGEJO_API_URL is the DEFAULT instance, not a named one — it must
        # never be parsed into FJ_INSTANCES (no alias segment between prefix and suffix).
        env = {"FORGEJO_API_URL": "https://fj.example", "FORGEJO_TOKEN": "t"}
        assert server._build_forgejo_instances(env, default_url="") == {}

    def test_canonicalization_refuses_alias_equal_to_default(self, server, capsys):
        env = {
            "FORGEJO_DUP_API_URL": "https://fj.example/",  # normalizes to the default URL
            "FORGEJO_DUP_TOKEN": "d",
        }
        default_url = fc.ForgejoClient(base_url="https://fj.example").base_url
        inst = server._build_forgejo_instances(env, default_url=default_url)
        assert "dup" not in inst  # refused: one PR must not grow two trackers
        err = capsys.readouterr().err
        assert "CONFIG ERROR" in err and "dup" in err

    def test_canonicalization_refuses_second_alias_same_url(self, server, capsys):
        # Two NAMED aliases at the same normalized URL must not both build — one PR under
        # both = two trackers double-polling. The first (sorted) wins; the second is
        # refused (default unconfigured here, so this is purely alias-vs-alias). Also
        # exercises URL normalization: bare vs /api/v1 form collapse to one URL.
        env = {
            "FORGEJO_ALPHA_API_URL": "https://same.example/",
            "FORGEJO_ALPHA_TOKEN": "a",
            "FORGEJO_BETA_API_URL": "https://same.example/api/v1",
            "FORGEJO_BETA_TOKEN": "b",
        }
        inst = server._build_forgejo_instances(env, default_url="")
        assert sorted(inst) == ["alpha"]  # beta refused — alpha took the URL first
        err = capsys.readouterr().err
        assert "CONFIG ERROR" in err and "beta" in err and "alpha" in err

    def test_token_env_naming(self, server):
        assert (
            server._forgejo_token_env("") == "FORGEJO_TOKEN"
        )  # default (v1-identical)
        assert server._forgejo_token_env("external") == "FORGEJO_EXTERNAL_TOKEN"
        assert server._forgejo_token_env("self-hosted") == "FORGEJO_SELF_HOSTED_TOKEN"

    def test_missing_url_value_ignored(self, server):
        env = {"FORGEJO_EMPTY_API_URL": "", "FORGEJO_EMPTY_TOKEN": "x"}
        assert server._build_forgejo_instances(env, default_url="") == {}


# --------------------------------------------------------------------------- #
# per-instance client selection + config-error resolution
# --------------------------------------------------------------------------- #


class TestClientSelection:
    def _wire(self, server):
        default = fc.ForgejoClient(base_url="https://fj.example", token="t")
        external = fc.ForgejoClient(base_url="https://ext.example", token="e")
        server.FJ = default
        server.FJ_INSTANCES = {"external": external}
        return default, external

    def test_default_and_named_selection(self, server):
        default, external = self._wire(server)
        try:
            assert server._pr_client("forgejo", "") is default
            assert server._pr_client("forgejo", "external") is external
            assert server._pr_client("forgejo", "nope") is None
            assert server._forgejo_aliases() == ["external"]
        finally:
            server.FJ = None
            server.FJ_INSTANCES = {}

    def test_config_error_messages(self, server):
        self._wire(server)
        try:
            assert server._forgejo_config_error("github", "") is None
            assert server._forgejo_config_error("forgejo", "") is None
            assert server._forgejo_config_error("forgejo", "external") is None
            err = server._forgejo_config_error("forgejo", "ghost")
            assert "unknown Forgejo instance 'ghost'" in err
            assert "external" in err  # lists configured aliases (names only)
            assert "https://" not in err  # never leaks URLs
        finally:
            server.FJ = None
            server.FJ_INSTANCES = {}

    def test_default_unconfigured_message_is_v1(self, server):
        server.FJ = None
        server.FJ_INSTANCES = {}
        err = server._forgejo_config_error("forgejo", "")
        assert "set FORGEJO_API_URL / FORGEJO_TOKEN" in err

    def test_pr_clients_map_covers_default_and_named(self, server):
        default, external = self._wire(server)
        server.GH = object()
        try:
            clients = server._pr_clients()
            assert clients["github"] is server.GH
            assert clients["forgejo"] is default
            assert clients[("forgejo", "external")] is external
        finally:
            server.FJ = None
            server.FJ_INSTANCES = {}
            server.GH = None


# --------------------------------------------------------------------------- #
# load_trackers: instance round-trip + base_url mismatch guard
# --------------------------------------------------------------------------- #


class _Client:
    def __init__(self, base_url):
        self.base_url = base_url


class TestLoadTrackersMultiInstance:
    def _seed(self, tmp_path, monkeypatch, *, instance, base_url):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        t = pm.PRTracker(
            "o", "r", 1, None, provider="forgejo", base_url=base_url, instance=instance
        )
        t.snapshot = {"timeline": {}, "labels": [], "state": "open"}
        pm.save_state(t)
        return t

    def test_instance_roundtrips_and_selects_named_client(self, tmp_path, monkeypatch):
        url = "https://ext.example/api/v1"
        self._seed(tmp_path, monkeypatch, instance="external", base_url=url)
        # storage dir is alias-tagged
        assert pm._tracker_dir("forgejo:external:o/r#1").exists()
        client = _Client(url)
        loaded = {
            t.storage_key: t
            for t in pm.load_trackers({("forgejo", "external"): client})
        }
        t = loaded["forgejo:external:o/r#1"]
        assert t.instance == "external"
        assert t.base_url == url
        assert t.client is client
        assert t.key == "o/r#1"  # display ref stays bare

    def test_persisted_instance_field_present(self, tmp_path, monkeypatch):
        self._seed(
            tmp_path, monkeypatch, instance="external", base_url="https://ext/api/v1"
        )
        state = json.loads(
            (pm._tracker_dir("forgejo:external:o/r#1") / "state.json").read_text()
        )
        assert state["instance"] == "external"

    def test_default_instance_absent_field_loads_as_default(
        self, tmp_path, monkeypatch
    ):
        # A v1 default forgejo tracker on disk has NO instance field; it must load as the
        # default instance and key on the bare provider string.
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        directory = pm._tracker_dir("forgejo:o/r#1")
        directory.mkdir(parents=True)
        (directory / "state.json").write_text(
            json.dumps(
                {
                    "owner": "o",
                    "repo": "r",
                    "number": 1,
                    "provider": "forgejo",
                    "base_url": "https://fj/api/v1",
                    "snapshot": {"timeline": {}, "state": "open"},
                }
            )
        )
        client = _Client("https://fj/api/v1")
        loaded = {t.storage_key: t for t in pm.load_trackers({"forgejo": client})}
        t = loaded["forgejo:o/r#1"]
        assert t.instance == ""
        assert t.client is client

    def test_base_url_mismatch_skips_tracker(self, tmp_path, monkeypatch, capsys):
        # The alias was repointed at a new URL since the tracker was written: the persisted
        # base_url disagrees with the configured client's -> skip (don't poll the wrong
        # instance).
        self._seed(
            tmp_path, monkeypatch, instance="external", base_url="https://OLD/api/v1"
        )
        client = _Client("https://NEW/api/v1")  # repointed
        loaded = pm.load_trackers({("forgejo", "external"): client})
        assert loaded == []  # skipped
        assert "disagrees" in capsys.readouterr().err

    def test_base_url_match_loads(self, tmp_path, monkeypatch):
        url = "https://same/api/v1"
        self._seed(tmp_path, monkeypatch, instance="external", base_url=url)
        loaded = pm.load_trackers({("forgejo", "external"): _Client(url)})
        assert len(loaded) == 1

    def test_unconfigured_named_instance_skipped(self, tmp_path, monkeypatch, capsys):
        # A forgejo tracker for an alias no longer configured is skipped, not crashed.
        self._seed(
            tmp_path, monkeypatch, instance="external", base_url="https://ext/api/v1"
        )
        loaded = pm.load_trackers({"forgejo": _Client("https://fj/api/v1")})
        assert loaded == []
        assert "no client" in capsys.readouterr().err

    def test_default_forgejo_unaffected_by_named_on_disk(self, tmp_path, monkeypatch):
        # A default and a named tracker for the same owner/repo#n coexist in distinct dirs.
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        default = pm.PRTracker(
            "o", "r", 1, None, provider="forgejo", base_url="https://fj/api/v1"
        )
        named = pm.PRTracker(
            "o",
            "r",
            1,
            None,
            provider="forgejo",
            base_url="https://ext/api/v1",
            instance="external",
        )
        for t in (default, named):
            t.snapshot = {"timeline": {}, "state": "open"}
            pm.save_state(t)
        loaded = {
            t.storage_key: t
            for t in pm.load_trackers(
                {
                    "forgejo": _Client("https://fj/api/v1"),
                    ("forgejo", "external"): _Client("https://ext/api/v1"),
                }
            )
        }
        assert loaded["forgejo:o/r#1"].instance == ""
        assert loaded["forgejo:external:o/r#1"].instance == "external"


# --------------------------------------------------------------------------- #
# server handlers: storage-key derivation + instance echo
# --------------------------------------------------------------------------- #


class _FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, data):
        self.sent.append(json.loads(data))


class _FakeForgejoClient:
    """A ForgejoClient stand-in whose fetch_pr returns a fixed open snapshot, so a
    subscribe handler can baseline without a network/subprocess."""

    def __init__(self, base_url):
        self.base_url = base_url
        self.configured = True

    def should_throttle(self, threshold=50):
        return None

    async def fetch_pr(self, owner, repo, number):
        return {
            "pr": {
                "number": number,
                "title": "T",
                "html_url": "u",
                "state": "open",
                "merged": False,
                "mergeable": True,
                "head": {"sha": "a" * 40},
                "labels": [],
                "requested_reviewers": [],
            },
            "reviews": [],
            "review_comments": [],
            "issue_comments": [],
            "statuses": [],
        }


class TestServerHandlersInstance:
    def _reset(self, server):
        server.TRACKERS.clear()
        server.CONNECTIONS.clear()

    def test_subscribe_named_instance_echoes_and_keys_by_alias(
        self, server, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        self._reset(server)
        external = _FakeForgejoClient("https://ext.example/api/v1")
        server.FJ = None
        server.FJ_INSTANCES = {"external": external}

        async def scenario():
            ws = _FakeWS()
            conn = server.Connection(ws, "default", "master")
            conn.session_key = "sidA"
            server.CONNECTIONS["sidA"] = [conn]
            await server._handle_subscribe(
                ws,
                conn,
                {
                    "session_id": "sidA",
                    "owner": "o",
                    "repo": "r",
                    "number": 1,
                    "instance": "external",
                    "req_id": 7,
                },
                provider="forgejo",
            )
            return ws

        try:
            ws = anyio.run(scenario)
            tracker_keys = set(server.TRACKERS)  # capture before teardown clears it
        finally:
            for t in list(server.TRACKERS.values()):
                if t.task is not None:
                    t.task.cancel()
            self._reset(server)
            server.FJ_INSTANCES = {}

        reply = ws.sent[-1]
        assert reply["type"] == server.proto.SUBSCRIBED
        assert reply["instance"] == "external"  # echoed back
        assert reply["pr"] == "o/r#1"  # display ref stays bare
        # tracker keyed by the alias-tagged storage key
        assert "forgejo:external:o/r#1" in tracker_keys

    def test_subscribe_unknown_instance_errors_with_aliases(
        self, server, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        self._reset(server)
        server.FJ = _FakeForgejoClient("https://fj.example/api/v1")
        server.FJ_INSTANCES = {"external": _FakeForgejoClient("https://ext/api/v1")}

        async def scenario():
            ws = _FakeWS()
            conn = server.Connection(ws, "default", "master")
            conn.session_key = "sidA"
            await server._handle_subscribe(
                ws,
                conn,
                {
                    "session_id": "sidA",
                    "owner": "o",
                    "repo": "r",
                    "number": 1,
                    "instance": "ghost",
                    "req_id": 3,
                },
                provider="forgejo",
            )
            return ws

        try:
            ws = anyio.run(scenario)
        finally:
            server.FJ = None
            server.FJ_INSTANCES = {}
            self._reset(server)

        reply = ws.sent[-1]
        assert reply["type"] == server.proto.ERROR
        assert "unknown Forgejo instance 'ghost'" in reply["error"]
        assert "external" in reply["error"]
        assert not server.TRACKERS  # no tracker created for an unknown instance

    def test_list_carries_instance_alias(self, server, tmp_path, monkeypatch):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        self._reset(server)
        # Two forgejo trackers for the same owner/repo#n: one default, one named.
        default = pm.PRTracker(
            "o", "r", 1, None, provider="forgejo", base_url="https://fj/api/v1"
        )
        named = pm.PRTracker(
            "o",
            "r",
            1,
            None,
            provider="forgejo",
            base_url="https://ext/api/v1",
            instance="external",
        )
        for t in (default, named):
            t.snapshot = {"timeline": {}, "state": "open"}
            t.subscribers.add("sidA")
            t.acked["sidA"] = set()
            server.TRACKERS[t.storage_key] = t

        async def scenario():
            ws = _FakeWS()
            conn = server.Connection(ws, "default", "master")
            conn.session_key = "sidA"
            await server._handle_list_pr_subscriptions(
                ws, conn, {"session_id": "sidA", "req_id": 9}, provider="forgejo"
            )
            return ws

        try:
            ws = anyio.run(scenario)
        finally:
            self._reset(server)

        items = ws.sent[-1]["items"]
        # Default items are v1-shaped (no "instance" key); named items carry the alias.
        default_item = next(it for it in items if it.get("instance", "") == "")
        named_item = next(it for it in items if it.get("instance") == "external")
        assert default_item["pr"] == "o/r#1" and "instance" not in default_item
        assert named_item["pr"] == "o/r#1" and named_item["instance"] == "external"


# --------------------------------------------------------------------------- #
# auth-error message names the right per-instance token env var
# --------------------------------------------------------------------------- #


class _AuthFailClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.configured = True

    def should_throttle(self, threshold=50):
        return None

    async def fetch_pr(self, owner, repo, number):
        import pr_errors

        raise pr_errors.PRAuthError("unauthorized (401)")


class TestAuthErrorNaming:
    def _run_auth(self, server, tmp_path, monkeypatch, *, instance):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("SUBSPACE_PR_POLL_SECONDS", "0.2")
        server.TRACKERS.clear()
        client = _AuthFailClient("https://x/api/v1")
        t = pm.PRTracker(
            "o",
            "r",
            1,
            client,
            provider="forgejo",
            base_url="https://x/api/v1",
            instance=instance,
        )
        t.snapshot = {"timeline": {}, "state": "open", "merged": False}
        t.subscribers.add("sidA")
        t.acked["sidA"] = set()

        async def scenario():
            import asyncio

            conn = server.Connection(_FakeWS(), "default", "master")
            conn.session_key = "sidA"
            server.CONNECTIONS["sidA"] = [conn]
            loop_task = asyncio.ensure_future(server._tracker_loop(t))
            for _ in range(200):
                if t.auth_notified:
                    break
                await asyncio.sleep(0.01)
            loop_task.cancel()
            try:
                await loop_task
            except asyncio.CancelledError:
                pass

        anyio.run(scenario)
        server.CONNECTIONS.clear()
        server.TRACKERS.clear()
        # The emitted auth event is on the tracker.
        return next(e for e in t.events if e["type"] == "pr_auth_error")

    def test_default_instance_names_forgejo_token(self, server, tmp_path, monkeypatch):
        event = self._run_auth(server, tmp_path, monkeypatch, instance="")
        assert "FORGEJO_TOKEN" in event["content"]
        assert "FORGEJO_EXTERNAL_TOKEN" not in event["content"]

    def test_named_instance_names_aliased_token(self, server, tmp_path, monkeypatch):
        event = self._run_auth(server, tmp_path, monkeypatch, instance="external")
        assert "FORGEJO_EXTERNAL_TOKEN" in event["content"]
        assert "external" in event["content"]  # alias named in the label
