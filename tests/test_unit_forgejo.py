# vim: filetype=python
"""Unit tests for Forgejo/Gitea PR monitoring: the REST snapshot mapping, the shared
diff over forgejo snapshots, the ForgejoClient's pagination / per-review comment
fan-out / HTTP error classification (driven through httpx.MockTransport, no network),
and the split on-disk storage for a provider-prefixed forgejo tracker.

Mirrors test_unit_pr.py's patterns: a `fj()` builder that maps a raw Gitea fetch into a
snapshot, `_types`/`_only` diff helpers, and a MockTransport fed by the same FakeForgejo
the e2e test uses (so the production client and the fake agree on the REST protocol)."""

import time

import anyio
import httpx
import pytest

import _harness as h
import forgejo_client as fc
import pr_monitor as pm


# --------------------------------------------------------------------------- #
# raw Gitea fetch -> snapshot builders (parallel to test_unit_pr's gql())
# --------------------------------------------------------------------------- #


def raw(**override) -> dict:
    """A raw forgejo_client.fetch_pr() dict (pr + sub-resource lists), with defaults."""
    data = {
        "pr": {
            "number": 1,
            "title": "T",
            "html_url": "https://fj/o/r/pulls/1",
            "state": "open",
            "merged": False,
            "merged_by": None,
            "mergeable": True,
            "draft": False,
            "head": {"sha": "a" * 40},
            "labels": [],
            "requested_reviewers": [],
        },
        "reviews": [],
        "review_comments": [],
        "issue_comments": [],
        "statuses": [],
    }
    data.update(override)
    return data


def fj(**override) -> dict:
    """A raw Gitea fetch -> the transport-agnostic snapshot diff() consumes."""
    return pm.snapshot_from_forgejo(raw(**override))


def _pr(**fields) -> dict:
    base = raw()["pr"]
    base.update(fields)
    return base


def _types(old, new):
    return [e["type"] for e in pm.diff(old, new, "o/r#1")]


def _only(old, new, kind):
    events = [e for e in pm.diff(old, new, "o/r#1") if e["type"] == kind]
    assert events, (kind, _types(old, new))
    return events


# --------------------------------------------------------------------------- #
# snapshot_from_forgejo mapping correctness
# --------------------------------------------------------------------------- #


class TestSnapshotMapping:
    def test_core_fields_and_empty_keys_present(self):
        snap = fj(pr=_pr(labels=[{"name": "bug"}, {"name": "urgent"}]))
        assert snap["state"] == "open"
        assert snap["mergeable_state"] == "clean"  # mergeable True -> clean
        assert snap["head_sha"] == "a" * 40
        assert snap["labels"] == ["bug", "urgent"]  # sorted names
        assert snap["draft"] is False
        # check_runs and timeline are always present-but-empty (diff .get()s them; the
        # timeline key must exist so load_trackers doesn't treat the snapshot as stale).
        assert snap["check_runs"] == {} and snap["timeline"] == {}
        assert pm.diff(None, snap, "o/r#1") == []  # baseline never replays

    def test_mergeable_tri_state(self):
        assert fj(pr=_pr(mergeable=True))["mergeable_state"] == "clean"
        assert fj(pr=_pr(mergeable=False))["mergeable_state"] == "dirty"
        assert fj(pr=_pr(mergeable=None))["mergeable_state"] == "unknown"

    def test_merged_by_login(self):
        snap = fj(pr=_pr(merged=True, merged_by={"login": "carol"}))
        assert snap["merged"] is True and snap["merged_by"] == "carol"

    def test_requested_reviewers_sorted(self):
        snap = fj(pr=_pr(requested_reviewers=[{"login": "bob"}, {"login": "amy"}]))
        assert snap["requested_reviewers"] == ["amy", "bob"]

    def test_review_state_normalization(self):
        reviews = [
            {
                "id": 1,
                "state": "REQUEST_CHANGES",
                "user": {"login": "a"},
                "html_url": "u",
            },
            {"id": 2, "state": "COMMENT", "user": {"login": "b"}, "html_url": "u"},
            {"id": 3, "state": "APPROVED", "user": {"login": "c"}, "html_url": "u"},
            {"id": 4, "state": "DISMISSED", "user": {"login": "d"}, "html_url": "u"},
        ]
        snap = fj(reviews=reviews)
        states = {rid: r["state"] for rid, r in snap["reviews"].items()}
        assert states == {
            "1": "CHANGES_REQUESTED",  # Gitea REQUEST_CHANGES -> canonical
            "2": "COMMENTED",  # Gitea COMMENT -> canonical
            "3": "APPROVED",
            "4": "DISMISSED",
        }

    def test_pending_and_request_review_excluded(self):
        reviews = [
            {"id": 1, "state": "PENDING", "user": {"login": "a"}, "html_url": "u"},
            {
                "id": 2,
                "state": "REQUEST_REVIEW",
                "user": {"login": "b"},
                "html_url": "u",
            },
            {"id": 3, "state": "APPROVED", "user": {"login": "c"}, "html_url": "u"},
        ]
        snap = fj(reviews=reviews)
        assert set(snap["reviews"]) == {"3"}  # unsubmitted states dropped

    def test_inline_comment_and_thread_from_resolver(self):
        comments = [
            {
                "id": 10,
                "user": {"login": "bob"},
                "body": "why?",
                "path": "x.py",
                "position": 12,
                "original_position": 12,
                "diff_hunk": "@@ -10,3 +10,3 @@\n-b=2\n+b=3",
                "html_url": "u",
                "resolver": {"login": "amy"},  # non-null -> resolved thread
            },
            {
                "id": 11,
                "user": {"login": "cat"},
                "body": "ok",
                "path": "y.py",
                "position": 5,
                "original_position": 5,
                "diff_hunk": "@@\n x",
                "html_url": "u",
                "resolver": None,  # unresolved
            },
        ]
        snap = fj(review_comments=comments)
        c10 = snap["review_comments"]["10"]
        assert c10["line"] == 12 and c10["original_line"] == 12  # position mapping
        assert c10["start_line"] is None and c10["original_start_line"] is None
        assert c10["user"] == "bob" and c10["path"] == "x.py"
        # review_threads synthesized per inline comment; resolver -> resolved bool
        assert snap["review_threads"]["10"] == {
            "resolved": True,
            "path": "x.py",
            "line": 12,
        }
        assert snap["review_threads"]["11"]["resolved"] is False

    def test_issue_comment_mapping(self):
        snap = fj(
            issue_comments=[
                {"id": 7, "user": {"login": "dave"}, "body": "LGTM", "html_url": "u"}
            ]
        )
        assert snap["issue_comments"]["7"] == {
            "user": "dave",
            "body": "LGTM",
            "url": "u",
        }

    def test_status_mapping(self):
        snap = fj(
            statuses=[
                {
                    "context": "ci/build",
                    "status": "success",
                    "target_url": "http://ci/1",
                    "description": "built",
                }
            ]
        )
        assert snap["statuses"]["ci/build"] == {
            "state": "success",
            "url": "http://ci/1",
            "desc": "built",
        }


# --------------------------------------------------------------------------- #
# diff() over forgejo snapshots -> the right event kinds
# --------------------------------------------------------------------------- #


class TestDiffOverForgejo:
    def test_review_changes_requested_is_high(self):
        new = fj(
            reviews=[
                {
                    "id": 1,
                    "state": "REQUEST_CHANGES",
                    "user": {"login": "alice"},
                    "body": "fix the null check",
                    "html_url": "u",
                }
            ]
        )
        event = _only(fj(), new, "pr_review")[0]
        assert event["meta"]["severity"] == "high"
        assert "alice" in event["content"] and "null check" in event["content"]

    def test_inline_comment_event(self):
        new = fj(
            review_comments=[
                {
                    "id": 10,
                    "user": {"login": "bob"},
                    "body": "why 3?",
                    "path": "x.py",
                    "position": 12,
                    "original_position": 12,
                    "diff_hunk": "@@ -10,3 +10,3 @@\n-    b = 2\n+    b = 3",
                    "html_url": "u",
                    "resolver": None,
                }
            ]
        )
        event = _only(fj(), new, "pr_inline_comment")[0]
        assert "x.py:12" in event["content"] and "b = 3" in event["content"]

    def test_conversation_comment_event(self):
        new = fj(
            issue_comments=[
                {"id": 7, "user": {"login": "dave"}, "body": "ship it", "html_url": "u"}
            ]
        )
        event = _only(fj(), new, "pr_comment")[0]
        assert "dave" in event["content"] and "ship it" in event["content"]

    def test_status_success_and_failure(self):
        def status(state):
            return fj(
                statuses=[
                    {
                        "context": "ci/build",
                        "status": state,
                        "target_url": "u",
                        "description": "d",
                    }
                ]
            )

        ok = _only(fj(), status("success"), "pr_status")[0]
        assert ok["meta"]["severity"] == "info" and "success" in ok["content"]
        bad = _only(status("success"), status("failure"), "pr_status")[0]
        assert bad["meta"]["severity"] == "high" and "failure" in bad["content"]

    def test_thread_resolve_transition(self):
        def comment(resolved):
            return fj(
                review_comments=[
                    {
                        "id": 10,
                        "user": {"login": "bob"},
                        "body": "hm",
                        "path": "x.py",
                        "position": 12,
                        "original_position": 12,
                        "diff_hunk": "@@\n x",
                        "html_url": "u",
                        "resolver": {"login": "amy"} if resolved else None,
                    }
                ]
            )

        # unresolved -> resolved fires pr_thread ("resolved"); the reverse fires "reopened"
        resolved = _only(comment(False), comment(True), "pr_thread")[0]
        assert "resolved" in resolved["content"] and "x.py:12" in resolved["content"]
        reopened = _only(comment(True), comment(False), "pr_thread")[0]
        assert "reopened" in reopened["content"]

    def test_new_commits_on_head_change(self):
        base = fj()
        pushed = fj(pr=_pr(head={"sha": "b" * 40}))
        event = _only(base, pushed, "pr_commits")[0]
        assert "New commits pushed" in event["content"]

    def test_merged_is_terminal(self):
        merged = fj(
            pr=_pr(state="closed", merged=True, merged_by={"login": "carol"}),
            reviews=[
                {"id": 1, "state": "APPROVED", "user": {"login": "x"}, "html_url": "u"}
            ],
        )
        events = pm.diff(fj(), merged, "o/r#1")
        assert len(events) == 1  # terminal; nothing else leaks
        assert events[0]["type"] == "pr_merged" and "carol" in events[0]["content"]

    def test_conflict_high(self):
        clean, dirty = fj(pr=_pr(mergeable=True)), fj(pr=_pr(mergeable=False))
        event = _only(clean, dirty, "pr_conflict")[0]
        assert event["meta"]["severity"] == "high"


# --------------------------------------------------------------------------- #
# ForgejoClient driven through httpx.MockTransport fed by FakeForgejo
# --------------------------------------------------------------------------- #

_NUM = 4


def _fake_transport(fake: h.FakeForgejo) -> httpx.MockTransport:
    """Drive the production client through FakeForgejo's REST routing without a
    subprocess: a MockTransport that resolves each request against the same fake the
    e2e test uses (so client and fake are verified to agree on the REST protocol)."""

    def handler(request: httpx.Request) -> httpx.Response:
        parsed = httpx.URL(str(request.url))
        status, body, extra = fake._next_response(parsed.path, parsed.query.decode())
        headers = {"X-RateLimit-Remaining": "4999", **extra}
        return httpx.Response(status, json=body, headers=headers)

    return httpx.MockTransport(handler)


def _run(coro_fn):
    return anyio.run(coro_fn)


class TestClientFetch:
    def test_fetch_assembles_all_pieces(self):
        fake = h.FakeForgejo(_NUM, h.forgejo_pr(_NUM))
        fake.set_reviews(
            [{"id": 1, "state": "APPROVED", "user": {"login": "a"}, "html_url": "u"}]
        )
        fake.set_issue_comments(
            [{"id": 7, "user": {"login": "d"}, "body": "hi", "html_url": "u"}]
        )
        fake.set_statuses(
            [
                {
                    "context": "ci",
                    "status": "success",
                    "target_url": "u",
                    "description": "",
                }
            ]
        )
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        data = _run(scenario)
        assert data["pr"]["number"] == _NUM
        assert len(data["reviews"]) == 1
        assert len(data["issue_comments"]) == 1
        assert len(data["statuses"]) == 1  # flattened from combined["statuses"]
        snap = pm.snapshot_from_forgejo(data)
        assert "1" in snap["reviews"] and "ci" in snap["statuses"]

    def test_reviews_paginate_across_pages(self):
        # 120 reviews at limit=50 -> pages of 50/50/20 (a short last page stops it).
        reviews = [
            {"id": i, "state": "APPROVED", "user": {"login": f"u{i}"}, "html_url": "u"}
            for i in range(120)
        ]
        fake = h.FakeForgejo(_NUM, h.forgejo_pr(_NUM))
        fake.set_reviews(reviews)
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        data = _run(scenario)
        assert len(data["reviews"]) == 120  # all pages merged
        assert len(pm.snapshot_from_forgejo(data)["reviews"]) == 120

    def test_issue_comments_paginate(self):
        comments = [
            {"id": i, "user": {"login": "d"}, "body": "c", "html_url": "u"}
            for i in range(51)  # 51 -> 50 + a short page of 1
        ]
        fake = h.FakeForgejo(_NUM, h.forgejo_pr(_NUM))
        fake.set_issue_comments(comments)
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        assert len(_run(scenario)["issue_comments"]) == 51

    def test_per_review_comment_fanout_only_when_count(self):
        # Two reviews: only the one with comments_count>0 costs a /comments GET.
        fake = h.FakeForgejo(_NUM, h.forgejo_pr(_NUM))
        fake.set_reviews(
            [
                {
                    "id": 1,
                    "state": "COMMENT",
                    "user": {"login": "a"},
                    "html_url": "u",
                    "comments_count": 2,
                },
                {
                    "id": 2,
                    "state": "APPROVED",
                    "user": {"login": "b"},
                    "html_url": "u",
                    "comments_count": 0,
                },
            ]
        )
        fake.set_review_comments(
            1,
            [
                {
                    "id": 10,
                    "user": {"login": "a"},
                    "body": "c1",
                    "path": "x.py",
                    "position": 3,
                    "original_position": 3,
                    "diff_hunk": "@@\n x",
                    "html_url": "u",
                    "resolver": None,
                },
                {
                    "id": 11,
                    "user": {"login": "a"},
                    "body": "c2",
                    "path": "x.py",
                    "position": 4,
                    "original_position": 4,
                    "diff_hunk": "@@\n y",
                    "html_url": "u",
                    "resolver": None,
                },
            ],
        )
        # review 2 has no comments endpoint registered; count=0 means it's never queried.
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        data = _run(scenario)
        assert len(data["review_comments"]) == 2  # only review 1's inline comments
        assert {c["id"] for c in data["review_comments"]} == {10, 11}

    def test_not_found_pr_is_terminal(self):
        # The fake serves a DIFFERENT number, so /pulls/{_NUM} 404s -> ForgejoNotFound.
        fake = h.FakeForgejo(_NUM + 1, h.forgejo_pr(_NUM + 1))
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        with pytest.raises(fc.ForgejoNotFound):
            _run(scenario)

    def test_subresource_404_swallowed_to_empty(self):
        # A transport that 404s the reviews + issue-comments LIST endpoints but 200s the
        # PR core (and the combined status): the fetch succeeds with empty lists (only
        # the pulls 404 is terminal; the _get_list_optional sub-resources swallow 404).
        pr = h.forgejo_pr(_NUM)

        def handler(request: httpx.Request) -> httpx.Response:
            path = httpx.URL(str(request.url)).path
            if path.endswith(f"/pulls/{_NUM}"):
                return httpx.Response(200, json=pr)
            if path.endswith("/status"):
                return httpx.Response(200, json={"statuses": []})
            return httpx.Response(404, json={})  # reviews / issue-comments 404

        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=httpx.MockTransport(handler)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        data = _run(scenario)
        assert data["reviews"] == [] and data["issue_comments"] == []
        assert data["statuses"] == []

    def test_combined_status_404_is_swallowed_not_terminal(self):
        # A 404 on the combined-status sub-resource means "no statuses", NOT "PR gone":
        # only the top-level pulls fetch decides existence. The status fetch is swallowed
        # to [] like the other sub-resources, so fetch_pr still succeeds. (Regression
        # guard for the fix to _fetch_pr_once, which previously used the non-swallowing
        # _get_obj and would spuriously declare the PR gone on a stray status 404.)
        pr = h.forgejo_pr(_NUM)

        def handler(request: httpx.Request) -> httpx.Response:
            path = httpx.URL(str(request.url)).path
            if path.endswith(f"/pulls/{_NUM}"):
                return httpx.Response(200, json=pr)
            if path.endswith("/status"):
                return httpx.Response(404, json={})
            return httpx.Response(200, json=[])

        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=httpx.MockTransport(handler)
        )

        async def scenario():
            return await client.fetch_pr("o", "r", _NUM)

        result = _run(scenario)
        assert result["pr"]["number"] == _NUM
        assert result["statuses"] == []


class TestErrorClassification:
    def _resp(self, code, headers=None):
        return httpx.Response(code, headers=headers or {}, json={})

    def test_http_status_classification(self):
        client = fc.ForgejoClient(base_url="http://fj", token="x")
        future = str(int(time.time()) + 3600)
        with pytest.raises(fc.ForgejoAuthError):
            client._classify_http(self._resp(401))
        # 403 without rate-limit headers -> auth (token lacks access)
        with pytest.raises(fc.ForgejoAuthError):
            client._classify_http(self._resp(403, {"X-RateLimit-Remaining": "7"}))
        # 403 WITH rate-limit headers -> rate limited
        with pytest.raises(fc.ForgejoRateLimited):
            client._classify_http(
                self._resp(
                    403, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": future}
                )
            )
        with pytest.raises(fc.ForgejoRateLimited):
            client._classify_http(self._resp(429, {"Retry-After": "30"}))
        with pytest.raises(fc.ForgejoNotFound):
            client._classify_http(self._resp(404))
        with pytest.raises(fc.ForgejoTransient):
            client._classify_http(self._resp(502))
        client._classify_http(self._resp(200))  # no raise

    def test_ratelimited_carries_reset_at(self):
        client = fc.ForgejoClient(base_url="http://fj", token="x")
        future = int(time.time()) + 3600
        with pytest.raises(fc.ForgejoRateLimited) as exc:
            client._classify_http(
                self._resp(
                    429,
                    {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(future)},
                )
            )
        assert exc.value.reset_at == float(future)

    def test_rate_limit_throttle(self):
        client = fc.ForgejoClient(base_url="http://fj", token="x")
        future = str(int(time.time()) + 3600)
        client._update_rate_limit(
            httpx.Headers({"X-RateLimit-Remaining": "10", "X-RateLimit-Reset": future})
        )
        assert client.should_throttle(threshold=50) == float(future)
        client._update_rate_limit(
            httpx.Headers(
                {"X-RateLimit-Remaining": "4000", "X-RateLimit-Reset": future}
            )
        )
        assert client.should_throttle(threshold=50) is None


class TestConfiguredAndBaseUrl:
    def test_base_url_normalization(self):
        # instance root -> .../api/v1 appended
        assert (
            fc.ForgejoClient(base_url="https://codeberg.org").base_url
            == "https://codeberg.org/api/v1"
        )
        # already-normalized root accepted as-is
        assert (
            fc.ForgejoClient(base_url="https://codeberg.org/api/v1").base_url
            == "https://codeberg.org/api/v1"
        )
        # trailing slash tolerated
        assert (
            fc.ForgejoClient(base_url="https://codeberg.org/").base_url
            == "https://codeberg.org/api/v1"
        )

    def test_configured_reflects_base_url(self):
        assert fc.ForgejoClient(base_url="https://fj").configured is True
        assert fc.ForgejoClient(base_url=None).configured is False

    def test_auth_header_is_token_scheme(self):
        headers = fc.ForgejoClient(base_url="https://fj", token="PAT")._headers()
        assert headers["Authorization"] == "token PAT"  # Gitea style, NOT Bearer

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("FORGEJO_API_URL", "https://env-fj")
        monkeypatch.setenv("FORGEJO_TOKEN", "env-pat")
        client = fc.ForgejoClient()
        assert client.base_url == "https://env-fj/api/v1"
        assert client._headers()["Authorization"] == "token env-pat"


class TestRetry:
    """The in-poll retry wrapper against a fake that faults a bounded number of times."""

    def test_transient_blip_recovers_via_injected_sleep(self):
        fake = h.FakeForgejo(_NUM, h.forgejo_pr(_NUM))
        fake.set_fault(503, count=2)  # two 503s, then auto-recover
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )
        sleeps: list[float] = []

        async def fake_sleep(delay):
            sleeps.append(delay)

        async def scenario():
            return await fc._retry_transient(
                lambda: client._fetch_pr_once("o", "r", _NUM), sleep=fake_sleep
            )

        data = _run(scenario)
        assert data["pr"]["number"] == _NUM  # recovered on the 3rd attempt
        assert sleeps == [1.0, 2.0]  # exponential backoff between the two failures

    def test_persistent_transient_exhausts(self):
        fake = h.FakeForgejo(_NUM, h.forgejo_pr(_NUM))
        fake.set_fault(500)  # never recovers
        client = fc.ForgejoClient(
            base_url="http://fj", token="x", transport=_fake_transport(fake)
        )

        async def fake_sleep(delay):
            pass

        async def scenario():
            return await fc._retry_transient(
                lambda: client._fetch_pr_once("o", "r", _NUM), sleep=fake_sleep
            )

        with pytest.raises(fc.ForgejoTransient):
            _run(scenario)

    def test_not_found_not_retried(self):
        calls = {"n": 0}

        async def once():
            calls["n"] += 1
            raise fc.ForgejoNotFound("gone")

        async def fake_sleep(delay):
            pass

        async def scenario():
            return await fc._retry_transient(once, sleep=fake_sleep)

        with pytest.raises(fc.ForgejoNotFound):
            _run(scenario)
        assert calls["n"] == 1  # not-found propagates immediately


# --------------------------------------------------------------------------- #
# split on-disk storage for a provider-prefixed forgejo tracker
# --------------------------------------------------------------------------- #


class TestForgejoStorage:
    def test_storage_key_prefixing(self):
        assert pm.storage_key("github", "o", "r", 1) == "o/r#1"  # unprefixed
        assert pm.storage_key("forgejo", "o", "r", 1) == "forgejo:o/r#1"

    def test_split_storage_roundtrip_forgejo(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        base_url = "https://fj.example/api/v1"
        tracker = pm.PRTracker("o", "r", 1, None, provider="forgejo", base_url=base_url)
        tracker.snapshot = {"timeline": {}, "labels": [], "state": "open"}
        tracker.consecutive_no_update = 2
        events = tracker.record(
            [
                pm._event("pr_review", "info", "one", "o/r#1", "u", "review:R1"),
                pm._event("pr_review", "info", "two", "o/r#1", "u", "review:R2"),
            ]
        )
        pm.save_state(tracker)
        pm.append_events(tracker, events)
        first_id = events[0]["id"]
        tracker.subscribers.add("sidA")
        tracker.acked["sidA"] = {first_id}
        pm.save_subscriber(tracker, "sidA")

        # On-disk dir is provider-prefixed (distinct from a same-ref github tracker).
        directory = pm._tracker_dir("forgejo:o/r#1")
        assert (directory / "state.json").exists()
        assert directory != pm._tracker_dir("o/r#1")

        # load_trackers with a {provider: client} map round-trips provider + base_url +
        # storage_key. The github ("o/r#1") tracker is absent, so the forgejo one loads
        # by its storage_key.
        loaded = {t.storage_key: t for t in pm.load_trackers({"forgejo": object()})}
        t = loaded["forgejo:o/r#1"]
        assert t.provider == "forgejo"
        assert t.base_url == base_url
        assert t.key == "o/r#1"  # display ref stays unprefixed
        assert t.consecutive_no_update == 2
        assert t.event_ids == {events[0]["id"], events[1]["id"]}
        assert t.subscribers == {"sidA"} and t.acked["sidA"] == {first_id}
        assert [e["id"] for e in t.unacked_for("sidA")] == [events[1]["id"]]

    def test_forgejo_dir_distinct_from_same_ref_github(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        gh = pm.PRTracker("o", "r", 1, None)  # github (default)
        fj_ = pm.PRTracker("o", "r", 1, None, provider="forgejo", base_url="https://fj")
        gh.snapshot = {"timeline": {}, "labels": [], "state": "open"}
        fj_.snapshot = {"timeline": {}, "labels": [], "state": "open"}
        pm.save_state(gh)
        pm.save_state(fj_)
        # Both trackers coexist on disk in distinct dirs, keyed by storage_key.
        assert pm._tracker_dir(gh.storage_key).exists()
        assert pm._tracker_dir(fj_.storage_key).exists()
        assert pm._tracker_dir(gh.storage_key) != pm._tracker_dir(fj_.storage_key)

        loaded = {
            t.storage_key: t
            for t in pm.load_trackers({"github": object(), "forgejo": object()})
        }
        assert loaded["o/r#1"].provider == "github"
        assert loaded["forgejo:o/r#1"].provider == "forgejo"

    def test_load_skips_forgejo_when_provider_unmapped(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SUBSPACE_DATA_DIR", str(tmp_path))
        fj_ = pm.PRTracker("o", "r", 1, None, provider="forgejo", base_url="https://fj")
        fj_.snapshot = {"timeline": {}, "labels": [], "state": "open"}
        pm.save_state(fj_)
        # A github-only client map (no "forgejo" key) skips the forgejo tracker rather
        # than loading it with no client (which would crash the poll loop).
        loaded = pm.load_trackers({"github": object()})
        assert all(t.provider != "forgejo" for t in loaded)
