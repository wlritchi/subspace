# vim: filetype=python
"""Shared test helpers: server process spawning and fake GitHub GraphQL and Forgejo
servers."""

import contextlib
import http.server
import json
import os
import re
import socket
import subprocess
import threading
import time
import urllib.parse
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SERVER = str(ROOT / "server" / "subspace-server.py")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port: int | str, timeout: float = 25.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.3)
            if s.connect_ex(("127.0.0.1", int(port))) == 0:
                return True
        time.sleep(0.2)
    return False


# Env-var prefixes that could couple a test process to the machine's real setup:
# SUBSPACE_WS_URL/SUBSPACE_TOKEN pointing relays at a live server (uri()
# prefers the full-URL override, ignoring the test's host/port), real GitHub
# credentials, and the live session's CLAUDE_* identity. Tests re-set exactly what
# they need.
_ISOLATE_PREFIXES = ("SUBSPACE_", "GITHUB_", "FORGEJO_", "CLAUDE_")


def _isolated_environ() -> dict:
    return {k: v for k, v in os.environ.items() if not k.startswith(_ISOLATE_PREFIXES)}


def server_env(
    ws_port: int,
    data_dir: Path,
    *,
    graphql_url: str | None = None,
    forgejo_url: str | None = None,
    forgejo_instances: dict[str, str] | None = None,
    poll_seconds: str = "1",
    warm_ttl: str | None = None,
    agent_ttl: str | None = None,
    channel_ttl: str | None = None,
    settle: str | None = None,
) -> dict:
    env = _isolated_environ()
    env["SUBSPACE_WS_PORT"] = str(ws_port)
    env["SUBSPACE_DATA_DIR"] = str(data_dir)
    env["SUBSPACE_PR_POLL_SECONDS"] = poll_seconds
    if warm_ttl is not None:
        env["SUBSPACE_PR_WARM_TTL_SECONDS"] = warm_ttl
    if agent_ttl is not None:
        env["SUBSPACE_AGENT_TTL_SECONDS"] = agent_ttl
    if channel_ttl is not None:
        env["SUBSPACE_CHANNEL_TTL_SECONDS"] = channel_ttl
    if settle is not None:
        env["SUBSPACE_RECLAIM_SETTLE_SECONDS"] = settle
    if graphql_url:
        env["GITHUB_GRAPHQL_URL"] = graphql_url
        env["GITHUB_TOKEN"] = "test-token"
    if forgejo_url:
        # Point the spawned server's default ForgejoClient at the fake (client appends
        # /api/v1). This is the DEFAULT instance (unaliased FORGEJO_API_URL/TOKEN).
        env["FORGEJO_API_URL"] = forgejo_url
        env["FORGEJO_TOKEN"] = "test-token"
    # Named Forgejo instances: {alias -> instance root url}. Each becomes a
    # FORGEJO_<ALIAS>_API_URL / FORGEJO_<ALIAS>_TOKEN env pair (alias uppercased,
    # '-' -> '_'), so the server builds one named ForgejoClient per entry.
    for alias, url in (forgejo_instances or {}).items():
        env_alias = alias.upper().replace("-", "_")
        env[f"FORGEJO_{env_alias}_API_URL"] = url
        env[f"FORGEJO_{env_alias}_TOKEN"] = "test-token"
    return env


@contextlib.contextmanager
def server_process(env: dict):
    proc = subprocess.Popen(
        ["uv", "run", "-qs", SERVER],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_port(env["SUBSPACE_WS_PORT"]):
            raise RuntimeError("server did not start listening")
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def _slice_page(nodes: list, offset: int, page_size: int) -> dict:
    """A connection {pageInfo, nodes} for nodes[offset:offset+page_size]. Cursors are
    just stringified next-page indices, so a follow-up `after:<endCursor>` slices on."""
    chunk = nodes[offset : offset + page_size]
    nxt = offset + page_size
    has_next = nxt < len(nodes)
    return {
        "pageInfo": {
            "hasNextPage": has_next,
            "endCursor": str(nxt) if has_next else None,
        },
        "nodes": chunk,
    }


def _rollup_contexts(pr: dict) -> dict | None:
    """The head commit's check-rollup `contexts` connection on a pr node, or None."""
    commits = (pr.get("commits") or {}).get("nodes") or []
    if not commits:
        return None
    rollup = (commits[0].get("commit") or {}).get("statusCheckRollup")
    if not rollup:
        return None
    return rollup.get("contexts")


def _with_sliced_contexts(pr: dict, sliced: dict) -> dict:
    """A copy of pr['commits'] with the rollup's contexts replaced by `sliced`, without
    mutating the canonical pr node (tests mutate pr between polls)."""
    commits = dict(pr.get("commits") or {})
    nodes = [dict(n) for n in (commits.get("nodes") or [])]
    if not nodes:
        return commits
    commit = dict(nodes[0].get("commit") or {})
    rollup = dict(commit.get("statusCheckRollup") or {})
    rollup["contexts"] = sliced
    commit["statusCheckRollup"] = rollup
    nodes[0]["commit"] = commit
    commits["nodes"] = nodes
    return commits


def _target_connection(query: str) -> str | None:
    """Which paginated connection a follow-up page query targets: the one whose arg
    list carries `after` (the per-thread comments(first:100) sub-selection has no
    `after`, so it never matches)."""
    for name, args in re.findall(r"(\w+)\(([^)]*)\)", query):
        if "after" in args and name in {
            "reviews",
            "comments",
            "reviewThreads",
            "contexts",
        }:
            return name
    return None


class FakeGitHub:
    """A tiny cursor-aware GraphQL endpoint. `pr` is the pullRequest node for `number`;
    mutate it between polls. Any other number returns null (i.e. not found).

    The initial query returns each paginated connection sliced to `page_size` (default
    large, so PRs that fit one page behave exactly as before) plus a pageInfo cursor;
    a follow-up query carrying `variables.cursor` returns the next slice of whichever
    connection it targets. Cursors are stringified node offsets."""

    def __init__(
        self,
        number: int,
        pr: dict,
        *,
        page_size: int = 100,
        extra: dict[int, dict] | None = None,
    ) -> None:
        self.number = number
        self.pr = pr
        # Optional additional PR nodes keyed by number, so one fake (one GraphQL URL)
        # can serve several PRs at once — the server has a single GITHUB_GRAPHQL_URL,
        # so a session subscribed to two PRs needs them behind the same endpoint.
        self.extra: dict[int, dict] = dict(extra or {})
        self.page_size = page_size
        self._server: http.server.ThreadingHTTPServer | None = None
        # Optional fault injection (driven cross-thread from a test). When
        # fault_status is set, do_POST returns that HTTP status (with any extra
        # headers, e.g. Retry-After) instead of the normal 200, so tests can exercise
        # the server's error taxonomy (auth / rate-limited / transient). fault_count
        # None means "until cleared"; an int auto-recovers after that many requests.
        self.fault_status: int | None = None
        self.fault_headers: dict[str, str] = {}
        self.fault_count: int | None = None

    def set_fault(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
        count: int | None = None,
    ) -> None:
        """Make the next request(s) fail with `status` (and optional extra `headers`).
        count=None faults until clear_fault(); an int faults that many requests then
        auto-recovers."""
        self.fault_status = status
        self.fault_headers = dict(headers or {})
        self.fault_count = count

    def clear_fault(self) -> None:
        self.fault_status = None
        self.fault_headers = {}
        self.fault_count = None

    def _pr_for(self, number: object) -> dict | None:
        """The live pr node for `number` (primary or one of `extra`), or None."""
        if number == self.number:
            return self.pr
        return self.extra.get(number)  # type: ignore[arg-type]

    def _first_page(self, source: dict) -> dict:
        """The pr node with every paginated connection sliced to its first page."""
        pr = dict(source)
        for conn in ("reviews", "comments", "reviewThreads"):
            if conn in source:
                full = (source.get(conn) or {}).get("nodes") or []
                pr[conn] = _slice_page(full, 0, self.page_size)
        contexts = _rollup_contexts(source)
        if contexts is not None:
            full = contexts.get("nodes") or []
            pr["commits"] = _with_sliced_contexts(
                source, _slice_page(full, 0, self.page_size)
            )
        return pr

    def _page_response(self, query: str, offset: int, source: dict) -> dict:
        """The body for a follow-up page query: just its connection at the right path."""
        name = _target_connection(query)
        if name == "contexts":
            contexts = _rollup_contexts(source) or {}
            full = contexts.get("nodes") or []
            pr = {
                "commits": _with_sliced_contexts(
                    source, _slice_page(full, offset, self.page_size)
                )
            }
        elif name in ("reviews", "comments", "reviewThreads"):
            full = (source.get(name) or {}).get("nodes") or []
            pr = {name: _slice_page(full, offset, self.page_size)}
        else:
            pr = {}
        return {"data": {"repository": {"pullRequest": pr}}}

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.server_address[1]

    @property
    def graphql_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/graphql"

    def __enter__(self) -> "FakeGitHub":
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:  # silence
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", 0))
                req = json.loads(self.rfile.read(length) or b"{}")
                variables = req.get("variables") or {}
                number = variables.get("number")
                cursor = variables.get("cursor")
                if outer.fault_status is not None:
                    status = outer.fault_status
                    headers = dict(outer.fault_headers)
                    if outer.fault_count is not None:
                        outer.fault_count -= 1
                        if outer.fault_count <= 0:
                            outer.clear_fault()  # auto-recover after the burst
                    body = json.dumps({"message": "fault"}).encode()
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    for name, value in headers.items():
                        self.send_header(name, value)
                    self.end_headers()
                    self.wfile.write(body)
                    return
                source = outer._pr_for(number)
                if source is None:
                    payload = {"data": {"repository": {"pullRequest": None}}}
                elif cursor is None:  # the main query -> first page of every connection
                    payload = {
                        "data": {
                            "repository": {"pullRequest": outer._first_page(source)}
                        }
                    }
                else:  # a follow-up query draining one connection from `cursor`
                    payload = outer._page_response(
                        req.get("query") or "", int(cursor), source
                    )
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-RateLimit-Remaining", "4999")
                self.send_header("X-RateLimit-Reset", str(int(time.time()) + 3600))
                self.end_headers()
                self.wfile.write(body)

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc) -> None:
        if self._server is not None:
            self._server.shutdown()


# --------------------------------------------------------------------------- #
# Fake Forgejo/Gitea REST endpoint (the REST parallel to FakeGitHub)
# --------------------------------------------------------------------------- #


def forgejo_pr(number: int, **override) -> dict:
    """A raw Gitea pull-request core object (the /pulls/{n} response), with sane
    defaults. Mutate the returned dict (or the copy stored on a FakeForgejo) between
    polls the way FakeGitHub tests mutate `gh.pr`."""
    pr = {
        "number": number,
        "title": "Add feature",
        "html_url": f"https://fj/owner/repo/pulls/{number}",
        "state": "open",  # Gitea uses lowercase "open"/"closed"
        "merged": False,
        "merged_by": None,
        "mergeable": True,  # bool | None (None -> "unknown")
        "draft": False,
        "head": {"sha": "a" * 40},
        "labels": [],
        "requested_reviewers": [],
    }
    pr.update(override)
    return pr


class FakeForgejo:
    """A tiny Gitea-style REST endpoint serving the five GETs forgejo_client.fetch_pr
    issues, driven by a mutable in-memory model you update between polls (mirrors
    FakeGitHub's mutability + set_fault fault injection).

    Endpoints (all under the client-appended /api/v1):
      GET /repos/{o}/{r}/pulls/{n}                       -> self.pr (or extra), 404 if absent
      GET /repos/{o}/{r}/pulls/{n}/reviews               -> self.reviews (paginated)
      GET /repos/{o}/{r}/pulls/{n}/reviews/{id}/comments -> self.review_comments[id]
      GET /repos/{o}/{r}/issues/{n}/comments             -> self.issue_comments (paginated)
      GET /repos/{o}/{r}/commits/{sha}/status            -> {"statuses": self.statuses}

    List endpoints honor ?page=&limit=; `api_url` is the instance root (the client
    appends /api/v1). A single fake can serve several PRs via `extra` (the server has
    one FORGEJO_API_URL)."""

    def __init__(
        self,
        number: int,
        pr: dict,
        *,
        page_size: int = 50,
        extra: dict[int, dict] | None = None,
    ) -> None:
        self.number = number
        self.pr = pr
        self.extra: dict[int, dict] = dict(extra or {})
        self.page_size = page_size
        # Sub-resources, keyed by PR number. reviews/issue_comments/statuses are lists;
        # review_comments maps a review id -> its inline comments.
        self.reviews: dict[int, list[dict]] = {}
        self.review_comments: dict[int, dict[int, list[dict]]] = {}
        self.issue_comments: dict[int, list[dict]] = {}
        self.statuses: dict[int, list[dict]] = {}
        self._server: http.server.ThreadingHTTPServer | None = None
        # Fault injection, identical semantics to FakeGitHub.set_fault.
        self.fault_status: int | None = None
        self.fault_headers: dict[str, str] = {}
        self.fault_count: int | None = None

    # --- fault injection (cross-thread, same contract as FakeGitHub) ------------- #
    def set_fault(
        self,
        status: int,
        *,
        headers: dict[str, str] | None = None,
        count: int | None = None,
    ) -> None:
        self.fault_status = status
        self.fault_headers = dict(headers or {})
        self.fault_count = count

    def clear_fault(self) -> None:
        self.fault_status = None
        self.fault_headers = {}
        self.fault_count = None

    # --- model helpers ----------------------------------------------------------- #
    def _pr_for(self, number: int) -> dict | None:
        if number == self.number:
            return self.pr
        return self.extra.get(number)

    def set_reviews(self, reviews: list[dict], *, number: int | None = None) -> None:
        self.reviews[number if number is not None else self.number] = reviews

    def set_review_comments(
        self, review_id: int, comments: list[dict], *, number: int | None = None
    ) -> None:
        n = number if number is not None else self.number
        self.review_comments.setdefault(n, {})[review_id] = comments

    def set_issue_comments(
        self, comments: list[dict], *, number: int | None = None
    ) -> None:
        self.issue_comments[number if number is not None else self.number] = comments

    def set_statuses(self, statuses: list[dict], *, number: int | None = None) -> None:
        self.statuses[number if number is not None else self.number] = statuses

    def set_pr(self, *, number: int | None = None, **fields) -> None:
        """Mutate PR-core fields (mergeable, merged, state, head, merged_by, ...)."""
        pr = self._pr_for(number if number is not None else self.number)
        assert pr is not None
        for k, v in fields.items():
            pr[k] = v

    # --- server ------------------------------------------------------------------ #
    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.server_address[1]

    @property
    def api_url(self) -> str:
        """The instance root; the ForgejoClient appends /api/v1 itself."""
        return f"http://127.0.0.1:{self.port}"

    def _slice(self, items: list[dict], query: str) -> list[dict]:
        """Return the page of `items` selected by ?page=&limit= in the query string."""
        params = urllib.parse.parse_qs(query)
        page = int((params.get("page") or ["1"])[0])
        limit = int((params.get("limit") or [str(self.page_size)])[0])
        start = (page - 1) * limit
        return items[start : start + limit]

    def _next_response(self, path: str, query: str) -> tuple[int, object, dict]:
        """(status, json-body, extra-headers) for one GET, applying any injected fault
        first (so both the live server and a MockTransport share fault semantics)."""
        if self.fault_status is not None:
            status = self.fault_status
            headers = dict(self.fault_headers)
            if self.fault_count is not None:
                self.fault_count -= 1
                if self.fault_count <= 0:
                    self.clear_fault()  # auto-recover after the burst
            return status, {"message": "fault"}, headers
        status, body = self._resolve(path, query)
        return status, body, {}

    def _resolve(self, path: str, query: str) -> tuple[int, object]:
        """Map a request path to (status, json-body). 404 with {} body for unknown PRs
        or missing sub-resources — the client swallows sub-resource 404s to []."""
        # /api/v1/repos/{o}/{r}/pulls/{n}[/reviews[/{id}/comments]]
        # /api/v1/repos/{o}/{r}/issues/{n}/comments
        # /api/v1/repos/{o}/{r}/commits/{sha}/status
        m = re.match(r"^/api/v1/repos/[^/]+/[^/]+/pulls/(\d+)$", path)
        if m:
            pr = self._pr_for(int(m.group(1)))
            return (200, pr) if pr is not None else (404, {})
        m = re.match(r"^/api/v1/repos/[^/]+/[^/]+/pulls/(\d+)/reviews$", path)
        if m:
            n = int(m.group(1))
            if self._pr_for(n) is None:
                return (404, {})
            return (200, self._slice(self.reviews.get(n, []), query))
        m = re.match(
            r"^/api/v1/repos/[^/]+/[^/]+/pulls/(\d+)/reviews/(\d+)/comments$", path
        )
        if m:
            n, rid = int(m.group(1)), int(m.group(2))
            comments = (self.review_comments.get(n) or {}).get(rid)
            if comments is None:
                return (404, {})
            return (200, self._slice(comments, query))
        m = re.match(r"^/api/v1/repos/[^/]+/[^/]+/issues/(\d+)/comments$", path)
        if m:
            n = int(m.group(1))
            if self._pr_for(n) is None:
                return (404, {})
            return (200, self._slice(self.issue_comments.get(n, []), query))
        m = re.match(r"^/api/v1/repos/[^/]+/[^/]+/commits/([^/]+)/status$", path)
        if m:
            sha = m.group(1)
            for n, pr in [(self.number, self.pr), *self.extra.items()]:
                if pr is not None and (pr.get("head") or {}).get("sha") == sha:
                    return (200, {"statuses": self.statuses.get(n, [])})
            return (200, {"statuses": []})  # unknown sha -> empty combined status
        return (404, {})

    def __enter__(self) -> "FakeForgejo":
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:  # silence
                pass

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                status, body, extra = outer._next_response(parsed.path, parsed.query)
                self._reply(status, body, extra)

            def _reply(
                self, status: int, body: object, extra: dict | None = None
            ) -> None:
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("X-RateLimit-Remaining", "4999")
                self.send_header("X-RateLimit-Reset", str(int(time.time()) + 3600))
                for name, value in (extra or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(data)

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc) -> None:
        if self._server is not None:
            self._server.shutdown()
