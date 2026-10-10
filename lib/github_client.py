# vim: filetype=python
"""Async GitHub client for PR monitoring: usually one GraphQL query per poll, but a
very active PR follows pagination so nothing is silently lost past the first page.

One query fetches PR core, reviews, review threads (inline comments + resolution),
conversation comments, the head commit's check rollup, and recent timeline events
(label add/remove, reviewer requested/removed, draft toggled, force-push) whose
globally-unique node ids give recurring transitions a stable identity. The four
connections that grow without bound with PR activity — reviews, conversation
comments, review threads, and the check rollup's contexts (a large CI matrix) — are
drained by following their pageInfo cursors, so a busy PR doesn't lose data past the
first page while a quiet PR still costs a single round-trip. This is still far fewer
API calls than the REST equivalent, which keeps us well under rate limits. Rate-limit
headers are parsed so the server can throttle, and failures are classified
(auth / not-found / rate-limited / transient) so the server can tailor its recovery.

Reads GITHUB_TOKEN. The GraphQL endpoint is GITHUB_GRAPHQL_URL (default
{GITHUB_API_URL}/graphql, GITHUB_API_URL default https://api.github.com), so it
works against GitHub Enterprise and a local fake in tests.
"""

import asyncio
import os
import sys
import time
from collections.abc import Awaitable, Callable

import httpx

import pr_errors

_USER_AGENT = "subspace"

# A brief GitHub blip (5xx / network) shouldn't cost a whole poll interval, so a
# single fetch is retried in-poll on GitHubTransient only, with exponential backoff
# (delays of _FETCH_BASE_DELAY * 2**attempt: ~1s, then ~2s for 3 attempts). Auth,
# not-found, and rate-limited errors are NOT retried — they won't clear in seconds
# and rate-limiting has its own reset handling in the server.
_FETCH_ATTEMPTS = 3
_FETCH_BASE_DELAY = 1.0


class GitHubError(pr_errors.PRError):
    """Base for classified GitHub failures. Subclasses the provider-agnostic PR-fetch
    taxonomy (pr_errors) so the server's tracker loop catches GitHub and Forgejo
    failures through the same base classes; the GitHub* names stay for existing
    call sites and tests."""


class GitHubAuthError(GitHubError, pr_errors.PRAuthError):
    """Bad/insufficient credentials (401/403 non-rate-limit, GraphQL FORBIDDEN)."""


class GitHubNotFound(GitHubError, pr_errors.PRNotFound):
    """The PR/repo does not exist or the token can't see it (404, GraphQL NOT_FOUND)."""


class GitHubRateLimited(GitHubError, pr_errors.PRRateLimited):
    """Rate limit hit; `reset_at` is the epoch seconds to wait until.

    Inherits PRRateLimited.__init__(reset_at, message), which stores `reset_at`."""


class GitHubTransient(GitHubError, pr_errors.PRTransient):
    """Server/network error worth retrying with backoff (5xx, timeouts)."""


_PR_QUERY = """
query($owner:String!, $repo:String!, $number:Int!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      title url state merged isDraft headRefOid
      mergedBy { login }
      mergeable
      labels(first:50) { nodes { name } }  # bounded slice: 50 is a generous cap; not paginated
      reviewRequests(first:50) { nodes { requestedReviewer {  # bounded slice: 50 pending requests; not paginated
        __typename ... on User { login } ... on Team { name } } } }
      reviews(first:100) { pageInfo { hasNextPage endCursor } nodes { id state author { login } body url } }
      reviewThreads(first:100) { pageInfo { hasNextPage endCursor } nodes {
        id isResolved isOutdated path line startLine
        comments(first:100) { nodes {  # bounded slice (bumped 50->100): per-thread comments; not paginated
          id author { login } body url diffHunk path line startLine originalLine originalStartLine } }
      } }
      comments(first:100) { pageInfo { hasNextPage endCursor } nodes { id author { login } body url } }
      commits(last:1) { nodes { commit { oid statusCheckRollup { state contexts(first:100) {
        pageInfo { hasNextPage endCursor }
        nodes {
        __typename
        ... on CheckRun { id name status conclusion detailsUrl title summary }
        ... on StatusContext { id context state targetUrl description }
      } } } } } }
      timelineItems(
        last:50  # bounded slice: most-recent 50 timeline events; not paginated
        itemTypes:[LABELED_EVENT, UNLABELED_EVENT, REVIEW_REQUESTED_EVENT, REVIEW_REQUEST_REMOVED_EVENT, READY_FOR_REVIEW_EVENT, CONVERT_TO_DRAFT_EVENT, HEAD_REF_FORCE_PUSHED_EVENT]
      ) { nodes {
        __typename
        ... on LabeledEvent { id label { name } }
        ... on UnlabeledEvent { id label { name } }
        ... on ReviewRequestedEvent { id requestedReviewer {
          __typename ... on User { login } ... on Bot { login } ... on Mannequin { login } ... on Team { name } ... on EnterpriseTeam { name } } }
        ... on ReviewRequestRemovedEvent { id requestedReviewer {
          __typename ... on User { login } ... on Bot { login } ... on Mannequin { login } ... on Team { name } ... on EnterpriseTeam { name } } }
        ... on ReadyForReviewEvent { id actor { login } }
        ... on ConvertToDraftEvent { id actor { login } }
        ... on HeadRefForcePushedEvent { id beforeCommit { oid } afterCommit { oid } }
      } }
    }
  }
}
"""


# Follow-up queries used to drain a connection that overflowed its first-page slice.
# Each returns ONLY its connection (first:100, after:$cursor) with the SAME node
# sub-selection as _PR_QUERY, so merged nodes are shape-identical, plus the pageInfo
# needed to keep following the cursor.
_REVIEWS_PAGE_QUERY = """
query($owner:String!, $repo:String!, $number:Int!, $cursor:String!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      reviews(first:100, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id state author { login } body url }
      }
    }
  }
}
"""

_COMMENTS_PAGE_QUERY = """
query($owner:String!, $repo:String!, $number:Int!, $cursor:String!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      comments(first:100, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id author { login } body url }
      }
    }
  }
}
"""

_REVIEW_THREADS_PAGE_QUERY = """
query($owner:String!, $repo:String!, $number:Int!, $cursor:String!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      reviewThreads(first:100, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id isResolved isOutdated path line startLine
          comments(first:100) { nodes {
            id author { login } body url diffHunk path line startLine originalLine originalStartLine } }
        }
      }
    }
  }
}
"""

_CONTEXTS_PAGE_QUERY = """
query($owner:String!, $repo:String!, $number:Int!, $cursor:String!) {
  repository(owner:$owner, name:$repo) {
    pullRequest(number:$number) {
      commits(last:1) { nodes { commit { statusCheckRollup { contexts(first:100, after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          __typename
          ... on CheckRun { id name status conclusion detailsUrl title summary }
          ... on StatusContext { id context state targetUrl description }
        }
      } } } } }
    }
  }
}
"""

# Each unbounded top-level connection drains the same way: name -> its page query.
_TOP_LEVEL_PAGE_QUERIES: dict[str, str] = {
    "reviews": _REVIEWS_PAGE_QUERY,
    "comments": _COMMENTS_PAGE_QUERY,
    "reviewThreads": _REVIEW_THREADS_PAGE_QUERY,
}

# Pagination is bounded so a pathological PR (or a server that keeps claiming
# hasNextPage) can't loop forever. Hitting the cap is loud (stderr), never silent.
_MAX_PAGES = 20


def _pr_node(body: dict) -> dict:
    """The pullRequest node from a GraphQL response body (empty dict if absent)."""
    return ((body.get("data") or {}).get("repository") or {}).get("pullRequest") or {}


def _rollup_contexts(pr: dict) -> dict | None:
    """The head commit's check-rollup `contexts` connection on a pr node, or None."""
    commits = (pr.get("commits") or {}).get("nodes") or []
    if not commits:
        return None
    rollup = (commits[0].get("commit") or {}).get("statusCheckRollup")
    if not rollup:
        return None
    return rollup.get("contexts")


def api_base() -> str:
    return os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")


def graphql_url() -> str:
    return os.environ.get("GITHUB_GRAPHQL_URL", f"{api_base()}/graphql")


async def _retry_transient(
    fetch: Callable[[], Awaitable[dict]],
    *,
    attempts: int = _FETCH_ATTEMPTS,
    base_delay: float = _FETCH_BASE_DELAY,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict:
    """Call `fetch` up to `attempts` times, retrying ONLY on GitHubTransient with
    exponential backoff. Any other exception (GitHubAuthError / GitHubNotFound /
    GitHubRateLimited, ...) propagates immediately. After the final failed attempt
    the last GitHubTransient is re-raised. `sleep` is injectable so the retry loop
    is testable without httpx or real delays."""
    last: GitHubTransient | None = None
    for attempt in range(attempts):
        try:
            return await fetch()
        except GitHubTransient as exc:
            last = exc
            if attempt + 1 >= attempts:
                break
            await sleep(base_delay * 2**attempt)
    assert last is not None  # only reachable after a GitHubTransient was caught
    raise last


class GitHubClient:
    def __init__(
        self,
        token: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._token = token if token is not None else os.environ.get("GITHUB_TOKEN")
        # A test seam: when set, httpx routes through this transport instead of the
        # network. Production constructs GitHubClient() with no transport.
        self._transport = transport
        self.rate_limit_remaining: int | None = None
        self.rate_limit_reset: float | None = None

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _update_rate_limit(self, headers: httpx.Headers) -> None:
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        if remaining and remaining.lstrip("-").isdigit():
            self.rate_limit_remaining = int(remaining)
        if reset and reset.isdigit():
            self.rate_limit_reset = float(reset)

    def should_throttle(self, threshold: int = 50) -> float | None:
        """If the remaining budget is low, the epoch to wait until; else None."""
        if (
            self.rate_limit_remaining is not None
            and self.rate_limit_remaining <= threshold
            and self.rate_limit_reset
        ):
            return self.rate_limit_reset
        return None

    @staticmethod
    def _reset_at(headers: httpx.Headers) -> float:
        reset = headers.get("X-RateLimit-Reset")
        if reset and reset.isdigit():
            return float(reset)
        retry_after = headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return time.time() + float(retry_after)
        return time.time() + 60.0

    def _classify_http(self, resp: httpx.Response) -> None:
        if resp.status_code < 400:
            return
        code = resp.status_code
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if code in (403, 429) and (remaining == "0" or resp.headers.get("Retry-After")):
            raise GitHubRateLimited(
                self._reset_at(resp.headers), f"rate limited (HTTP {code})"
            )
        if code == 401:
            raise GitHubAuthError("unauthorized (401): token invalid or missing")
        if code == 403:
            raise GitHubAuthError("forbidden (403): token lacks access")
        if code == 404:
            raise GitHubNotFound("not found (404)")
        if 500 <= code < 600:
            raise GitHubTransient(f"server error (HTTP {code})")
        raise GitHubTransient(f"unexpected HTTP {code}")

    @staticmethod
    def _classify_graphql(body: dict) -> None:
        errors = body.get("errors")
        if not errors:
            return
        types = {e.get("type") for e in errors}
        message = "; ".join(e.get("message", "") for e in errors if e.get("message"))[
            :300
        ]
        if "NOT_FOUND" in types:
            raise GitHubNotFound(message or "not found")
        if "FORBIDDEN" in types:
            raise GitHubAuthError(message or "forbidden")
        if "RATE_LIMITED" in types:
            raise GitHubRateLimited(time.time() + 60.0, message or "rate limited")
        raise GitHubTransient(message or "graphql error")

    async def fetch_pr(self, owner: str, repo: str, number: int) -> dict:
        """Return the GraphQL pullRequest node, or raise a classified GitHubError.

        A single fetch (_fetch_pr_once) — which itself may span several pages on a very
        active PR — is retried in-poll on GitHubTransient only; other classified errors
        propagate immediately, so a transient blip on any page retries the whole fetch."""
        return await _retry_transient(lambda: self._fetch_pr_once(owner, repo, number))

    async def _post(self, query: str, variables: dict) -> dict:
        """One POST + classify; returns the parsed JSON body or raises a classified
        GitHubError. Rate-limit headers are updated on every response."""
        payload = {"query": query, "variables": variables}
        try:
            async with httpx.AsyncClient(
                transport=self._transport, timeout=httpx.Timeout(20.0)
            ) as client:
                resp = await client.post(
                    graphql_url(), headers=self._headers(), json=payload
                )
        except httpx.HTTPError as exc:
            raise GitHubTransient(f"network error: {exc}") from exc
        self._update_rate_limit(resp.headers)
        self._classify_http(resp)
        try:
            body = resp.json()
        except ValueError as exc:
            raise GitHubTransient(f"non-JSON response: {exc}") from exc
        self._classify_graphql(body)
        return body

    async def _drain(
        self,
        connection: dict,
        query: str,
        variables: dict,
        extract: Callable[[dict], dict | None],
        *,
        label: str,
    ) -> list[dict]:
        """Follow `connection`'s pageInfo cursor, returning the nodes from every
        follow-up page (the caller appends them to the first page's nodes). Each page
        is a fresh POST classified exactly like the first; `extract(body)` pulls this
        connection's dict out of a page response (its path differs for the nested check
        rollup). Bounded by _MAX_PAGES — hitting the cap prints to stderr rather than
        silently truncating."""
        extra: list[dict] = []
        page_info = connection.get("pageInfo") or {}
        pages = 0
        while page_info.get("hasNextPage"):
            if pages >= _MAX_PAGES:
                print(
                    f"subspace: {label} pagination hit _MAX_PAGES={_MAX_PAGES} "
                    "cap; dropping later pages",
                    file=sys.stderr,
                )
                break
            body = await self._post(
                query, {**variables, "cursor": page_info.get("endCursor")}
            )
            page_conn = extract(body) or {}
            extra.extend(page_conn.get("nodes") or [])
            page_info = page_conn.get("pageInfo") or {}
            pages += 1
        return extra

    async def _fetch_pr_once(self, owner: str, repo: str, number: int) -> dict:
        """One fetch + classify attempt, draining any of the four unbounded
        connections that overflowed its first page so snapshot_from_graphql (which
        reads `nodes`) sees every page merged. Raises a classified GitHubError on
        failure."""
        variables = {"owner": owner, "repo": repo, "number": number}
        body = await self._post(_PR_QUERY, variables)
        pr = ((body.get("data") or {}).get("repository") or {}).get("pullRequest")
        if pr is None:
            raise GitHubNotFound(f"{owner}/{repo}#{number} not found")
        # Top-level connections: merge later pages' nodes into the first page in place.
        for name, page_query in _TOP_LEVEL_PAGE_QUERIES.items():
            connection = pr.get(name)
            if connection and (connection.get("pageInfo") or {}).get("hasNextPage"):
                connection["nodes"] = (
                    connection.get("nodes") or []
                ) + await self._drain(
                    connection,
                    page_query,
                    variables,
                    lambda b, n=name: _pr_node(b).get(n),
                    label=f"{owner}/{repo}#{number} {name}",
                )
        # The check rollup's contexts connection is nested under the head commit.
        contexts = _rollup_contexts(pr)
        if contexts and (contexts.get("pageInfo") or {}).get("hasNextPage"):
            contexts["nodes"] = (contexts.get("nodes") or []) + await self._drain(
                contexts,
                _CONTEXTS_PAGE_QUERY,
                variables,
                lambda b: _rollup_contexts(_pr_node(b)),
                label=f"{owner}/{repo}#{number} contexts",
            )
        return pr
