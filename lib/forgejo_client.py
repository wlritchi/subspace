# vim: filetype=python
"""Async Forgejo/Gitea client for PR monitoring — the REST parallel to github_client.

Forgejo/Gitea exposes a Gitea-style REST API (there is NO GraphQL), so one poll fans
out to a handful of GETs instead of a single query:

  GET /repos/{o}/{r}/pulls/{n}                       PR core
  GET /repos/{o}/{r}/pulls/{n}/reviews               reviews (paginated)
  GET /repos/{o}/{r}/pulls/{n}/reviews/{id}/comments per-review inline comments
  GET /repos/{o}/{r}/issues/{n}/comments             conversation comments (paginated)
  GET /repos/{o}/{r}/commits/{sha}/status            combined commit status

fetch_pr returns those pieces as a raw dict; pr_monitor.snapshot_from_forgejo() maps
the Gitea field names onto the transport-agnostic snapshot the diff engine consumes.
Pagination is Gitea-style (?page=&limit=), not GraphQL cursors. Failures are classified
into the shared pr_errors taxonomy (auth / not-found / rate-limited / transient) so the
server's tracker loop recovers identically for GitHub and Forgejo.

Reads FORGEJO_TOKEN and the instance base URL (constructor arg, else FORGEJO_API_URL).
Gitea auth is the `token <PAT>` header scheme (NOT `Bearer`). The instance URL may be
given with or without a trailing /api/v1 — both are accepted.
"""

import asyncio
import os
import sys
import time
from collections.abc import Awaitable, Callable

import httpx

import pr_errors

_USER_AGENT = "subspace"

# Mirror github_client's in-poll retry: a brief blip (5xx / network) is retried with
# exponential backoff, ONLY on the transient class; auth/not-found/rate-limited
# propagate immediately (they won't clear in seconds).
_FETCH_ATTEMPTS = 3
_FETCH_BASE_DELAY = 1.0

# Gitea list endpoints paginate with ?page=&limit=. Keep the page generous and bound
# the page count so a pathological PR can't loop forever (loud on stderr, never silent).
_PAGE_LIMIT = 50
_MAX_PAGES = 20


class ForgejoError(pr_errors.PRError):
    """Base for classified Forgejo/Gitea failures (subclasses the shared taxonomy)."""


class ForgejoAuthError(ForgejoError, pr_errors.PRAuthError):
    """Bad/insufficient credentials (401, or 403 without rate-limit headers)."""


class ForgejoNotFound(ForgejoError, pr_errors.PRNotFound):
    """The PR/repo does not exist or the token can't see it (404)."""


class ForgejoRateLimited(ForgejoError, pr_errors.PRRateLimited):
    """Rate limit hit; inherits PRRateLimited.__init__(reset_at, message)."""


class ForgejoTransient(ForgejoError, pr_errors.PRTransient):
    """Server/network error worth retrying with backoff (5xx, timeouts)."""


async def _retry_transient(
    fetch: Callable[[], Awaitable[dict]],
    *,
    attempts: int = _FETCH_ATTEMPTS,
    base_delay: float = _FETCH_BASE_DELAY,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict:
    """Call `fetch` up to `attempts` times, retrying ONLY on ForgejoTransient with
    exponential backoff. Any other classified error propagates immediately. `sleep` is
    injectable so the retry loop is testable without httpx or real delays."""
    last: ForgejoTransient | None = None
    for attempt in range(attempts):
        try:
            return await fetch()
        except ForgejoTransient as exc:
            last = exc
            if attempt + 1 >= attempts:
                break
            await sleep(base_delay * 2**attempt)
    assert last is not None  # only reachable after a ForgejoTransient was caught
    raise last


def _api_root(base_url: str | None) -> str:
    """Normalize an instance URL to its /api/v1 root, accepting either form."""
    root = (base_url or os.environ.get("FORGEJO_API_URL") or "").rstrip("/")
    if not root:
        return ""
    if root.endswith("/api/v1"):
        return root
    return f"{root}/api/v1"


class ForgejoClient:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = _api_root(base_url)
        self._token = token if token is not None else os.environ.get("FORGEJO_TOKEN")
        # A test seam: when set, httpx routes through this transport instead of the
        # network. Production constructs ForgejoClient() with no transport.
        self._transport = transport
        self.rate_limit_remaining: int | None = None
        self.rate_limit_reset: float | None = None

    @property
    def configured(self) -> bool:
        """True when an instance URL is set (a token may still be optional for public
        repos). The server uses this to reject forgejo subscriptions when unconfigured."""
        return bool(self.base_url)

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}
        if self._token:
            # Gitea/Forgejo scheme is `token <PAT>`, not `Bearer <PAT>`.
            headers["Authorization"] = f"token {self._token}"
        return headers

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

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
        if code == 429 or (
            code == 403 and (remaining == "0" or resp.headers.get("Retry-After"))
        ):
            raise ForgejoRateLimited(
                self._reset_at(resp.headers), f"rate limited (HTTP {code})"
            )
        if code == 401:
            raise ForgejoAuthError("unauthorized (401): token invalid or missing")
        if code == 403:
            raise ForgejoAuthError("forbidden (403): token lacks access")
        if code == 404:
            raise ForgejoNotFound("not found (404)")
        if 500 <= code < 600:
            raise ForgejoTransient(f"server error (HTTP {code})")
        raise ForgejoTransient(f"unexpected HTTP {code}")

    async def _get(
        self, client: httpx.AsyncClient, path: str, params: dict | None = None
    ) -> httpx.Response:
        """One GET on the caller's shared client + classify; returns the response or
        raises a classified error. Rate-limit headers are updated on every response."""
        try:
            resp = await client.get(
                self._url(path), headers=self._headers(), params=params or {}
            )
        except httpx.HTTPError as exc:
            raise ForgejoTransient(f"network error: {exc}") from exc
        self._update_rate_limit(resp.headers)
        self._classify_http(resp)
        return resp

    @staticmethod
    def _json(resp: httpx.Response):
        try:
            return resp.json()
        except ValueError as exc:
            raise ForgejoTransient(f"non-JSON response: {exc}") from exc

    async def _get_obj(self, client: httpx.AsyncClient, path: str) -> dict:
        return self._json(await self._get(client, path)) or {}

    async def _get_obj_optional(self, client: httpx.AsyncClient, path: str) -> dict:
        """Like _get_obj but a 404 on a sub-resource yields {} instead of terminating
        the PR — only the top-level pulls fetch treats 404 as 'PR gone'."""
        try:
            return await self._get_obj(client, path)
        except ForgejoNotFound:
            return {}

    async def _get_list(self, client: httpx.AsyncClient, path: str) -> list[dict]:
        """Follow ?page=&limit= pagination, returning every page's items merged. Stops
        when a short page is returned or _MAX_PAGES is hit (loud on the cap, never a
        silent truncation)."""
        items: list[dict] = []
        page = 1
        while True:
            if page > _MAX_PAGES:
                print(
                    f"subspace: forgejo {path} pagination hit _MAX_PAGES="
                    f"{_MAX_PAGES} cap; dropping later pages",
                    file=sys.stderr,
                )
                break
            batch = self._json(
                await self._get(client, path, {"page": page, "limit": _PAGE_LIMIT})
            )
            if not isinstance(batch, list) or not batch:
                break
            items.extend(batch)
            if len(batch) < _PAGE_LIMIT:
                break
            page += 1
        return items

    async def _get_list_optional(
        self, client: httpx.AsyncClient, path: str
    ) -> list[dict]:
        """Like _get_list but a 404 on a sub-resource yields [] instead of terminating
        the PR — only the top-level pulls fetch treats 404 as 'PR gone'."""
        try:
            return await self._get_list(client, path)
        except ForgejoNotFound:
            return []

    async def fetch_pr(self, owner: str, repo: str, number: int) -> dict:
        """Return the raw Gitea pieces for one PR, or raise a classified error.

        A single fetch (which spans several REST GETs over ONE pooled httpx client)
        is retried in-poll on ForgejoTransient only; other classified errors propagate
        immediately."""
        return await _retry_transient(lambda: self._fetch_pr_once(owner, repo, number))

    async def _fetch_pr_once(self, owner: str, repo: str, number: int) -> dict:
        base = f"/repos/{owner}/{repo}"
        # One pooled AsyncClient for the whole fan-out (pulls + reviews + per-review
        # comments + issue comments + status): connections are reused across the GETs
        # instead of opening a fresh client per request.
        async with httpx.AsyncClient(
            transport=self._transport, timeout=httpx.Timeout(20.0)
        ) as client:
            # The pulls fetch is the one that decides existence: its 404 is terminal.
            pr = await self._get_obj(client, f"{base}/pulls/{number}")
            if not pr:
                raise ForgejoNotFound(f"{owner}/{repo}#{number} not found")
            reviews = await self._get_list_optional(
                client, f"{base}/pulls/{number}/reviews"
            )
            review_comments: list[dict] = []
            for review in reviews:
                rid = review.get("id")
                # Only reviews that carry inline comments cost an extra request.
                if rid is not None and review.get("comments_count"):
                    review_comments.extend(
                        await self._get_list_optional(
                            client, f"{base}/pulls/{number}/reviews/{rid}/comments"
                        )
                    )
            issue_comments = await self._get_list_optional(
                client, f"{base}/issues/{number}/comments"
            )
            statuses: list[dict] = []
            head_sha = (pr.get("head") or {}).get("sha")
            if head_sha:
                # Swallow a 404 here (like the other sub-resources): a missing/absent
                # status for the head commit means "no statuses", NOT that the PR is
                # gone. Only the pulls fetch above decides existence.
                combined = await self._get_obj_optional(
                    client, f"{base}/commits/{head_sha}/status"
                )
                statuses = combined.get("statuses") or []
            return {
                "pr": pr,
                "reviews": reviews,
                "review_comments": review_comments,
                "issue_comments": issue_comments,
                "statuses": statuses,
            }
