# vim: filetype=python
"""Provider-agnostic PR-fetch error taxonomy, shared by every PR client.

The server's tracker loop recovers from fetch failures by *class* (not-found ->
terminal pr_gone; rate-limited -> defer to reset; auth -> one-time notice + long
backoff; transient -> back off and retry). Those semantics are identical whether the
PR lives on GitHub or a Gitea/Forgejo instance, so the classes live here and both
``github_client`` and ``forgejo_client`` raise them. The server catches these base
classes, so it stays provider-agnostic and a third provider needs no loop change.

``github_client`` keeps its historical ``GitHub*`` names as thin subclasses of these
bases (so existing imports/tests are unchanged); ``forgejo_client`` defines ``Forgejo*``
subclasses. ``PRRateLimited`` carries ``reset_at`` (epoch seconds to wait until), which
both providers populate from their rate-limit headers.

stdlib only (imported by both the asyncio server and the PR clients).
"""


class PRError(Exception):
    """Base for classified PR-fetch failures (any provider)."""


class PRAuthError(PRError):
    """Bad/insufficient credentials (401/403 non-rate-limit, GraphQL FORBIDDEN)."""


class PRNotFound(PRError):
    """The PR/repo does not exist or the token can't see it (404, GraphQL NOT_FOUND)."""


class PRRateLimited(PRError):
    """Rate limit hit; ``reset_at`` is the epoch seconds to wait until."""

    def __init__(self, reset_at: float, message: str = "rate limited") -> None:
        super().__init__(message)
        self.reset_at = reset_at


class PRTransient(PRError):
    """Server/network error worth retrying with backoff (5xx, timeouts)."""
