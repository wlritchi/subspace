#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""
subspace gateway

An internet-facing MCP server (Streamable HTTP) for remote agents that cannot run
the stdio relay. Each request carries an ``ssp_`` credential as a bearer token.
The gateway holds no secret of its own: it opens a WebSocket to the subspace
server with the agent's token, attaches to the agent's session, and turns tool
calls into protocol frames. Notifications are pulled with catch_up and
wait_for_messages. See ../docs/specs/2026-10-10-remote-agent-mcp.md.

A UI connector that supports only OAuth signs in on a page where the user
pastes an ``ssp_`` token. The access token that it gets is that token. This
needs SUBSPACE_GATEWAY_PUBLIC_URL.

A request names its session with the ``session`` or ``handle`` URL parameter, or
the ``X-Subspace-Session`` or ``X-Subspace-Handle`` header. A client that keeps
an MCP session (protocol versions before 2026-07-28) can call attach or
register_agent instead.

Config (env):  SUBSPACE_GATEWAY_HOST (default 127.0.0.1)
               SUBSPACE_GATEWAY_PORT (default 8138)
               SUBSPACE_CORE_URL (default ws://127.0.0.1:8137)
               SUBSPACE_GATEWAY_ALLOWED_HOSTS (comma-separated Host values)
               SUBSPACE_GATEWAY_ALLOWED_ORIGINS (comma-separated; default none)
               SUBSPACE_GATEWAY_IDLE_SECONDS (default 600)
               SUBSPACE_GATEWAY_LINGER_SECONDS (default 5)
               SUBSPACE_GATEWAY_WAIT_MAX_SECONDS (default 30)
               SUBSPACE_GATEWAY_CORE_RETRY_SECONDS (default 10)
               SUBSPACE_GATEWAY_MAX_BODY_BYTES (default 65536)
               SUBSPACE_GATEWAY_SESSIONS_PER_CREDENTIAL (default 20)
               SUBSPACE_GATEWAY_AUTH_FAILURES_PER_MINUTE (default 20)
               SUBSPACE_GATEWAY_TOKEN_CACHE_SECONDS (default 60)
               SUBSPACE_GATEWAY_MAX_MCP_SESSIONS (default 1000)
               SUBSPACE_GATEWAY_TRUST_FORWARDED (default 0)
               SUBSPACE_GATEWAY_PUBLIC_URL (https://host; unset turns off OAuth)
               SUBSPACE_GATEWAY_OAUTH_REDIRECT_URIS (comma-separated; an entry
                   that ends in * is a prefix; loopback is always allowed)
"""

# /// script
# requires-python = ">=3.12"
# dependencies = ["mcp>=2.3,<3", "uvicorn", "websockets", "subspace-protocol"]
#
# [tool.uv.sources]
# subspace-protocol = { path = "../protocol", editable = true }
# ///

import asyncio
import functools
import hashlib
import html
import json
import logging
import os
import re
import secrets
import sys
import time
import urllib.parse
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import subspace_protocol as proto
import uvicorn
from mcp.server import MCPServer
from mcp.server.auth.handlers.authorize import AuthorizationHandler
from mcp.server.auth.handlers.metadata import (
    MetadataHandler,
    ProtectedResourceMetadataHandler,
)
from mcp.server.auth.handlers.token import TokenHandler
from mcp.server.auth.middleware.client_auth import ClientAuthenticator
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    TokenError,
    construct_redirect_uri,
)
from mcp.server.auth.routes import cors_middleware
from mcp.server.mcpserver import Context
from mcp.server.transport_security import (
    RequestBodyLimitMiddleware,
    TransportSecuritySettings,
)
from mcp.shared.auth import (
    InvalidRedirectUriError,
    OAuthClientInformationFull,
    OAuthClientMetadata,
    OAuthMetadata,
    OAuthToken,
    ProtectedResourceMetadata,
)
from mcp_types import ListToolsResult
from pydantic import AnyUrl, Field, ValidationError
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from starlette.routing import Route, request_response
from starlette.types import ASGIApp, Receive, Scope, Send
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

log = logging.getLogger("subspace-gateway")

# The token format that the gateway forwards. The master token is never accepted
# here: the gateway is internet-facing.
_TOKEN_RE = re.compile(r"^ssp_[a-z2-7]{12}_[A-Za-z0-9_-]{20,}\Z")
# How long a verdict on a token stays cached, by default.
TOKEN_CACHE_SECONDS = 60.0
# How long an MCP session (protocol versions before 2026-07-28) lives without
# requests; the SDK closes it then, and the gateway forgets its attach.
MCP_SESSION_IDLE_SECONDS = 1800.0
# How long to wait for the core to answer one request.
CORE_TIMEOUT_SECONDS = 10.0
# Default time to keep a session's WebSocket open after its last tool call ends.
# While the WebSocket is closed, the core counts the session as offline.
LINGER_SECONDS = 5.0
# Default time to try again while the core cannot be reached. A core restart
# that is shorter than this does not make a request fail.
CORE_RETRY_SECONDS = 10.0
# Limits for one catch_up batch, the same as the relay's.
CATCHUP_MAX_ITEMS = 25
CATCHUP_MAX_CHARS = 50_000

_PR_REF_RE = re.compile(
    r"^(?:(?P<instance>[a-z0-9](?:[a-z0-9-]*[a-z0-9])?):)?"
    r"(?P<owner>[^/\s#:]+)/(?P<repo>[^/\s#]+)#(?P<number>\d+)\Z"
)


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return default


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _credential_id(token: str) -> str:
    """The non-secret part of a token, for logs."""
    return token[len("ssp_") : len("ssp_") + 12]


class GatewayError(Exception):
    """A failure to show to the agent as the tool result."""


# --------------------------------------------------------------------------- #
# one attached session: a WebSocket to the core
# --------------------------------------------------------------------------- #


class CoreSession:
    """A subspace session that remote agents use through this gateway.

    Holds a WebSocket to the core with the agent's token, the notify frames that
    the core sent and the agent did not pull yet, and a copy of the batch that
    the last pull returned. A pull acks its batch at once, so that the core
    knows what the agent got. An agent that did not get a pull's result asks
    for the batch again with ``redeliver``. The WebSocket is open only while a
    tool call uses the session, and for ``linger_seconds`` after; the core
    sends unacked items again after a reconnect.
    """

    def __init__(
        self,
        core_url: str,
        token: str,
        spec: str,
        on_revoked: Callable[[str], None],
        retry_seconds: float = 0.0,
        linger_seconds: float = 0.0,
    ) -> None:
        self.core_url: str = core_url
        self.token: str = token
        self.retry_seconds: float = retry_seconds
        self.linger_seconds: float = linger_seconds
        # Tool calls in progress that use this session.
        self.calls: int = 0
        self._linger: asyncio.TimerHandle | None = None
        # The session id that the session attaches with, or "@<handle>".
        self.spec: str = spec
        # The session id that the core attached to. A reconnect uses it, so a
        # "@<handle>" session stays the same session.
        self.session_id: str | None = None
        self._on_revoked = on_revoked
        self.ws: ClientConnection | None = None
        self.buffer: list[dict] = []
        # The last pull's result, already acked; kept for ``redeliver``.
        self.batch: list[dict] = []
        self.connections: int = 1
        self.last_used: float = time.monotonic()
        self.arrived = asyncio.Event()
        self._next_req: int = 0
        self._replies: dict[int, asyncio.Future[dict]] = {}
        self._lock = asyncio.Lock()
        self._recv_task: asyncio.Task[None] | None = None

    async def ensure_connected(self) -> None:
        self.last_used = time.monotonic()
        async with self._lock:
            if self.ws is not None:
                return
            ws = await _open_core(self.core_url, self.token, self.retry_seconds)
            self.ws = ws
            self._recv_task = asyncio.create_task(self._recv(ws))
            await ws.send(
                json.dumps(
                    {"type": proto.REGISTER, "session_id": self.session_id or self.spec}
                )
            )
            reply = await self._round_trip(ws, {"type": proto.WHOAMI})
            if reply.get("type") == proto.ERROR:
                await ws.close()
                raise GatewayError(f"Could not attach: {reply.get('error')}")
            self.session_id = reply.get("session_id") or self.session_id

    async def _recv(self, ws: ClientConnection) -> None:
        try:
            async for raw in ws:
                frame = json.loads(raw)
                if frame.get("type") == proto.NOTIFY:
                    self._buffer(frame)
                    continue
                req_id = frame.get("req_id")
                future = self._replies.pop(req_id, None) if req_id is not None else None
                if future is not None and not future.done():
                    future.set_result(frame)
        except ConnectionClosed:
            pass
        finally:
            if self.ws is ws:
                self.ws = None
            if ws.close_code == proto.UNAUTHORIZED_CLOSE_CODE:
                # Revoked: drop what is buffered, so that it cannot be pulled.
                self.buffer.clear()
                self.batch.clear()
                self._on_revoked(self.token)
            for future in self._replies.values():
                if not future.done():
                    future.set_exception(GatewayError("the subspace server closed"))
            self._replies.clear()

    def _buffer(self, frame: dict) -> None:
        meta = frame.get("meta") or {}
        if "connections" in meta:
            self.connections = int(meta["connections"])
        if meta.get("kind") == "multiple_connections":
            # A notice about the count, not content: keep only the count.
            self.connections = int(meta.get("connections", 1))
            self._ack_now(frame["id"])
            return
        notify_id = frame["id"]
        if any(f["id"] == notify_id for f in self.batch):
            # Sent again after a reconnect: the ack did not reach the core.
            self._ack_now(notify_id)
            return
        if any(f["id"] == notify_id for f in self.buffer):
            return  # sent again after a reconnect
        self.buffer.append(frame)
        self.arrived.set()

    def _ack_now(self, notify_id: str) -> None:
        ws = self.ws
        if ws is not None:
            asyncio.ensure_future(
                _send_quietly(ws, {"type": proto.ACK, "id": notify_id})
            )

    async def request(self, frame: dict[str, Any]) -> dict:
        await self.ensure_connected()
        ws = self.ws
        if ws is None:
            raise GatewayError("the subspace server closed")
        return await self._round_trip(ws, frame)

    async def _round_trip(self, ws: ClientConnection, frame: dict[str, Any]) -> dict:
        self._next_req += 1
        req_id = self._next_req
        future: asyncio.Future[dict] = asyncio.get_running_loop().create_future()
        self._replies[req_id] = future
        try:
            await ws.send(json.dumps({**frame, "req_id": req_id}))
            return await asyncio.wait_for(future, CORE_TIMEOUT_SECONDS)
        except ConnectionClosed as exc:
            raise GatewayError("the subspace server closed") from exc
        except TimeoutError as exc:
            raise GatewayError("the subspace server did not answer") from exc
        finally:
            self._replies.pop(req_id, None)

    def forget_batch(self) -> int:
        """Drop the copy of the last batch. Returns how many items it had."""
        count = len(self.batch)
        self.batch = []
        return count

    async def pull(self, max_items: int, redeliver: bool = False) -> list[dict]:
        """Take the next batch from the buffer and ack it. With ``redeliver``,
        start the batch with the last one again."""
        await self.ensure_connected()
        taken: list[dict] = list(self.batch) if redeliver else []
        chars = sum(len(f.get("content", "")) for f in taken)
        new: list[dict] = []
        while self.buffer and len(taken) + len(new) < max_items:
            size = len(self.buffer[0].get("content", ""))
            if (taken or new) and chars + size > CATCHUP_MAX_CHARS:
                break
            new.append(self.buffer.pop(0))
            chars += size
        if not self.buffer:
            self.arrived.clear()
        # Keep the batch before the first await, so that a cancelled call does
        # not lose the items.
        self.batch = taken + new
        batch = self.batch
        ws = self.ws
        if ws is not None:
            for frame in new:
                await _send_quietly(ws, {"type": proto.ACK, "id": frame["id"]})
        return batch

    def begin_call(self) -> None:
        self.calls += 1
        if self._linger is not None:
            self._linger.cancel()
            self._linger = None

    def end_call(self) -> None:
        self.calls -= 1
        if self.calls == 0 and self.ws is not None:
            self._linger = asyncio.get_running_loop().call_later(
                self.linger_seconds,
                lambda: asyncio.ensure_future(self._disconnect_if_idle()),
            )

    async def _disconnect_if_idle(self) -> None:
        """Close the WebSocket if no call uses the session. The session state
        stays, and the next call connects again."""
        self._linger = None
        async with self._lock:
            ws = self.ws
            if self.calls == 0 and ws is not None:
                # Clear it first, so that the next call connects again and does
                # not use a WebSocket that is closing.
                self.ws = None
                await ws.close()

    async def close(self) -> None:
        if self._linger is not None:
            self._linger.cancel()
            self._linger = None
        ws = self.ws
        self.ws = None
        if ws is not None:
            await ws.close()
        if self._recv_task is not None:
            self._recv_task.cancel()


async def _send_quietly(ws: ClientConnection, frame: dict) -> None:
    try:
        await ws.send(json.dumps(frame))
    except ConnectionClosed:
        pass


async def _open_core(
    core_url: str, token: str, retry_seconds: float = 0.0
) -> ClientConnection:
    """Connect to the core. Try again until ``retry_seconds`` pass."""
    deadline = time.monotonic() + retry_seconds
    delay = 0.2
    while True:
        try:
            return await connect(
                core_url,
                additional_headers={proto.AUTH_HEADER: proto.auth_header_value(token)},
                open_timeout=CORE_TIMEOUT_SECONDS,
            )
        except (OSError, InvalidStatus, TimeoutError) as exc:
            if time.monotonic() + delay > deadline:
                # The details name internal addresses: log them, do not return
                # them.
                log.warning("cannot reach the subspace server: %s", exc)
                raise GatewayError("the subspace server is not reachable") from exc
        await asyncio.sleep(delay)
        delay = min(delay * 2, 1.0)


# --------------------------------------------------------------------------- #
# gateway state
# --------------------------------------------------------------------------- #


@dataclass
class TokenVerdict:
    whoami: dict | None  # None means the core refused the token
    checked_at: float


class Gateway:
    def __init__(self) -> None:
        self.core_url: str = _env_str("SUBSPACE_CORE_URL", "ws://127.0.0.1:8137")
        self.idle_seconds: float = _env_float("SUBSPACE_GATEWAY_IDLE_SECONDS", 600.0)
        self.wait_max: float = _env_float("SUBSPACE_GATEWAY_WAIT_MAX_SECONDS", 30.0)
        self.core_retry_seconds: float = _env_float(
            "SUBSPACE_GATEWAY_CORE_RETRY_SECONDS", CORE_RETRY_SECONDS
        )
        self.linger_seconds: float = _env_float(
            "SUBSPACE_GATEWAY_LINGER_SECONDS", LINGER_SECONDS
        )
        self.sessions_per_credential: int = int(
            _env_float("SUBSPACE_GATEWAY_SESSIONS_PER_CREDENTIAL", 20)
        )
        self.auth_failures_per_minute: float = _env_float(
            "SUBSPACE_GATEWAY_AUTH_FAILURES_PER_MINUTE", 20.0
        )
        self.token_cache_seconds: float = _env_float(
            "SUBSPACE_GATEWAY_TOKEN_CACHE_SECONDS", TOKEN_CACHE_SECONDS
        )
        self.trust_forwarded: bool = os.environ.get(
            "SUBSPACE_GATEWAY_TRUST_FORWARDED", ""
        ) not in ("", "0")
        # (token hash, spec) -> session
        self.sessions: dict[tuple[str, str], CoreSession] = {}
        # (token hash, MCP session id) -> (spec, last use), for clients that keep
        # MCP sessions
        self.attached: dict[tuple[str, str], tuple[str, float]] = {}
        self.verdicts: dict[str, TokenVerdict] = {}
        # client address -> (failures left, last refill)
        self.failures: dict[str, tuple[float, float]] = {}

    async def verify(self, token: str) -> dict | None:
        """The core's whoami for a token with no session, or None if the core
        refuses the token. Cached for token_cache_seconds."""
        key = _token_hash(token)
        now = time.monotonic()
        verdict = self.verdicts.get(key)
        if verdict is not None and now - verdict.checked_at < self.token_cache_seconds:
            return verdict.whoami
        deadline = now + self.core_retry_seconds
        while True:
            try:
                whoami = await self._whoami(token, deadline - time.monotonic())
                break
            except ConnectionClosed as exc:
                # The core refuses a token with this close code. Any other close
                # (for example, the core stops) is not a verdict: do not cache it.
                if (
                    exc.rcvd is not None
                    and exc.rcvd.code == proto.UNAUTHORIZED_CLOSE_CODE
                ):
                    whoami = None
                    break
                if time.monotonic() >= deadline:
                    raise GatewayError("the subspace server closed") from exc
                await asyncio.sleep(0.2)
        self.verdicts[key] = TokenVerdict(whoami, now)
        return whoami

    async def _whoami(self, token: str, retry_seconds: float) -> dict | None:
        ws = await _open_core(self.core_url, token, retry_seconds)
        try:
            await ws.send(json.dumps({"type": proto.WHOAMI, "req_id": 1}))
            reply = json.loads(await asyncio.wait_for(ws.recv(), CORE_TIMEOUT_SECONDS))
        except TimeoutError as exc:
            raise GatewayError("the subspace server did not answer") from exc
        finally:
            await ws.close()
        return reply if reply.get("type") == proto.WHOAMI_RESULT else None

    def known_valid(self, token: str) -> bool:
        """True if the core accepted the token within the cache time."""
        verdict = self.verdicts.get(_token_hash(token))
        return (
            verdict is not None
            and verdict.whoami is not None
            and time.monotonic() - verdict.checked_at < self.token_cache_seconds
        )

    def forget_token(self, token: str) -> None:
        """The core revoked the token: drop its verdict and its sessions."""
        key = _token_hash(token)
        self.verdicts.pop(key, None)
        for session_key in [k for k in self.sessions if k[0] == key]:
            session = self.sessions.pop(session_key)
            asyncio.ensure_future(session.close())
        for attach_key in [k for k in self.attached if k[0] == key]:
            del self.attached[attach_key]

    def attach(self, token: str, mcp_session: str, spec: str) -> None:
        self.attached[(_token_hash(token), mcp_session)] = (spec, time.monotonic())

    def attached_spec(self, token: str, mcp_session: str) -> str | None:
        key = (_token_hash(token), mcp_session)
        entry = self.attached.get(key)
        if entry is None:
            return None
        self.attached[key] = (entry[0], time.monotonic())
        return entry[0]

    def _failures_left(self, client: str, now: float) -> float:
        rate = self.auth_failures_per_minute
        left, last = self.failures.get(client, (rate, now))
        return min(rate, left + (now - last) * rate / 60.0)

    def blocked(self, client: str) -> bool:
        """True if ``client`` used up its failed authentications. The gateway
        then refuses it without asking the core."""
        return self._failures_left(client, time.monotonic()) < 1.0

    def record_failure(self, client: str) -> None:
        now = time.monotonic()
        self.failures[client] = (max(0.0, self._failures_left(client, now) - 1.0), now)

    def session(self, token: str, spec: str) -> CoreSession:
        key = (_token_hash(token), spec)
        existing = self.sessions.get(key)
        if existing is not None:
            return existing
        count = sum(1 for k in self.sessions if k[0] == key[0])
        if count >= self.sessions_per_credential:
            raise GatewayError(
                f"too many sessions for this credential (at most "
                f"{self.sessions_per_credential} at a time)"
            )
        created = CoreSession(
            self.core_url,
            token,
            spec,
            self.forget_token,
            self.core_retry_seconds,
            self.linger_seconds,
        )
        self.sessions[key] = created
        return created

    async def reap_idle(self) -> None:
        while True:
            await asyncio.sleep(min(30.0, max(1.0, self.idle_seconds / 4)))
            now = time.monotonic()
            for key, session in list(self.sessions.items()):
                if session.calls == 0 and now - session.last_used > self.idle_seconds:
                    del self.sessions[key]
                    await session.close()
            for key, verdict in list(self.verdicts.items()):
                if now - verdict.checked_at > self.token_cache_seconds:
                    del self.verdicts[key]
            for client, (_left, last) in list(self.failures.items()):
                if now - last > 60.0:  # refilled by now
                    del self.failures[client]
            for key, (_spec, last) in list(self.attached.items()):
                if now - last > MCP_SESSION_IDLE_SECONDS:
                    del self.attached[key]


GATEWAY = Gateway()


# --------------------------------------------------------------------------- #
# HTTP: authentication and health
# --------------------------------------------------------------------------- #


def _client_address(scope: Scope) -> str:
    if GATEWAY.trust_forwarded:
        # The last entry is the one that the trusted proxy added. The client
        # controls the entries before it.
        forwarded = [
            value.decode("latin-1")
            for name, value in scope.get("headers", [])
            if name == b"x-forwarded-for"
        ]
        if forwarded:
            return forwarded[-1].split(",")[-1].strip()
    client = scope.get("client")
    return client[0] if client else "unknown"


class AuthMiddleware:
    """Require a valid ``ssp_`` bearer token on every MCP request."""

    def __init__(self, app: ASGIApp, path: str) -> None:
        self.app = app
        self.path = path

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] != self.path:
            await self.app(scope, receive, send)
            return
        client = _client_address(scope)
        request = Request(scope)
        header = request.headers.get("authorization", "")
        token = header[len("Bearer ") :] if header.startswith("Bearer ") else ""
        # A blocked address still gets through with a token that the core
        # accepted recently, so failures from others on a shared address do not
        # lock out valid agents.
        if GATEWAY.blocked(client) and not GATEWAY.known_valid(token):
            await PlainTextResponse("too many failed attempts", status_code=429)(
                scope, receive, send
            )
            return
        whoami = None
        if _TOKEN_RE.match(token):
            try:
                whoami = await GATEWAY.verify(token)
            except GatewayError:
                await PlainTextResponse("service unavailable", status_code=503)(
                    scope, receive, send
                )
                return
        if whoami is None:
            GATEWAY.record_failure(client)
            log.info(
                "refused a token (credential %s)",
                _credential_id(token) if token.startswith("ssp_") else "-",
            )
            challenge = 'Bearer realm="subspace"'
            if OAUTH.enabled:
                challenge += f', resource_metadata="{OAUTH.resource_metadata_url}"'
            await PlainTextResponse(
                "unauthorized",
                status_code=401,
                headers={"WWW-Authenticate": challenge},
            )(scope, receive, send)
            return
        await self.app(scope, receive, send)


# --------------------------------------------------------------------------- #
# HTTP: OAuth (token paste)
# --------------------------------------------------------------------------- #

# The client ids that the gateway accepts. A client id grants nothing: the user
# pastes a credential on the login page, and the redirect URI must be allowed.
_CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}\Z")
OAUTH_SCOPE = "subspace"
# How long a login page stays valid, and how long an issued code stays valid.
LOGIN_SECONDS = 600.0
CODE_SECONDS = 300.0
# At most this many open logins and unredeemed codes. /authorize needs no
# credential, so the limit keeps memory bounded.
MAX_PENDING = 1000
# At most this many open logins from one client address, so that one address
# cannot fill the store.
MAX_LOGINS_PER_ADDRESS = 10
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")


class IssuedCode(AuthorizationCode):
    """An authorization code, and the credential that the user pasted for it."""

    credential: str = Field(repr=False)


@dataclass
class PendingLogin:
    client_id: str
    params: AuthorizationParams
    created: float
    address: str
    # sha256 of the browser cookie that opened the login page first. Only that
    # browser can complete the login.
    browser: str | None = None


class OAuthClient(OAuthClientInformationFull):
    """A public client. It can request any scope; the scope has no effect,
    because the pasted credential's account decides the permissions."""

    def validate_redirect_uri(self, redirect_uri: AnyUrl | None) -> AnyUrl:
        if redirect_uri is None:
            raise InvalidRedirectUriError("redirect_uri is required")
        if not OAUTH.redirect_allowed(str(redirect_uri)):
            log.info("refused an OAuth redirect URI: %s", redirect_uri)
            raise InvalidRedirectUriError(
                f"Redirect URI '{redirect_uri}' is not allowed by this gateway"
            )
        return redirect_uri

    def validate_scope(self, requested_scope: str | None) -> list[str] | None:
        return requested_scope.split() if requested_scope else None


@dataclass(frozen=True)
class RedirectPattern:
    """An allowed redirect URI, or with ``prefix``, a path prefix on one
    origin."""

    scheme: str
    netloc: str
    path: str
    prefix: bool
    exact: str = ""


# What a prefix pattern accepts after its path: plain path segments only, so
# that encoded separators, empty segments, and dot segments cannot reach other
# pages on the host.
_SAFE_REST_RE = re.compile(r"^[A-Za-z0-9_~-][A-Za-z0-9._~-]*(?:/[A-Za-z0-9._~-]+)*/?\Z")


def _parse_redirect_patterns(raw: list[str]) -> list[RedirectPattern]:
    """Allowed redirect URIs, https only. An entry that ends in ``*`` is a
    prefix, and must include a path after the host."""
    patterns: list[RedirectPattern] = []
    for entry in raw:
        prefix = entry.endswith("*")
        try:
            normal = str(AnyUrl(entry[:-1] if prefix else entry))
        except ValidationError:
            log.warning("ignored the redirect URI %r: not a URL", entry)
            continue
        parsed = urllib.parse.urlsplit(normal)
        if parsed.scheme != "https":
            log.warning("ignored the redirect URI %r: not https", entry)
            continue
        if prefix and (
            parsed.query
            or parsed.fragment
            or not urllib.parse.urlsplit(entry).path.startswith("/")
        ):
            log.warning("ignored the redirect URI prefix %r: no path", entry)
            continue
        patterns.append(
            RedirectPattern(parsed.scheme, parsed.netloc, parsed.path, prefix, normal)
        )
    return patterns


def _prefix_match(pattern: RedirectPattern, uri: str) -> bool:
    parsed = urllib.parse.urlsplit(uri)
    if (
        parsed.scheme != pattern.scheme
        or parsed.netloc != pattern.netloc
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith(pattern.path)
    ):
        return False
    rest = parsed.path[len(pattern.path) :]
    if rest.startswith("/") and not pattern.path.endswith("/"):
        rest = rest[1:]
    if rest == "":
        return True
    return bool(_SAFE_REST_RE.match(rest)) and not any(
        segment in (".", "..") for segment in rest.split("/")
    )


# The client address of the current request. The SDK calls authorize without
# the request, so WithAddress sets it.
_REQUEST_ADDRESS: ContextVar[str] = ContextVar("request_address", default="unknown")


class WithAddress:
    """ASGI wrapper that sets _REQUEST_ADDRESS. A class, because Starlette
    treats a plain function endpoint as a request handler."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        reset = _REQUEST_ADDRESS.set(_client_address(scope))
        try:
            await self.app(scope, receive, send)
        finally:
            _REQUEST_ADDRESS.reset(reset)


class OAuthServer:
    """An OAuth authorization server whose login page takes an ``ssp_`` token.

    The access token is the pasted token itself, so the gateway keeps no token
    state, and a credential that is revoked in accounts.toml stops working at
    once. Clients need no registration: any client id is a public client, and
    PKCE and the redirect URI allowlist protect the code.
    """

    def __init__(self) -> None:
        self.public_url: str = os.environ.get("SUBSPACE_GATEWAY_PUBLIC_URL", "").rstrip(
            "/"
        )
        self.redirect_patterns: list[RedirectPattern] = _parse_redirect_patterns(
            _env_list("SUBSPACE_GATEWAY_OAUTH_REDIRECT_URIS", [])
        )
        self.logins: dict[str, PendingLogin] = {}
        self.codes: dict[str, IssuedCode] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.public_url)

    @property
    def resource_url(self) -> str:
        return f"{self.public_url}/mcp"

    @property
    def resource_metadata_url(self) -> str:
        return f"{self.public_url}/.well-known/oauth-protected-resource/mcp"

    def redirect_allowed(self, uri: str) -> bool:
        parsed = urllib.parse.urlsplit(uri)
        if parsed.scheme == "http" and parsed.hostname in _LOOPBACK_HOSTS:
            # RFC 8252: a native client on this machine, on any port.
            return True
        for pattern in self.redirect_patterns:
            if pattern.prefix:
                if _prefix_match(pattern, uri):
                    return True
            elif uri == pattern.exact:
                return True
        return False

    def resource_allowed(self, resource: str) -> bool:
        """True if an RFC 8707 resource indicator names this gateway's /mcp.
        A query (for example ?handle=) is allowed."""
        parsed = urllib.parse.urlsplit(resource)
        if parsed.fragment:
            return False
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        return base.lower() == self.resource_url.lower()

    @property
    def secure(self) -> bool:
        return self.public_url.startswith("https://")

    @property
    def cookie_name(self) -> str:
        # __Host- needs Secure, so plain http (loopback tests) uses another name.
        return "__Host-subspace-login" if self.secure else "subspace-login"

    def _prune(self, now: float) -> None:
        for rid, login in list(self.logins.items()):
            if now - login.created > LOGIN_SECONDS:
                del self.logins[rid]
        for code, issued in list(self.codes.items()):
            if issued.expires_at < time.time():
                del self.codes[code]

    # --- OAuthAuthorizationServerProvider ----------------------------------

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        if not _CLIENT_ID_RE.match(client_id):
            return None
        return OAuthClient(
            client_id=client_id,
            token_endpoint_auth_method="none",
            grant_types=["authorization_code"],
            response_types=["code"],
        )

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        pass  # _register answers registrations; nothing is stored

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        if params.resource is not None and not self.resource_allowed(params.resource):
            # Otherwise a client that another server sent here would give that
            # server the credential.
            log.info("refused an OAuth resource: %s", params.resource)
            raise AuthorizeError(
                "invalid_target", "this server issues tokens only for its own /mcp"
            )
        now = time.monotonic()
        self._prune(now)
        if len(self.logins) >= MAX_PENDING:
            raise AuthorizeError("temporarily_unavailable", "too many open logins")
        address = _REQUEST_ADDRESS.get()
        if (
            sum(1 for login in self.logins.values() if login.address == address)
            >= MAX_LOGINS_PER_ADDRESS
        ):
            raise AuthorizeError(
                "temporarily_unavailable", "too many open logins from this address"
            )
        rid = secrets.token_urlsafe(24)
        self.logins[rid] = PendingLogin(str(client.client_id), params, now, address)
        return f"{self.public_url}/oauth/login?request={rid}"

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> IssuedCode | None:
        # One use only, also when the PKCE check that follows fails.
        return self.codes.pop(authorization_code, None)

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: IssuedCode
    ) -> OAuthToken:
        return OAuthToken(
            access_token=authorization_code.credential,
            scope=" ".join(authorization_code.scopes) or None,
        )

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        return None

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        raise TokenError("unsupported_grant_type", "no refresh tokens are issued")

    async def load_access_token(self, token: str) -> AccessToken | None:
        return None  # AuthMiddleware checks access tokens

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        pass

    # --- login page ----------------------------------------------------------

    def issue_code(self, login: PendingLogin, credential: str) -> IssuedCode:
        self._prune(time.monotonic())
        if len(self.codes) >= MAX_PENDING:
            raise GatewayError("too many unredeemed codes; try again later")
        params = login.params
        issued = IssuedCode(
            code=secrets.token_urlsafe(32),
            scopes=params.scopes or [],
            expires_at=time.time() + CODE_SECONDS,
            client_id=login.client_id,
            code_challenge=params.code_challenge,
            redirect_uri=params.redirect_uri,
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            resource=params.resource,
            credential=credential,
        )
        self.codes[issued.code] = issued
        return issued


OAUTH = OAuthServer()

# The page has no scripts. It must not be framed (clickjacking).
# Use "same-origin", not "no-referrer": with "no-referrer" the browser sends
# "Origin: null" on the form POST, and the login refuses it.
_PAGE_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; "
    "frame-ancestors 'none'",
    "Referrer-Policy": "same-origin",
    "X-Frame-Options": "DENY",
}

_LOGIN_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Connect to subspace</title>
<style>
body {{ font-family: system-ui, sans-serif; max-width: 32rem; margin: 3rem auto;
  padding: 0 1rem; line-height: 1.5; }}
code {{ word-break: break-all; }}
input[type=password] {{ width: 100%; padding: .5rem; box-sizing: border-box; }}
.error {{ color: #b00020; }}
button {{ padding: .5rem 1rem; margin: 1rem .5rem 0 0; }}
</style></head><body>
<h1>Connect to subspace</h1>
<p>An application asks for access to a subspace account. After you continue,
your browser goes to:</p>
<p><code>{redirect}</code></p>
<p>That application gets the access of the credential that you paste here.
Continue only if you started this connection yourself, just now, from
<strong>{host}</strong>.</p>
{error}
<form method="post" action="login">
<input type="hidden" name="request" value="{request}">
<label for="token">Credential (<code>ssp_…</code>)</label>
<input type="password" id="token" name="token" autocomplete="off" autofocus>
<button type="submit" name="action" value="allow">Continue</button>
<button type="submit" name="action" value="deny">Cancel</button>
</form></body></html>
"""


def _page(text: str, status_code: int) -> HTMLResponse:
    body = (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        "<title>subspace</title></head><body>"
        f"<p>{html.escape(text)}</p></body></html>"
    )
    return HTMLResponse(body, status_code=status_code, headers=_PAGE_HEADERS)


def _login_page(rid: str, login: PendingLogin, error: str = "") -> HTMLResponse:
    redirect = str(login.params.redirect_uri)
    host = urllib.parse.urlsplit(redirect).netloc
    return HTMLResponse(
        _LOGIN_PAGE.format(
            redirect=html.escape(redirect),
            host=html.escape(host),
            request=html.escape(rid),
            error=f'<p class="error">{html.escape(error)}</p>' if error else "",
        ),
        status_code=401 if error else 200,
        headers=_PAGE_HEADERS,
    )


def _redirect(login: PendingLogin, **params: str | None) -> RedirectResponse:
    url = construct_redirect_uri(
        str(login.params.redirect_uri), state=login.params.state, **params
    )
    return RedirectResponse(url, status_code=302, headers=_PAGE_HEADERS)


def _open_login(rid: str) -> PendingLogin | None:
    login = OAUTH.logins.get(rid)
    if login is None or time.monotonic() - login.created > LOGIN_SECONDS:
        return None
    return login


def _browser_hash(request: Request) -> str | None:
    value = request.cookies.get(OAUTH.cookie_name)
    return hashlib.sha256(value.encode()).hexdigest() if value else None


_EXPIRED = "This login expired. Start the connection again."
_OTHER_BROWSER = "This login is open in another browser. Start the connection again."


async def _login(request: Request) -> Response:
    if request.method == "GET":
        rid = request.query_params.get("request", "")
        login = _open_login(rid)
        if login is None:
            return _page(_EXPIRED, 400)
        value = request.cookies.get(OAUTH.cookie_name) or secrets.token_urlsafe(32)
        browser = hashlib.sha256(value.encode()).hexdigest()
        if login.browser is None:
            login.browser = browser
        elif login.browser != browser:
            return _page(_OTHER_BROWSER, 403)
        response = _login_page(rid, login)
        response.set_cookie(
            OAUTH.cookie_name,
            value,
            max_age=int(LOGIN_SECONDS),
            path="/",
            secure=OAUTH.secure,
            httponly=True,
            samesite="strict",
        )
        return response

    origin = request.headers.get("origin")
    public = urllib.parse.urlsplit(OAUTH.public_url)
    if origin is not None and origin != f"{public.scheme}://{public.netloc}":
        return _page("This form was sent from another site.", 403)
    form = await request.form()
    rid = str(form.get("request") or "")
    login = _open_login(rid)
    if login is None:
        return _page(_EXPIRED, 400)
    if login.browser is None or login.browser != _browser_hash(request):
        return _page(_OTHER_BROWSER, 403)
    if form.get("action") == "deny":
        OAUTH.logins.pop(rid, None)
        return _redirect(login, error="access_denied")

    client = _client_address(request.scope)
    token = str(form.get("token") or "").strip()
    if GATEWAY.blocked(client) and not GATEWAY.known_valid(token):
        return _login_page(rid, login, "Too many failed attempts. Wait a minute.")
    whoami = None
    if _TOKEN_RE.match(token):
        try:
            whoami = await GATEWAY.verify(token)
        except GatewayError:
            return _page("The service is not available. Try again later.", 503)
    if whoami is None:
        GATEWAY.record_failure(client)
        log.info(
            "OAuth login refused a token (credential %s)",
            _credential_id(token) if token.startswith("ssp_") else "-",
        )
        return _login_page(rid, login, "subspace does not accept that credential.")

    # Another request for the same login can finish while verify waits.
    if OAUTH.logins.pop(rid, None) is None:
        return _page(_EXPIRED, 400)
    try:
        issued = OAUTH.issue_code(login, token)
    except GatewayError as exc:
        return _page(str(exc), 503)
    log.info(
        "OAuth login for credential %s, redirect to %s",
        _credential_id(token),
        urllib.parse.urlsplit(str(login.params.redirect_uri)).netloc,
    )
    return _redirect(login, code=issued.code)


async def _register(request: Request) -> Response:
    """Dynamic client registration (RFC 7591), with nothing stored: every
    client is a public client, and the redirect URIs must be allowed."""
    try:
        metadata = OAuthClientMetadata.model_validate_json(await request.body())
    except ValidationError as exc:
        return JSONResponse(
            {"error": "invalid_client_metadata", "error_description": str(exc)},
            status_code=400,
        )
    refused = [
        str(uri)
        for uri in metadata.redirect_uris or []
        if not OAUTH.redirect_allowed(str(uri))
    ]
    if refused:
        log.info("refused an OAuth registration for %s", ", ".join(refused))
        return JSONResponse(
            {
                "error": "invalid_redirect_uri",
                "error_description": "not allowed by this gateway: "
                + ", ".join(refused),
            },
            status_code=400,
        )
    info = OAuthClientInformationFull.model_validate(
        {
            **metadata.model_dump(),
            "client_id": f"dcr-{secrets.token_hex(8)}",
            "client_id_issued_at": int(time.time()),
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
        }
    )
    return JSONResponse(
        info.model_dump(mode="json", exclude_none=True), status_code=201
    )


def _limited(app: ASGIApp) -> ASGIApp:
    return RequestBodyLimitMiddleware(app, 16384)


def _cors_limited(
    handler: Callable[[Request], Awaitable[Response]], methods: list[str]
) -> ASGIApp:
    """For endpoints that browser-based clients call directly."""
    return CORSMiddleware(
        _limited(request_response(handler)),
        allow_origins=["*"],
        allow_methods=methods,
        allow_headers=["mcp-protocol-version"],
    )


def oauth_routes() -> list[Route]:
    issuer = OAUTH.public_url
    # Strings, not AnyHttpUrl: the models keep an empty path, and clients
    # compare the issuer as an exact string.
    metadata = OAuthMetadata.model_validate(
        {
            "issuer": issuer,
            "authorization_endpoint": f"{issuer}/authorize",
            "token_endpoint": f"{issuer}/token",
            "registration_endpoint": f"{issuer}/register",
            "scopes_supported": [OAUTH_SCOPE],
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code"],
            "token_endpoint_auth_methods_supported": ["none"],
            "code_challenge_methods_supported": ["S256"],
        }
    )
    resource = ProtectedResourceMetadataHandler(
        ProtectedResourceMetadata.model_validate(
            {
                "resource": OAUTH.resource_url,
                "authorization_servers": [issuer],
                "scopes_supported": [OAUTH_SCOPE],
                "resource_name": "subspace",
            }
        )
    ).handle
    token_handler = TokenHandler(OAUTH, ClientAuthenticator(OAUTH))
    get_options = ["GET", "OPTIONS"]
    return [
        Route(
            "/.well-known/oauth-authorization-server",
            cors_middleware(MetadataHandler(metadata).handle, get_options),
            methods=get_options,
        ),
        # The RFC 9728 path for /mcp, and the root path that some clients try.
        Route(
            "/.well-known/oauth-protected-resource/mcp",
            cors_middleware(resource, get_options),
            methods=get_options,
        ),
        Route(
            "/.well-known/oauth-protected-resource",
            cors_middleware(resource, get_options),
            methods=get_options,
        ),
        Route(
            "/authorize",
            WithAddress(_limited(request_response(AuthorizationHandler(OAUTH).handle))),
            methods=["GET", "POST"],
        ),
        Route(
            "/oauth/login",
            _limited(request_response(_login)),
            methods=["GET", "POST"],
        ),
        Route(
            "/token",
            _cors_limited(token_handler.handle, ["POST", "OPTIONS"]),
            methods=["POST", "OPTIONS"],
        ),
        Route(
            "/register",
            _cors_limited(_register, ["POST", "OPTIONS"]),
            methods=["POST", "OPTIONS"],
        ),
    ]


# --------------------------------------------------------------------------- #
# MCP server
# --------------------------------------------------------------------------- #

# Tool name -> the account permission that it needs. Other tools need none.
_TOOL_PERMISSIONS: dict[str, str] = {
    "list_agents": "messaging",
    "set_availability": "messaging",
    "join_channel": "messaging",
    "leave_channel": "messaging",
    "list_channels": "messaging",
    "set_channel_topic": "messaging",
    "post": "messaging",
    "dm": "messaging",
    "react": "messaging",
    "message_status": "messaging",
    "set_threshold": "messaging",
    "list_subscriptions": "messaging",
    "schedule_test_notification": "schedule",
    "list_scheduled_notifications": "schedule",
    "subscribe_github_pr": "pr",
    "unsubscribe_github_pr": "pr",
    "refresh_github_pr": "pr",
    "list_github_pr_subscriptions": "pr",
    "subscribe_forgejo_pr": "pr",
    "unsubscribe_forgejo_pr": "pr",
    "refresh_forgejo_pr": "pr",
    "list_forgejo_pr_subscriptions": "pr",
}

INSTRUCTIONS = (
    "subspace: agent messaging (channels, DMs, reactions) and notifications. "
    "Nothing is pushed to you: call catch_up periodically, and wait_for_messages "
    "when you wait for a reply. Call whoami to see your account, session, and "
    "handle. Handles in your own account are bare (reviewer); handles in other "
    "accounts are qualified (reviewer@ci)."
)


class GatewayServer(MCPServer):
    """MCPServer that lists only the tools that the caller's account can use."""

    async def _handle_list_tools(self, ctx, params) -> ListToolsResult:  # type: ignore[override]
        result = await super()._handle_list_tools(ctx, params)
        request = ctx.request
        headers: Mapping[str, str] = getattr(request, "headers", None) or {}
        token = _bearer(headers)
        whoami = await GATEWAY.verify(token) if token else None
        allowed = set(whoami["permissions"]) if whoami else set()
        result.tools = [
            t
            for t in result.tools
            if _TOOL_PERMISSIONS.get(t.name) is None
            or _TOOL_PERMISSIONS[t.name] in allowed
        ]
        return result


mcp = GatewayServer("subspace", instructions=INSTRUCTIONS)


def _bearer(headers: Mapping[str, str]) -> str:
    header = headers.get("authorization", "")
    return header[len("Bearer ") :] if header.startswith("Bearer ") else ""


def _request_spec(ctx: Context) -> str | None:
    """The session that the request names in its URL or headers, or None."""
    headers: Mapping[str, str] = ctx.headers or {}
    request = ctx.request_context.request
    query: Mapping[str, str] = getattr(request, "query_params", None) or {}
    session_id = query.get("session") or headers.get("x-subspace-session")
    if session_id:
        return session_id
    handle = query.get("handle") or headers.get("x-subspace-handle")
    if handle:
        return f"@{handle}"
    return None


def _mcp_session_id(ctx: Context) -> str | None:
    return (ctx.headers or {}).get("mcp-session-id") or None


_NOT_ATTACHED = (
    "This request names no session. Add ?handle=<name> or ?session=<id> to the "
    "gateway URL (or send the X-Subspace-Handle or X-Subspace-Session header). A "
    "client that keeps an MCP session can call attach instead."
)


def _spec(ctx: Context) -> str | None:
    spec = _request_spec(ctx)
    if spec is not None:
        return spec
    mcp_session = _mcp_session_id(ctx)
    if mcp_session is None:
        return None
    return GATEWAY.attached_spec(_bearer(ctx.headers or {}), mcp_session)


# The sessions that the current tool call uses. The tool wrapper ends the call
# on each of them.
_CALL_SESSIONS: ContextVar[list[CoreSession] | None] = ContextVar(
    "_CALL_SESSIONS", default=None
)


async def _session(ctx: Context) -> CoreSession:
    """The attached session for this request, connected."""
    spec = _spec(ctx)
    if spec is None:
        raise GatewayError(_NOT_ATTACHED)
    session = GATEWAY.session(_bearer(ctx.headers or {}), spec)
    used = _CALL_SESSIONS.get()
    if used is not None and session not in used:
        session.begin_call()
        used.append(session)
    await session.ensure_connected()
    return session


async def _call(ctx: Context, frame: dict[str, Any], verb: str) -> dict:
    """Send a frame for the attached session. Raises GatewayError with a message
    for the agent if the core replies with an error."""
    session = await _session(ctx)
    reply = await session.request(frame)
    if reply.get("type") == proto.ERROR:
        raise GatewayError(f"Could not {verb}: {reply.get('error')}")
    return reply


def _tool(
    fn: Callable[..., Awaitable[str]],
) -> Callable[..., Awaitable[str]]:
    """Register ``fn`` as a tool, and turn a GatewayError into its message."""

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> str:
        used: list[CoreSession] = []
        reset = _CALL_SESSIONS.set(used)
        try:
            return await fn(*args, **kwargs)
        except GatewayError as exc:
            return str(exc)
        finally:
            _CALL_SESSIONS.reset(reset)
            for session in used:
                session.end_call()

    mcp.tool()(wrapper)
    return fn


def _fmt_time(epoch: float | None) -> str:
    if not epoch:
        return "never"
    ago = max(0, int(time.time() - epoch))
    if ago < 90:
        return f"{ago}s ago"
    if ago < 5400:
        return f"{ago // 60}m ago"
    if ago < 129600:
        return f"{ago // 3600}h ago"
    return f"{ago // 86400}d ago"


def _render_batch(session: CoreSession, items: list[dict]) -> str:
    lines: list[str] = []
    if session.connections > 1:
        lines.append(
            f"⚠️ {session.connections} connections are attached to this session; "
            "delivery can be split between them."
        )
    if not items:
        lines.append("No new notifications.")
    else:
        more = len(session.buffer)
        tail = f" ({more} more waiting; call catch_up again)" if more else ""
        lines.append(f"{len(items)} new notification(s){tail}:")
        for item in items:
            meta = item.get("meta") or {}
            level = meta.get("level")
            prefix = f"[{level}] " if level in ("direct", "urgent") else ""
            lines.append(f"{prefix}{item.get('content', '')}")
    return "\n".join(lines)


# --- session and identity ---------------------------------------------------


@_tool
async def whoami(ctx: Context) -> str:
    """Show your account, permissions, channel scope, and attached session and handle."""
    token = _bearer(ctx.headers or {})
    spec = _spec(ctx)
    if spec is not None:
        reply = await (await _session(ctx)).request({"type": proto.WHOAMI})
    else:
        reply = await GATEWAY.verify(token) or {}
    channels = reply.get("channels")
    lines = [
        f"account: {reply.get('account')} ({reply.get('label') or 'no label'})",
        f"permissions: {', '.join(reply.get('permissions') or []) or 'none'}",
        f"channels: {'all' if channels is None else ', '.join(channels) or 'none'}",
        f"max severity: {reply.get('max_severity')}",
    ]
    if reply.get("session_id") is None:
        lines.append(f"session: not attached. {_NOT_ATTACHED}")
    else:
        lines.append(f"session: {reply['session_id']}")
        lines.append(f"handle: {reply.get('handle') or '(none; call register_agent)'}")
        if int(reply.get("connections") or 0) > 1:
            lines.append(
                f"⚠️ {reply['connections']} connections are attached to this "
                "session; delivery can be split between them."
            )
    return "\n".join(lines)


@_tool
async def attach(ctx: Context, session_id: str = "", handle: str = "") -> str:
    """Attach this MCP session to a subspace session, by session id or by handle.

    Use this only if the gateway URL names no session (no ?handle= or ?session=).
    A session id is any stable identifier you have for your own work, for example a
    run id or a conversation id. A handle attaches to the session that holds that
    name in your account, or makes a new session that takes the name. Your channel
    memberships, unread messages, and subscriptions belong to the session, so
    attach to the same one each time you return.

    Args:
        session_id: a stable id for your session (1-128 visible ASCII characters).
        handle: a name (lowercase kebab-case) to attach by instead.
    """
    if _request_spec(ctx) is not None:
        return "This request already names a session in its URL or headers."
    if bool(session_id) == bool(handle):
        return "Pass exactly one of session_id or handle."
    mcp_session = _mcp_session_id(ctx)
    if mcp_session is None:
        return (
            "This client does not keep an MCP session, so an attach cannot last "
            "beyond this call. " + _NOT_ATTACHED.split(". ", 1)[1]
        )
    GATEWAY.attach(_bearer(ctx.headers or {}), mcp_session, session_id or f"@{handle}")
    return await whoami(ctx)


@_tool
async def register_agent(
    ctx: Context,
    name: str,
    description: str = "",
    capabilities: str = "",
    default_threshold: str | None = None,
    takeover: bool = False,
) -> str:
    """Register a handle for your session, so that other agents can address you.

    Handles are lowercase kebab-case, 2-64 chars (letters, digits, hyphens; no
    leading/trailing hyphen) — e.g. 'frontend', 'pr-bot', 'reviewer-2'. Agents in
    your account see it bare; agents in other accounts see it as name@account.

    Re-registering with the same name updates your profile in place. Registering a
    different name renames you, releasing the old one. A name that another session
    holds is refused, unless that session has been gone for a while or you pass
    takeover=true. Your channel memberships and messages stay with your session,
    not with the name.

    If this request names no session and your client keeps an MCP session, this
    also attaches you to the session that holds the name (or a new one).

    Args:
        name: your handle (lowercase kebab-case, 2-64 chars).
        description: one line on who you are or what you're working on.
        capabilities: free-form note of what you can help with.
        default_threshold: optional wake threshold ('all', 'direct', or 'urgent').
        takeover: take the name from the session that holds it now.
    """
    if _spec(ctx) is None:
        mcp_session = _mcp_session_id(ctx)
        if mcp_session is None:
            return _NOT_ATTACHED
        GATEWAY.attach(_bearer(ctx.headers or {}), mcp_session, f"@{name}")
    frame: dict[str, Any] = {
        "type": proto.REGISTER_AGENT,
        "name": name,
        "description": description,
        "capabilities": capabilities,
        "takeover": takeover,
    }
    if default_threshold is not None:
        frame["default_threshold"] = default_threshold
    reply = await _call(ctx, frame, f"register as '{name}'")
    agent = reply.get("agent") or {}
    return (
        f"Registered as '{agent.get('name', name)}' in account "
        f"'{agent.get('account', '?')}' (wake threshold: "
        f"{agent.get('default_threshold', 'direct')}). Other agents can find you "
        "with list_agents."
    )


@_tool
async def unregister_agent(ctx: Context) -> str:
    """Release your handle and leave every channel and DM.

    Your scheduled notifications and PR subscriptions are unaffected.
    """
    reply = await _call(ctx, {"type": proto.UNREGISTER_AGENT}, "unregister")
    agent = reply.get("agent")
    if agent is None:
        return "You held no handle."
    return f"Released '{agent.get('name')}' and left every channel and DM."


@_tool
async def list_agents(ctx: Context) -> str:
    """List every agent that holds a handle, with live presence.

    Shows each agent's name (bare in your account, name@account in others), whether
    it is connected (or roughly how long ago it was last seen), its description,
    capabilities, and wake threshold.
    """
    reply = await _call(ctx, {"type": proto.LIST_AGENTS}, "list agents")
    agents = sorted(reply.get("agents") or [], key=lambda a: a.get("name", ""))
    if not agents:
        return "No agents are registered."
    lines = []
    for a in agents:
        presence = (
            "connected"
            if a.get("connected")
            else f"last seen {_fmt_time(a.get('last_seen'))}"
        )
        if not a.get("connected") and a.get("wakeable"):
            presence += "; a message wakes it"
        detail = "; ".join(
            x
            for x in (
                a.get("description", ""),
                f"can: {a['capabilities']}" if a.get("capabilities") else "",
                f"wakes on: {a.get('default_threshold')}",
            )
            if x
        )
        lines.append(f"- {a.get('name')} ({presence}): {detail}")
    return "\n".join(lines)


@_tool
async def set_availability(ctx: Context, default_threshold: str) -> str:
    """Set your wake threshold — how insistent a message must be to count as direct.

    You must hold a handle first (see register_agent). Levels, from least to most
    strict:

        all     every message to any channel you're in.
        direct  (default) messages that mention you, DMs, and @here.
        urgent  only @here.

    Every catch_up item shows its level, so you can triage.

    Args:
        default_threshold: one of 'all', 'direct', or 'urgent'.
    """
    await _call(
        ctx,
        {"type": proto.SET_AVAILABILITY, "default_threshold": default_threshold},
        "set availability",
    )
    return f"Wake threshold set to '{default_threshold}'."


# --- channels and messages --------------------------------------------------


def _history_lines(history: list[dict]) -> list[str]:
    lines = []
    for m in history:
        handle = f"#{m['ordinal']} " if m.get("ordinal") else ""
        body = "\n      ".join(str(m.get("body", "")).splitlines())
        lines.append(f"  {handle}{m.get('sender')}: {body}")
    return lines


@_tool
async def join_channel(
    ctx: Context, channel: str, threshold: str | None = None, topic: str | None = None
) -> str:
    """Join (creating if needed) a shared channel so its messages reach you.

    Channels are communal: there is no owner, joining or posting creates one, and the
    member set is just whoever joined. Names are lowercase kebab-case (e.g. 'backend',
    'release-2'). Your account may limit which channels you can use (see whoami).
    Joining replays no history as notifications, but the reply shows the recent
    scrollback so you can catch up.

    Args:
        channel: the channel name (lowercase kebab-case).
        threshold: optional per-channel wake threshold ('all', 'direct', 'urgent') that
            overrides your global default just for this channel.
        topic: optional one-line topic to set if the channel is new/empty.
    """
    frame: dict[str, Any] = {"type": proto.JOIN_CHANNEL, "channel": channel}
    if threshold is not None:
        frame["threshold"] = threshold
    if topic is not None:
        frame["topic"] = topic
    reply = await _call(ctx, frame, f"join #{channel}")
    lines = [
        f"Joined #{reply.get('channel')}"
        + (f" — {reply['topic']}" if reply.get("topic") else "")
        + f". Members: {', '.join(reply.get('members') or [])}."
    ]
    history = reply.get("history") or []
    if history:
        lines.append("Recent messages:")
        lines.extend(_history_lines(history))
    return "\n".join(lines)


@_tool
async def leave_channel(ctx: Context, channel: str) -> str:
    """Leave a channel you previously joined; its messages stop reaching you."""
    await _call(ctx, {"type": proto.LEAVE_CHANNEL, "channel": channel}, "leave")
    return f"Left #{channel}."


@_tool
async def post(
    ctx: Context,
    channel: str,
    body: str,
    intent: str = "fyi",
    severity: str = "normal",
    mentions: list[str] | None = None,
) -> str:
    """Post a message to a channel (auto-joining it if you haven't already).

    Args:
        channel: the channel name (lowercase kebab-case). A name nobody else has joined
            means only you will see the post — the reply's member count flags that.
        body: the message text.
        intent: 'fyi' (default, terminal — no reply expected), 'question', 'request',
            or 'reply'. Intent conveys reply/display semantics, not loudness.
        severity: 'low', 'normal' (default), or 'high'. 'high' is an @here; your
            account may lower it.
        mentions: a list of handles to @-mention (name, or name@account for an
            agent in another account).
    """
    reply = await _call(
        ctx,
        {
            "type": proto.POST,
            "channel": channel,
            "body": body,
            "intent": intent,
            "severity": severity,
            "mentions": list(mentions or []),
        },
        f"post to #{channel}",
    )
    members = int(reply.get("members") or 0)
    alone = " Nobody else is in this channel." if members <= 1 else ""
    return f"Posted #{reply.get('ordinal')} to #{channel} ({members} members).{alone}"


@_tool
async def dm(
    ctx: Context,
    to: list[str],
    body: str,
    intent: str = "request",
    severity: str = "normal",
) -> str:
    """Send a direct message to one or more agents by handle.

    A DM is a private thread keyed by its participants, so messaging the same agents
    again reuses one thread. Every recipient is addressed, so a DM counts as direct.

    Args:
        to: a list of handles (name, or name@account for an agent in another
            account; see list_agents).
        body: the message text.
        intent: 'request' (default), 'question', 'reply', or 'fyi' (terminal — no
            reply expected).
        severity: 'low', 'normal' (default), or 'high' (@here).
    """
    reply = await _call(
        ctx,
        {
            "type": proto.DM,
            "to": list(to),
            "body": body,
            "intent": intent,
            "severity": severity,
        },
        "send the DM",
    )
    return f"Sent #{reply.get('ordinal')} to {', '.join(to)}."


@_tool
async def react(ctx: Context, message_id: str, reaction: str) -> str:
    """React to a message with a short, TERMINAL acknowledgment.

    A reaction acknowledges a message without asking for a reply, and never wakes
    the recipient. Prefer it over a courtesy "got it" post.

    Args:
        message_id: the short '#N' handle shown on any message (e.g. '#147'), or
            the full id ('msg:chan:room:3').
        reaction: a short, single-line reaction body (e.g. '👍', 'ack', 'done').
    """
    await _call(
        ctx,
        {"type": proto.REACT, "target": message_id, "reaction": reaction},
        "react",
    )
    return f"Reacted '{reaction}' to {message_id}."


@_tool
async def message_status(ctx: Context, message_id: str) -> str:
    """Show who has received a message, who is still pending, and any reactions to it.

    'Delivered' means the message reached that agent (it pulled it, or its client
    surfaced it), NOT that they read or acted on it. The author is never counted.

    Args:
        message_id: the short '#N' handle (e.g. '#147'), or the full id.
    """
    reply = await _call(
        ctx, {"type": proto.MESSAGE_STATUS, "target": message_id}, "get the status"
    )
    recipients = reply.get("recipients") or {}

    def _who(names: list[str]) -> str:
        parts = []
        for n in names:
            info = recipients.get(n) or {}
            state = (
                "connected"
                if info.get("connected")
                else f"last seen {_fmt_time(info.get('last_seen'))}"
            )
            parts.append(f"{n} ({state})")
        return ", ".join(parts) or "nobody"

    lines = [
        f"Delivered: {_who(reply.get('delivered') or [])}",
        f"Pending: {_who(reply.get('pending') or [])}",
    ]
    reactions = reply.get("reactions") or []
    if reactions:
        lines.append(
            "Reactions: " + ", ".join(f"{r['by']} {r['reaction']}" for r in reactions)
        )
    return "\n".join(lines)


@_tool
async def set_threshold(ctx: Context, context: str, threshold: str) -> str:
    """Set a per-channel/DM wake threshold, overriding your global default there.

    Args:
        context: the topic key from list_subscriptions (e.g. 'chan:backend').
        threshold: 'all' (every message), 'direct' (mentions / DMs / @here), or
            'urgent' (@here only).
    """
    await _call(
        ctx,
        {"type": proto.SET_THRESHOLD, "context": context, "threshold": threshold},
        "set the threshold",
    )
    return f"Threshold for {context} set to '{threshold}'."


@_tool
async def set_channel_topic(ctx: Context, channel: str, topic: str) -> str:
    """Set a channel's one-line topic (the 'what this channel is for' description)."""
    await _call(
        ctx,
        {"type": proto.SET_CHANNEL_TOPIC, "channel": channel, "topic": topic},
        "set the topic",
    )
    return f"Topic of #{channel} set."


@_tool
async def list_channels(ctx: Context) -> str:
    """List the channels your account can use, with topic and member count."""
    reply = await _call(ctx, {"type": proto.LIST_CHANNELS}, "list channels")
    channels = sorted(reply.get("channels") or [], key=lambda c: c.get("name", ""))
    if not channels:
        return "No channels."
    return "\n".join(
        f"- #{c['name']} ({c.get('members', 0)} members)"
        + (f": {c['topic']}" if c.get("topic") else "")
        for c in channels
    )


@_tool
async def list_subscriptions(ctx: Context) -> str:
    """List the channels and DMs you're in, with your effective wake threshold for each."""
    reply = await _call(ctx, {"type": proto.LIST_SUBSCRIPTIONS}, "list subscriptions")
    subs = reply.get("subscriptions") or []
    if not subs:
        return "You are in no channels or DMs."
    return "\n".join(f"- {s['context']} (threshold: {s['threshold']})" for s in subs)


# --- delivery ---------------------------------------------------------------


@_tool
async def catch_up(
    ctx: Context, max_items: int = CATCHUP_MAX_ITEMS, redeliver: bool = False
) -> str:
    """Retrieve pending notifications: messages, PR updates, and callbacks.

    Nothing is pushed to you through this gateway, so call this periodically and
    after long pauses. Items count as delivered when a call returns them. If the
    previous catch_up or wait_for_messages failed or you did not see its result,
    pass redeliver=true to get its batch again. Items marked [direct] or
    [urgent] are addressed to you.

    Args:
        max_items: at most this many new items (1-25).
        redeliver: return the previous batch again, before new items.
    """
    session = await _session(ctx)
    items = await session.pull(max(1, min(CATCHUP_MAX_ITEMS, max_items)), redeliver)
    return _render_batch(session, items)


@_tool
async def wait_for_messages(
    ctx: Context, timeout_s: float = 30.0, redeliver: bool = False
) -> str:
    """Wait until a notification arrives (or the timeout passes), then return like catch_up.

    Use this in a loop when you wait for a reply. The gateway caps the timeout
    (30 s by default), because clients time out long tool calls. Pass
    redeliver=true if you did not see the previous result, and the call returns
    that batch at once.

    Args:
        timeout_s: how long to wait, in seconds.
        redeliver: return the previous batch again, without waiting.
    """
    session = await _session(ctx)
    if redeliver and session.batch:
        return _render_batch(session, await session.pull(CATCHUP_MAX_ITEMS, True))
    timeout = max(0.0, min(GATEWAY.wait_max, timeout_s))
    if not session.buffer:
        try:
            await asyncio.wait_for(session.arrived.wait(), timeout)
        except TimeoutError:
            pass
    items = await session.pull(CATCHUP_MAX_ITEMS)
    return _render_batch(session, items)


@_tool
async def ack(ctx: Context) -> str:
    """Confirm that you saw the batch that the last catch_up or wait_for_messages
    returned, so that redeliver can no longer return it.

    You do not need this: a batch counts as delivered when a call returns it.
    """
    session = await _session(ctx)
    count = session.forget_batch()
    return f"Acknowledged {count} notification(s)."


# --- scheduled callbacks and PRs --------------------------------------------


@_tool
async def schedule_test_notification(ctx: Context, delay_seconds: int = 300) -> str:
    """Schedule a callback notification `delay_seconds` out (default 300).

    It arrives through catch_up when it is due, even across a restart.
    """
    reply = await _call(
        ctx,
        {"type": proto.SCHEDULE, "delay_seconds": max(0, delay_seconds)},
        "schedule",
    )
    return f"Scheduled callback {reply.get('id')} in {max(0, delay_seconds)}s."


@_tool
async def list_scheduled_notifications(ctx: Context) -> str:
    """List this session's pending scheduled notifications."""
    reply = await _call(ctx, {"type": proto.LIST}, "list callbacks")
    items = reply.get("items") or []
    if not items:
        return "No scheduled notifications."
    return "\n".join(
        f"- {i['id']} ({i.get('kind')}) due {i.get('due_at')}" for i in items
    )


def _pr_frame(kind: str, pr: str, forgejo: bool) -> dict[str, Any]:
    m = _PR_REF_RE.match(pr.strip())
    if m is None or (m.group("instance") and not forgejo):
        form = "[alias:]owner/repo#number" if forgejo else "owner/repo#number"
        raise GatewayError(f"Not a PR reference: {pr!r} (expected {form}).")
    frame: dict[str, Any] = {
        "type": kind,
        "owner": m.group("owner"),
        "repo": m.group("repo"),
        "number": int(m.group("number")),
    }
    if m.group("instance"):
        frame["instance"] = m.group("instance")
    return frame


def _pr_list(reply: dict) -> str:
    items = reply.get("items") or []
    if not items:
        return "No PR subscriptions."
    lines = []
    for i in items:
        ref = f"{i['instance']}:{i['pr']}" if i.get("instance") else i["pr"]
        lines.append(f"- {ref} ({i.get('pending', 0)} pending)")
    return "\n".join(lines)


async def _subscribe(ctx: Context, pr: str, forgejo: bool) -> str:
    kind = proto.SUBSCRIBE_FORGEJO_PR if forgejo else proto.SUBSCRIBE_PR
    reply = await _call(ctx, _pr_frame(kind, pr, forgejo), f"subscribe to {pr}")
    if reply.get("closed"):
        return f"{pr} is already closed or merged: {reply.get('summary')}"
    return (
        f"Subscribed to {pr}: {reply.get('summary')}. Updates arrive through catch_up."
    )


async def _refresh(ctx: Context, pr: str, forgejo: bool) -> str:
    kind = proto.REFRESH_FORGEJO_PR if forgejo else proto.REFRESH_PR
    reply = await _call(ctx, _pr_frame(kind, pr, forgejo), f"refresh {pr}")
    if reply.get("polled"):
        return f"Polling {pr} now; results reach its subscribers."
    return f"Did not poll {pr}: {reply.get('reason')}."


@_tool
async def subscribe_github_pr(ctx: Context, pr: str) -> str:
    """Subscribe this session to notifications for a GitHub PR, given as org/repo#number."""
    return await _subscribe(ctx, pr, forgejo=False)


@_tool
async def unsubscribe_github_pr(ctx: Context, pr: str) -> str:
    """Unsubscribe this session from a GitHub PR, given as org/repo#number."""
    await _call(ctx, _pr_frame(proto.UNSUBSCRIBE_PR, pr, False), "unsubscribe")
    return f"Unsubscribed from {pr}."


@_tool
async def refresh_github_pr(ctx: Context, pr: str) -> str:
    """Poll a tracked GitHub PR right now instead of waiting for its scheduled poll."""
    return await _refresh(ctx, pr, forgejo=False)


@_tool
async def list_github_pr_subscriptions(ctx: Context) -> str:
    """List this session's active GitHub PR subscriptions."""
    return _pr_list(
        await _call(ctx, {"type": proto.LIST_PR_SUBSCRIPTIONS}, "list subscriptions")
    )


@_tool
async def subscribe_forgejo_pr(ctx: Context, pr: str) -> str:
    """Subscribe this session to a Forgejo/Gitea PR, as [alias:]owner/repo#number."""
    return await _subscribe(ctx, pr, forgejo=True)


@_tool
async def unsubscribe_forgejo_pr(ctx: Context, pr: str) -> str:
    """Unsubscribe this session from a Forgejo/Gitea PR, as [alias:]owner/repo#number."""
    await _call(ctx, _pr_frame(proto.UNSUBSCRIBE_FORGEJO_PR, pr, True), "unsubscribe")
    return f"Unsubscribed from {pr}."


@_tool
async def refresh_forgejo_pr(ctx: Context, pr: str) -> str:
    """Poll a tracked Forgejo/Gitea PR right now instead of waiting for its scheduled poll."""
    return await _refresh(ctx, pr, forgejo=True)


@_tool
async def list_forgejo_pr_subscriptions(ctx: Context) -> str:
    """List this session's active Forgejo/Gitea PR subscriptions."""
    return _pr_list(
        await _call(
            ctx, {"type": proto.LIST_FORGEJO_PR_SUBSCRIPTIONS}, "list subscriptions"
        )
    )


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(_request: Request) -> Response:
    return PlainTextResponse("ok")


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #


def build_app() -> ASGIApp:
    host = _env_str("SUBSPACE_GATEWAY_HOST", "127.0.0.1")
    port = int(_env_float("SUBSPACE_GATEWAY_PORT", 8138))
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=_env_list(
            "SUBSPACE_GATEWAY_ALLOWED_HOSTS",
            [f"127.0.0.1:{port}", f"localhost:{port}", f"{host}:{port}"],
        ),
        allowed_origins=_env_list("SUBSPACE_GATEWAY_ALLOWED_ORIGINS", []),
    )
    app = mcp.streamable_http_app(
        streamable_http_path="/mcp",
        transport_security=security,
        max_request_body_size=int(_env_float("SUBSPACE_GATEWAY_MAX_BODY_BYTES", 65536)),
        session_idle_timeout=MCP_SESSION_IDLE_SECONDS,
        max_sessions=int(_env_float("SUBSPACE_GATEWAY_MAX_MCP_SESSIONS", 1000)),
        host=host,
    )
    if OAUTH.enabled:
        public = urllib.parse.urlsplit(OAUTH.public_url)
        if not OAUTH.secure and public.hostname not in _LOOPBACK_HOSTS:
            # Credentials and codes must not cross the network in clear text.
            raise SystemExit("SUBSPACE_GATEWAY_PUBLIC_URL must be https")
        app.router.routes.extend(oauth_routes())
    return AuthMiddleware(app, "/mcp")


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    host = _env_str("SUBSPACE_GATEWAY_HOST", "127.0.0.1")
    port = int(_env_float("SUBSPACE_GATEWAY_PORT", 8138))
    config = uvicorn.Config(
        build_app(), host=host, port=port, log_level="info", proxy_headers=False
    )
    server = uvicorn.Server(config)
    reaper = asyncio.create_task(GATEWAY.reap_idle())
    print(
        f"subspace gateway listening on http://{host}:{port}/mcp "
        f"(core {GATEWAY.core_url})",
        file=sys.stderr,
    )
    try:
        await server.serve()
    finally:
        reaper.cancel()
        for session in list(GATEWAY.sessions.values()):
            await session.close()


if __name__ == "__main__":
    asyncio.run(main())
