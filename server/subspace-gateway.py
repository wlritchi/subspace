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
               SUBSPACE_GATEWAY_WAIT_MAX_SECONDS (default 30)
               SUBSPACE_GATEWAY_MAX_BODY_BYTES (default 65536)
               SUBSPACE_GATEWAY_SESSIONS_PER_CREDENTIAL (default 20)
               SUBSPACE_GATEWAY_AUTH_FAILURES_PER_MINUTE (default 20)
               SUBSPACE_GATEWAY_TOKEN_CACHE_SECONDS (default 60)
               SUBSPACE_GATEWAY_MAX_MCP_SESSIONS (default 1000)
               SUBSPACE_GATEWAY_TRUST_FORWARDED (default 0)
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
import json
import logging
import os
import re
import sys
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import subspace_protocol as proto
import uvicorn
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.server.transport_security import TransportSecuritySettings
from mcp_types import ListToolsResult
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
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
    the core sent and the agent did not pull yet, and the batch that the last
    pull returned. A batch is acked when the agent pulls again or calls ack. An
    agent that did not get a pull's result asks for the batch again with
    ``redeliver``. The core sends unacked items again after a reconnect.
    """

    def __init__(
        self,
        core_url: str,
        token: str,
        spec: str,
        on_revoked: Callable[[str], None],
    ) -> None:
        self.core_url: str = core_url
        self.token: str = token
        # The session id that the session attaches with, or "@<handle>".
        self.spec: str = spec
        # The session id that the core attached to. A reconnect uses it, so a
        # "@<handle>" session stays the same session.
        self.session_id: str | None = None
        self._on_revoked = on_revoked
        self.ws: ClientConnection | None = None
        self.buffer: list[dict] = []
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
            ws = await _open_core(self.core_url, self.token)
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
        if any(f["id"] == notify_id for f in (*self.batch, *self.buffer)):
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

    async def ack_batch(self) -> int:
        """Ack the batch that the last pull returned. Returns how many."""
        batch, self.batch = self.batch, []
        ws = self.ws
        if ws is not None:
            for frame in batch:
                await _send_quietly(ws, {"type": proto.ACK, "id": frame["id"]})
        return len(batch)

    async def pull(self, max_items: int, redeliver: bool = False) -> list[dict]:
        """Take the next batch from the buffer. Ack the previous batch first,
        or, with ``redeliver``, start the new batch with it again."""
        await self.ensure_connected()
        taken: list[dict] = []
        if redeliver:
            taken, self.batch = self.batch, []
        else:
            await self.ack_batch()
        chars = sum(len(f.get("content", "")) for f in taken)
        while self.buffer and len(taken) < max_items:
            size = len(self.buffer[0].get("content", ""))
            if taken and chars + size > CATCHUP_MAX_CHARS:
                break
            taken.append(self.buffer.pop(0))
            chars += size
        if not self.buffer:
            self.arrived.clear()
        self.batch = taken
        return taken

    async def close(self) -> None:
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


async def _open_core(core_url: str, token: str) -> ClientConnection:
    try:
        return await connect(
            core_url,
            additional_headers={proto.AUTH_HEADER: proto.auth_header_value(token)},
            open_timeout=CORE_TIMEOUT_SECONDS,
        )
    except (OSError, InvalidStatus, TimeoutError) as exc:
        # The details name internal addresses: log them, do not return them.
        log.warning("cannot reach the subspace server: %s", exc)
        raise GatewayError("the subspace server is not reachable") from exc


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
        whoami: dict | None = None
        try:
            ws = await _open_core(self.core_url, token)
            try:
                await ws.send(json.dumps({"type": proto.WHOAMI, "req_id": 1}))
                reply = json.loads(
                    await asyncio.wait_for(ws.recv(), CORE_TIMEOUT_SECONDS)
                )
                if reply.get("type") == proto.WHOAMI_RESULT:
                    whoami = reply
            finally:
                await ws.close()
        except ConnectionClosed:
            whoami = None
        self.verdicts[key] = TokenVerdict(whoami, now)
        return whoami

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
        created = CoreSession(self.core_url, token, spec, self.forget_token)
        self.sessions[key] = created
        return created

    async def reap_idle(self) -> None:
        while True:
            await asyncio.sleep(min(30.0, max(1.0, self.idle_seconds / 4)))
            now = time.monotonic()
            for key, session in list(self.sessions.items()):
                if now - session.last_used > self.idle_seconds:
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
            await PlainTextResponse(
                "unauthorized",
                status_code=401,
                headers={"WWW-Authenticate": 'Bearer realm="subspace"'},
            )(scope, receive, send)
            return
        await self.app(scope, receive, send)


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


async def _session(ctx: Context) -> CoreSession:
    """The attached session for this request, connected."""
    spec = _spec(ctx)
    if spec is None:
        raise GatewayError(_NOT_ATTACHED)
    session = GATEWAY.session(_bearer(ctx.headers or {}), spec)
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
        try:
            return await fn(*args, **kwargs)
        except GatewayError as exc:
            return str(exc)

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
        body = str(m.get("body", "")).replace("\r", "").replace("\n", "\n      ")
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
    after long pauses. Each call acknowledges the batch that the previous
    catch_up or wait_for_messages returned. If that call failed or you did not
    see its result, pass redeliver=true to get its batch again. Items marked
    [direct] or [urgent] are addressed to you.

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
    (30 s by default), because clients time out long tool calls. Like catch_up,
    it acknowledges the previous batch; pass redeliver=true if you did not see
    the previous result, and the call returns that batch at once.

    Args:
        timeout_s: how long to wait, in seconds.
        redeliver: return the previous batch again, without waiting.
    """
    session = await _session(ctx)
    if redeliver and session.batch:
        return _render_batch(session, await session.pull(CATCHUP_MAX_ITEMS, True))
    await session.ack_batch()
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
    """Acknowledge the batch that the last catch_up or wait_for_messages returned.

    catch_up does this for you on its next call; use ack when you stop pulling.
    """
    session = await _session(ctx)
    count = await session.ack_batch()
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
