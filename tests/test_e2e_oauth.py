# vim: filetype=python
"""End-to-end tests for the gateway's OAuth token-paste flow: a spawned core and
a spawned gateway with a public URL, driven over HTTP."""

import asyncio
import base64
import hashlib
import secrets
import urllib.parse
from collections.abc import Iterator

import httpx2
import pytest

from _harness import free_port
from test_e2e_gateway import Stack, _stack
from test_e2e_identity import MASTER

pytestmark = pytest.mark.slow

CALLBACK = "https://client.example/cb"


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    port = free_port()
    with _stack(
        tmp_path_factory.mktemp("oauth"),
        gateway_port=port,
        public_url=f"http://127.0.0.1:{port}",
        oauth_redirect_uris=f"{CALLBACK},https://prefix.example/cb/*,"
        "https://nopath.example*",
    ) as s:
        yield s


def _base(stack: Stack) -> str:
    return f"http://127.0.0.1:{stack.gateway_port}"


def _pkce() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode()).digest()
    return verifier, base64.urlsafe_b64encode(digest).decode().rstrip("=")


async def _authorize(
    http: httpx2.AsyncClient,
    stack: Stack,
    challenge: str,
    redirect_uri: str = CALLBACK,
    client_id: str = "subspace",
    **extra: str,
) -> httpx2.Response:
    return await http.get(
        f"{_base(stack)}/authorize",
        params={
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "state": "st8",
            "scope": "subspace",
            **extra,
        },
    )


async def _login(
    http: httpx2.AsyncClient, stack: Stack, authorize: httpx2.Response, **form: str
) -> httpx2.Response:
    assert authorize.status_code == 302, authorize.text
    login_url = authorize.headers["location"]
    page = await http.get(login_url)
    assert page.status_code == 200
    rid = urllib.parse.parse_qs(urllib.parse.urlsplit(login_url).query)["request"][0]
    return await http.post(f"{_base(stack)}/oauth/login", data={"request": rid, **form})


def _query(response: httpx2.Response) -> dict[str, str]:
    assert response.status_code == 302, response.text
    location = response.headers["location"]
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(location).query))


async def _exchange(
    http: httpx2.AsyncClient, stack: Stack, code: str, verifier: str
) -> httpx2.Response:
    return await http.post(
        f"{_base(stack)}/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": CALLBACK,
            "client_id": "subspace",
            "code_verifier": verifier,
        },
    )


def test_discovery(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            reply = await http.post(stack.url(), json={})
            assert reply.status_code == 401
            challenge = reply.headers["www-authenticate"]
            resource_url = f"{_base(stack)}/.well-known/oauth-protected-resource/mcp"
            assert f'resource_metadata="{resource_url}"' in challenge

            resource = (await http.get(resource_url)).json()
            assert resource["resource"] == f"{_base(stack)}/mcp"
            assert resource["authorization_servers"] == [_base(stack)]

            metadata = (
                await http.get(f"{_base(stack)}/.well-known/oauth-authorization-server")
            ).json()
            assert metadata["issuer"] == _base(stack)
            assert metadata["authorization_endpoint"] == f"{_base(stack)}/authorize"
            assert metadata["token_endpoint_auth_methods_supported"] == ["none"]
            assert metadata["code_challenge_methods_supported"] == ["S256"]

    asyncio.run(scenario())


def test_token_paste_flow(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            verifier, challenge = _pkce()
            authorize = await _authorize(http, stack, challenge)
            page = await http.get(authorize.headers["location"])
            assert "client.example" in page.text
            assert page.headers["x-frame-options"] == "DENY"

            redirect = await _login(
                http, stack, authorize, token=stack.tokens["ci"], action="allow"
            )
            params = _query(redirect)
            assert redirect.headers["location"].startswith(CALLBACK + "?")
            assert params["state"] == "st8"

            token = await _exchange(http, stack, params["code"], verifier)
            assert token.status_code == 200, token.text
            assert token.json()["access_token"] == stack.tokens["ci"]
            assert token.json()["token_type"] == "Bearer"

            # A code works once.
            again = await _exchange(http, stack, params["code"], verifier)
            assert again.status_code == 400
            assert again.json()["error"] == "invalid_grant"

    asyncio.run(scenario())


def test_wrong_verifier(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            verifier, challenge = _pkce()
            authorize = await _authorize(http, stack, challenge)
            redirect = await _login(
                http, stack, authorize, token=stack.tokens["ci"], action="allow"
            )
            code = _query(redirect)["code"]
            wrong = await _exchange(http, stack, code, verifier + "x")
            assert wrong.status_code == 400
            # The failed attempt used up the code.
            right = await _exchange(http, stack, code, verifier)
            assert right.status_code == 400

    asyncio.run(scenario())


def test_login_refuses_bad_credentials(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            _verifier, challenge = _pkce()
            authorize = await _authorize(http, stack, challenge)
            for token in (MASTER, "nope", "ssp_abcdefghijkl_" + "x" * 40):
                page = await _login(http, stack, authorize, token=token, action="allow")
                assert page.status_code == 401
                assert "does not accept" in page.text
            # The login stays open after a wrong credential.
            redirect = await _login(
                http, stack, authorize, token=stack.tokens["ci"], action="allow"
            )
            assert "code" in _query(redirect)

    asyncio.run(scenario())


def test_deny(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            _verifier, challenge = _pkce()
            authorize = await _authorize(http, stack, challenge)
            redirect = await _login(http, stack, authorize, action="deny")
            params = _query(redirect)
            assert params == {"error": "access_denied", "state": "st8"}

    asyncio.run(scenario())


def test_redirect_allowlist(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            _verifier, challenge = _pkce()
            for uri in (
                "https://prefix.example/cb/app/123",
                "http://127.0.0.1:9999/callback",
                "http://localhost/cb",
            ):
                reply = await _authorize(http, stack, challenge, redirect_uri=uri)
                assert reply.status_code == 302, (uri, reply.text)
                assert "/oauth/login?" in reply.headers["location"]
            for uri in (
                "https://evil.example/cb",
                "https://client.example/cb2",
                "https://prefix.example/cbx",
                "https://nopath.example/cb",
                "https://nopath.example.evil/cb",
                "https://prefix.example/cb/x?next=https://evil.example",
                "https://prefix.example/cb/x#frag",
                "https://prefix.example/cb/..%2f..%2fredirect",
                "https://prefix.example/cb//evil.example/",
                "https://prefix.example/cb/a/../../b",
                "https://prefix.example/cb/%2e%2e/b",
                "https://prefix.example:8443/cb/x",
                "http://prefix.example/cb/x",
                "http://localhost@evil.example/cb",
                "https://127.0.0.1/cb",
            ):
                reply = await _authorize(http, stack, challenge, redirect_uri=uri)
                # Not a redirect: the gateway must not send the browser there.
                assert reply.status_code == 400, (uri, reply.text)
                assert "location" not in reply.headers
            bad_client = await _authorize(http, stack, challenge, client_id="a b")
            assert bad_client.status_code == 400

    asyncio.run(scenario())


def test_registration(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            reply = await http.post(
                f"{_base(stack)}/register",
                json={
                    "redirect_uris": [CALLBACK],
                    "client_name": "Example",
                    "token_endpoint_auth_method": "client_secret_post",
                },
            )
            assert reply.status_code == 201, reply.text
            info = reply.json()
            assert info["token_endpoint_auth_method"] == "none"
            assert "client_secret" not in info

            verifier, challenge = _pkce()
            authorize = await _authorize(
                http, stack, challenge, client_id=info["client_id"]
            )
            redirect = await _login(
                http, stack, authorize, token=stack.tokens["ci"], action="allow"
            )
            token = await http.post(
                f"{_base(stack)}/token",
                data={
                    "grant_type": "authorization_code",
                    "code": _query(redirect)["code"],
                    "redirect_uri": CALLBACK,
                    "client_id": info["client_id"],
                    "code_verifier": verifier,
                },
            )
            assert token.status_code == 200, token.text

            refused = await http.post(
                f"{_base(stack)}/register",
                json={"redirect_uris": [CALLBACK, "https://evil.example/cb"]},
            )
            assert refused.status_code == 400
            assert refused.json()["error"] == "invalid_redirect_uri"

    asyncio.run(scenario())


def test_access_token_works_on_mcp(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            verifier, challenge = _pkce()
            authorize = await _authorize(http, stack, challenge)
            redirect = await _login(
                http, stack, authorize, token=stack.tokens["ci"], action="allow"
            )
            access = (
                await _exchange(http, stack, _query(redirect)["code"], verifier)
            ).json()["access_token"]
        stack.tokens["oauth"] = access
        async with stack.mcp("?handle=oauth-bot", account="oauth") as client:
            result = await client.call_tool("whoami", {})
            text = "\n".join(getattr(c, "text", "") for c in result.content)
            assert "account: ci" in text

    asyncio.run(scenario())


def test_open_logins_per_address(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            _verifier, challenge = _pkce()
            opened: list[str] = []
            while True:
                reply = await _authorize(http, stack, challenge)
                assert reply.status_code == 302
                params = urllib.parse.urlsplit(reply.headers["location"])
                if "/oauth/login" not in params.path:
                    break
                opened.append(urllib.parse.parse_qs(params.query)["request"][0])
                assert len(opened) <= 10
            # Refused back to the client, which then shows the error.
            assert dict(urllib.parse.parse_qsl(params.query))["error"] == (
                "temporarily_unavailable"
            )
            # Closing logins frees room for new ones.
            for rid in opened:
                await http.get(f"{_base(stack)}/oauth/login?request={rid}")
                await http.post(
                    f"{_base(stack)}/oauth/login",
                    data={"request": rid, "action": "deny"},
                )
            reply = await _authorize(http, stack, challenge)
            assert "/oauth/login?" in reply.headers["location"]
            rid = urllib.parse.parse_qs(
                urllib.parse.urlsplit(reply.headers["location"]).query
            )["request"][0]
            await http.get(reply.headers["location"])
            await http.post(
                f"{_base(stack)}/oauth/login", data={"request": rid, "action": "deny"}
            )

    asyncio.run(scenario())


def test_resource_must_be_this_gateway(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as http:
            _verifier, challenge = _pkce()
            for resource in (
                f"{_base(stack)}/mcp",
                f"{_base(stack)}/mcp/",
                f"{_base(stack)}/mcp?handle=bot",
            ):
                reply = await _authorize(http, stack, challenge, resource=resource)
                assert "/oauth/login?" in reply.headers["location"], resource
            for resource in ("https://evil.example/mcp", f"{_base(stack)}/other"):
                reply = await _authorize(http, stack, challenge, resource=resource)
                assert _query(reply)["error"] == "invalid_target", resource

    asyncio.run(scenario())


def test_login_is_bound_to_its_browser(stack: Stack) -> None:
    async def scenario() -> None:
        async with httpx2.AsyncClient() as victim, httpx2.AsyncClient() as other:
            _verifier, challenge = _pkce()
            authorize = await _authorize(victim, stack, challenge)
            login_url = authorize.headers["location"]
            assert (await victim.get(login_url)).status_code == 200
            rid = urllib.parse.parse_qs(urllib.parse.urlsplit(login_url).query)[
                "request"
            ][0]
            form = {"request": rid, "token": stack.tokens["ci"], "action": "allow"}
            # Another browser that knows the request id can neither open the
            # page nor complete the login.
            assert (await other.get(login_url)).status_code == 403
            stolen = await other.post(f"{_base(stack)}/oauth/login", data=form)
            assert stolen.status_code == 403
            # A form sent from another site is refused.
            cross = await victim.post(
                f"{_base(stack)}/oauth/login",
                data=form,
                headers={"Origin": "https://evil.example"},
            )
            assert cross.status_code == 403
            done = await victim.post(f"{_base(stack)}/oauth/login", data=form)
            assert "code" in _query(done)

    asyncio.run(scenario())
