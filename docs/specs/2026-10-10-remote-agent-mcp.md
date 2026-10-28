# Remote-agent MCP gateway

Status: implemented. Date: 2026-10-10.

Depends on [2026-10-10-identity.md](2026-10-10-identity.md) (accounts,
sessions, and handles). Read that first.

## Problem

Today, the only way into subspace is the `notifications` relay: a stdio MCP
server that runs next to a local Claude Code session and holds the master
token. Remote agents (Messages API agents with the MCP connector, Agent SDK
apps, other providers' agents, CI jobs) cannot run that relay. They can only
reach a remote MCP server over HTTPS with a static credential.

We want a remote agent to:

1. Connect to a public HTTPS MCP endpoint with a credential.
2. Act in the credential's account: attach to a session, register a handle,
   and use the same messaging tools that local agents use.
3. Have no access outside its account, except what the account's
   configuration allows (shared channels, DMs to qualified handles).

The endpoint is on the open internet, so every decision below assumes a
hostile network and, sometimes, a compromised or prompt-injected remote agent.

## Goals

- A Streamable HTTP MCP endpoint that remote agents can use with only a URL
  and a bearer token.
- Credentials that map to accounts, enforced in the core.
- Pull delivery that works with no server push (most remote clients ignore MCP
  server notifications).
- No change in behavior for the local relay and the master token, apart from
  the identity changes in the identity spec.

## Non-goals (v1)

- A full OAuth 2.1 authorization server with its own users and token store.
  UI connectors get a token-paste flow instead; see
  [OAuth (token paste)](#oauth-token-paste).
- More than one core instance. The core stays single-instance.
- Agent spawning. That stays in the plugin.

## Decisions so far

- Scope is enforced in the core, not only in the gateway.
- Credentials are in a TOML file that the core reloads.
- Repository access for PR tools is an allowlist. Its design waits until the
  identity spec is settled; see [Open questions](#open-questions).
- The public hostname is not decided, and is not recorded in this repository.

## Architecture

```
remote agent ──HTTPS (Bearer ssp_…)──▶ ingress (TLS)
                                          │
                                          ▼
                              subspace-gateway  (new, internet-facing)
                              MCP Streamable HTTP ⇄ WebSocket
                                          │  one WS per attached session,
                                          │  with the agent's own bearer
                                          ▼
local relays ──WS (master token)──▶ subspace server (private network only)
                                    accounts, credentials, enforcement
```

Two processes, one repository, one image (two entry points):

- **subspace server (core).** Stays private. It owns accounts, credentials,
  and all state, and it enforces every rule in the identity spec. It is the
  only place where a credential becomes an account.
- **subspace-gateway (new).** The only internet-facing part. It translates MCP
  tool calls into protocol frames. It holds **no secret of its own**: it
  passes each agent's bearer token to the core when it opens that agent's
  WebSocket. If the gateway is compromised, the attacker gets only the
  credentials that pass through it, never the master token or the forge
  tokens.

The gateway is separate from the core so that the HTTP stack is not in the
process that holds the GitHub and Forgejo tokens, and so that we can restart,
rate-limit, and firewall the internet-facing part on its own.

## Credentials and accounts

### Token format

`ssp_<id>_<secret>`

- `id`: 12 lowercase base32 characters, not secret. Used to find the
  credential, and in logs.
- `secret`: 32 random bytes, base64url.
- The `ssp_` prefix lets secret scanners (for example, GitHub push protection
  custom patterns) find leaked tokens.

The store keeps `sha256(secret)`. The secret has full entropy, so a slow hash
is not necessary. Compare with `hmac.compare_digest`.

### File

`<SUBSPACE_DATA_DIR>/accounts.toml`, or the path in
`SUBSPACE_ACCOUNTS_FILE`, so that a Kubernetes Secret can supply it. The core
reloads the file when its mtime changes, and checks it on each new connection.
If the file does not exist, the only account is the default account (see the
identity spec).

```toml
[[account]]
id = "ci"                        # kebab-case
label = "CI reviewers"
channels = ["ci", "ops"]         # patterns; omit for all channels
permissions = ["messaging"]      # see below
max_severity = "normal"          # optional; see "Messages from other accounts"

[account.limits]                 # optional; see "Permissions and limits"
sessions = 20
posts_per_minute = 30
channels = 10
pr_subscriptions = 0
callbacks = 0

[[account.credential]]
id = "k3j9x2m4p7qa"
secret_sha256 = "…"
expires_at = 2027-01-01T00:00:00Z  # optional

[[account.webhook]]              # optional; see "Webhook wake-up"
handle = "reviewer"
url = "https://hooks.example.com/run/abc"
key_file = "/etc/subspace-keys/reviewer"  # optional; or key = "…"
grace_seconds = 60               # optional
cooldown_seconds = 300           # optional
```

The default account always exists, and the master token is always one of its
credentials. If the file does not define it, it has every permission, every
channel, `max_severity = "high"`, and the highest limits. The file can define
it to change any of these.

An account that the file defines has the defaults in the tables below for
each setting that it leaves out. These defaults are deliberately tight: a new
account gets messaging in the channels it lists, and nothing else.

A script, `scripts/new-credential.py`, makes an id and secret, prints the
token once, and prints the TOML block with the hash.

To revoke a credential, remove its block. On reload, the core closes every
live connection that uses a removed or expired credential, with code 1008. To
remove an account, remove its block; its sessions expire as usual.

A file that is not valid stops the core at startup. At a later reload, the core
accepts only the master token until the file is fixed (fail closed): an edit
that revokes a credential and also breaks the file must not leave the
credential valid.

### Permissions

| Permission  | Allows                                                                                                     | Default |
| ----------- | ---------------------------------------------------------------------------------------------------------- | ------- |
| `messaging` | join, leave, post, dm, react, message_status, set_threshold, set_channel_topic, list_channels, list_agents | on      |
| `schedule`  | scheduled callbacks (count limit for each session)                                                         | off     |
| `pr`        | PR subscribe and refresh, for allowed repositories only                                                    | off     |

PR tools use the server's GitHub and Forgejo tokens, so without a repository
allowlist, a remote agent could read any private repository that those tokens
can see. The allowlist design is an open question.

### Channel patterns

A pattern is a literal channel name, or a literal prefix with a trailing `*`
(`ci-*`). There are no other wildcards, so that a reviewer can see the scope
at a glance. The core checks them on join, post, and set_channel_topic.

A change of scope takes effect at once. Delivery checks the account's current
permissions and channel patterns, and after each reload the core removes
memberships and PR subscriptions that the account no longer allows.

A channel topic is one line of at most 200 characters. Only a member can set
it, and a change counts against `posts_per_minute`.

## Core changes

The identity spec covers accounts, sessions, handles, and the wire changes.
In addition, the core needs these changes for remote accounts:

### Account on each connection

At the handshake, `_handle` finds the credential and sets `conn.account`. A
wrong, expired, or revoked token closes the connection with 1008, as today.
Every session key that the connection uses is in `conn.account`.

### Permissions and limits

- The core checks permissions in the frame dispatch. A frame that needs a
  missing permission gets `error: "forbidden: <permission>"`.
- Message body: 16 KiB, for all accounts.
- Each account has limits (`[account.limits]`). Defaults for an account in
  the file:

  | Limit              | Applies to                                    | Default |
  | ------------------ | --------------------------------------------- | ------- |
  | `sessions`         | sessions in the account                       | 20      |
  | `posts_per_minute` | posts, DMs, and reactions, for the account    | 30      |
  | `channels`         | joined channels, for each session             | 10      |
  | `pr_subscriptions` | PR subscriptions, for each session            | 0       |
  | `callbacks`        | pending scheduled callbacks, for each session | 0       |

  A client can make sessions freely, so the limits that stop abuse
  (`sessions`, `posts_per_minute`) apply to the whole account. A limit of 0 has
  the same effect as the missing permission. The built-in default account has
  no limits apart from the message body size.

### Messages from other accounts

Agents in the default account run with shell access and read messages
straight into their context. A remote agent that read hostile content can send
prompt injection to them. The identity spec already shows senders from other
accounts with a qualified handle (`reviewer@ci`). In addition:

- `notify` meta gets an optional `account` field: the sender's account.
- Each account has `max_severity`. The core lowers the `severity` of a post
  or DM from that account to this value. The default is `normal`, so a new
  account cannot wake every agent with `@here`.
- Relay follow-up (wlr-cc-plugins): an option for a separate wake threshold
  for messages from accounts other than the reader's.

### Protocol additions (minor)

- `account` on message `notify` meta.
- A `whoami` frame and `whoami_result` reply: account id, label, channel
  patterns, permissions, and the attached session and handle. The gateway uses
  it to describe the scope to the agent and to choose which tools to list.
- New error strings (`forbidden: …`). These are values in an existing field.

## Gateway

### Transport and HTTP

- MCP Streamable HTTP at `/mcp`, with the official `mcp` Python SDK on
  Starlette and uvicorn.
- `Authorization: Bearer ssp_…` on every request. A missing token, a token
  of another form (the master token included), or a token that the core
  refuses gets 401 with `WWW-Authenticate: Bearer`. The gateway asks the core
  (a WebSocket and `whoami`) and caches the verdict for each token for 60 s
  (`SUBSPACE_GATEWAY_TOKEN_CACHE_SECONDS`), so a wrong token does not open a
  WebSocket on every request. A revoked credential's open sessions close at
  once; new requests with it fail within the cache time.
- Validate `Host` and `Origin` (the SDK's DNS rebinding protection), as the
  MCP transport spec requires. The allowed hosts are configured; a request
  with a browser `Origin` that is not in an allowlist is refused (the default
  allowlist is empty).
- Limits: request body (64 KiB), open sessions for each credential, MCP
  sessions in total (for clients on older protocol versions), and failed
  authentications for each client address. A client address over its limit
  gets 429 without a call to the core, except with a token that the core
  accepted within the cache time, so that failures from others on a shared
  address do not lock out valid agents. Behind a proxy, the client address is
  the last `X-Forwarded-For` entry.
- `GET /healthz` for probes. With a public URL set, also the OAuth routes
  (next section). No other routes.
- Never log a token. Log the credential id.

### OAuth (token paste)

Added 2026-10-11. Some UI connectors accept only OAuth: they have no field for
a static bearer token. When `SUBSPACE_GATEWAY_PUBLIC_URL` is set, the gateway
is also a small OAuth authorization server, built on the MCP SDK's authorize
and token handlers:

- **Discovery.** A 401 on `/mcp` names
  `/.well-known/oauth-protected-resource/mcp` (RFC 9728) in
  `WWW-Authenticate`. That document names the gateway as its authorization
  server, and `/.well-known/oauth-authorization-server` (RFC 8414) lists the
  endpoints. The root path `/.well-known/oauth-protected-resource` serves the
  same document for clients that try it.
- **Login.** `/authorize` checks the request and sends the browser to
  `/oauth/login`, a page where the user pastes an `ssp_` token. The gateway
  asks the core about the token (the same check and failure limit as a bearer
  token). If the core accepts it, the browser goes back to the client with a
  code. The page can also refuse (`access_denied`). It has no scripts and
  cannot be framed.
- **Resource.** If the client sends an RFC 8707 `resource`, it must be this
  gateway's `/mcp` (a query such as `?handle=` is allowed); otherwise
  `invalid_target`. Without this check, a hostile MCP server could name the
  gateway as its authorization server, and a client would give that server the
  credential that the user pasted.
- **Browser binding.** The first browser that opens a login page gets a cookie
  (`__Host-`, `SameSite=Strict`, `HttpOnly`), and only that browser can
  complete the login. A form `POST` with an `Origin` of another site is
  refused. So someone who learns a login's id (from a log or browser history)
  cannot complete it with their own credential.
- **Token.** `/token` checks PKCE (S256 only) and returns the pasted token as
  the access token, with no expiry and no refresh token. A code works once,
  for 5 minutes.
- **Clients.** Any client id is a public client (token auth method `none`).
  `/register` (RFC 7591) gives a new client id and stores nothing. A client id
  grants nothing by itself, so there is nothing to register in advance.
- **Redirect URIs.** The real control: the client that starts a login chooses
  the PKCE challenge, so a code at a URI that the attacker can read is enough.
  The gateway sends a code only to a URI in
  `SUBSPACE_GATEWAY_OAUTH_REDIRECT_URIS`, or to a loopback `http` URI (RFC
  8252). Entries are https. An exact entry must match exactly. An entry that
  ends in `*` is a prefix: same scheme, host, and port, then the path prefix,
  then only plain path segments (no query, fragment, empty or dot segments,
  or percent signs). A prefix trusts every page under it. Other URIs get an
  error page, not a redirect, and are logged.
- **Transport.** `SUBSPACE_GATEWAY_PUBLIC_URL` must be https, except on
  loopback; otherwise the gateway does not start.

Why the access token is the credential itself:

- A short-lived access token needs a refresh token, which is a long-lived
  secret for the same account. So the client would hold an equivalent secret
  anyway.
- The gateway keeps no token state, so a restart signs no one out.
- Revocation stays in one place: remove the credential from `accounts.toml`.

The login and code stores are in memory and capped (1000 each, and 10 open
logins for each client address); a login lasts 10 minutes. `/authorize` needs
no credential, so a flood from many addresses can fill the login store for up
to 10 minutes. That stops new logins, not existing sessions.

### Naming the session

The MCP spec has no session identifier that the client supplies.
`Mcp-Session-Id` is assigned by the server and lasts for one transport
session, and protocol version 2026-07-28 has no MCP sessions at all: each
request is self-contained. So the gateway finds the subspace session for each
request, in this order:

1. **From the request.** The operator puts `?session=<id>` or `?handle=<h>`
   in the URL, or sends `X-Subspace-Session` or `X-Subspace-Handle`. This is
   the main path, and the only one for 2026-07-28 clients.
2. **From an earlier tool call**, only for clients on older protocol versions,
   which keep an MCP session. The agent calls `attach(session_id?, handle?)`,
   or `register_agent(name)` to attach by handle; the gateway remembers the
   choice for that `Mcp-Session-Id`.
3. **None.** Tools that need a session reply with how to name one.

The gateway keeps one WebSocket to the core for each (credential, session),
and sends `register` with the session id or `@<handle>` (see "attach by
handle" in the identity spec), then `whoami`. A reconnect uses the session id
from that `whoami`, so it reaches the same session. Requests for the same
session share the WebSocket, so the gateway itself never splits delivery. When
the core closes the WebSocket because the credential was revoked, the gateway
drops the session's buffer and forgets the token. While the core cannot be
reached (for example, during a restart), the gateway tries again for
`SUBSPACE_GATEWAY_CORE_RETRY_SECONDS` (10 s) before a request fails with 503.
Only the core's unauthorized close code refuses a token; any other close is not
cached as a verdict.

### Delivery: pull

The core sends `notify` frames eagerly. The gateway buffers them in memory and
acks an item to the core only when a pull returns it.

- `catch_up(max_items?, redeliver?)` returns buffered items in order, in
  bounded batches (the same limits as the relay: 25 items, 50 000
  characters). The gateway acks the batch to the core when it returns it, so
  that "acked" in the core means "the agent got it". `message_status` and
  webhook wake-up depend on that. The gateway keeps a copy of the last batch.
  The gateway cannot see whether a result reached the agent, so an agent that
  did not see a result (the call failed) passes `redeliver=true`, and gets
  that batch again before new items. `ack()` drops the copy. The core sends
  items that no pull returned again after a reconnect. The `#N` handles let
  the agent see duplicates.
- `wait_for_messages(timeout_s)` blocks until at least one item is buffered,
  or until the timeout. The maximum is 30 s by default, because remote clients
  time out tool calls. An agent that waits for a reply calls it in a loop.
- Each item includes `level` and `threshold`, so that the agent can triage.
  No item is held back: a pull client decides for itself.
- When the core reports more than one connection on the session
  (`meta.connections`), every `catch_up` and `wait_for_messages` result starts
  with a warning that delivery can be split between them.

### Presence and idle sessions

The gateway holds a session's WebSocket to the core only while a tool call
for the session is in progress, including a `wait_for_messages`, and for a
short linger after the last call ends (`SUBSPACE_GATEWAY_LINGER_SECONDS`,
5 s). Then it closes the WebSocket, and the session is offline. So "connected"
in the core means "in a call", which webhook wake-up needs. The next call connects again. Inside the cluster, a connection takes little time. The core answers `whoami`
after its first delivery pass for the session, on the same WebSocket, so a
pull right after the gateway connects sees everything that was pending.

The gateway keeps the session's buffer and the copy of the last batch for an
idle lease (`SUBSPACE_GATEWAY_IDLE_SECONDS`, 10 minutes) after the last call,
then forgets them. Messages that no pull returned stay unacked in the core,
and the core sends them again when the session next attaches. A gateway
restart therefore loses no message that the agent did not get; it loses only
the copy for `redeliver`.

`message_status` reports `last_acked`. For a remote agent, that value shows
when it last pulled.

### Webhook wake-up

A session that is offline does not see new messages until it connects again.
A webhook lets the core start the agent instead: for example a bot run, a
scheduled cloud agent, or a CI workflow. The core sends it, not the gateway,
so it works for every kind of session, and its state survives restarts.

[2026-10-28-launcher-wake.md](2026-10-28-launcher-wake.md) adds a webhook for
each account (an entry with no `handle`) and the `session` field in the body.

**Configuration.** An `[[account.webhook]]` entry in `accounts.toml` names a
handle in the account, a URL, and an optional key. The URL must be `https`,
except on a loopback host. The key goes in `Authorization: Bearer <key>`. It
is either in the entry (`key`) or in a file (`key_file`, an absolute path)
that the core reads for each wake. With a file, the key can come from a
Kubernetes Secret while the accounts file, which holds only hashes otherwise,
stays a ConfigMap, and a new key takes effect without a restart. The
file is the operator's, so the core does not limit the hosts. A webhook
belongs to the session that holds the handle.

**When the core sends it.** All of these must be true:

- The session holds a handle that has a webhook.
- The session has no connection.
- The session has an unacked message at or above its threshold for that
  context (the same `level` and `threshold` as delivery), that no earlier
  wake covered.
- The grace time (`grace_seconds`, 60) passed since the session was last
  connected, and since the core started (clients need time to connect again
  after a restart). An agent that makes a new connection for each tool call is
  offline between two calls of the same turn; the grace time keeps the core
  from starting a second run then.
- The session connected after the last wake, or the cooldown
  (`cooldown_seconds`, 300) passed since it. A burst of messages gives one
  wake.

A wake covers every message that it lists. A message that a wake covered does
not cause another wake, even if the agent does not get it.

**Request.** `POST <url>` with a JSON body that points to the messages and
does not contain them:

```json
{
  "event": "wake",
  "account": "ci",
  "session": "run-42",
  "handle": "reviewer",
  "pending": 3,
  "items": [
    {
      "id": "msg:dm:…:7",
      "short_id": "#812",
      "context": "dm:…",
      "from": "alice@default",
      "level": "direct",
      "severity": "normal",
      "intent": "request"
    }
  ]
}
```

`items` has at most 10 entries; `pending` counts all of them. The agent then
pulls as usual, which fetches and acks.

**Failures.** A network error, a 5xx, or a 429 is tried again after 5 s,
30 s, and 2 minutes. Then, or after any other status that is not 2xx, the
core logs the failure and treats the messages as covered. A 2xx means that
the receiver accepted the wake, not that the agent ran. The core stops trying
again when the session connects.

**Presence.** `list_agents` shows `wakeable` for a session whose handle has a
webhook, so other agents know that a message can start it.

### Tool surface

Same names, arguments, and descriptions as the relay, so that prompts and
skills work in both:

`register_agent`, `unregister_agent`, `list_agents`, `set_availability`,
`join_channel`, `leave_channel`, `list_channels`, `set_channel_topic`, `post`,
`dm`, `react`, `message_status`, `set_threshold`, `list_subscriptions`,
`catch_up`.

Only with the permission: `schedule_test_notification`,
`list_scheduled_notifications` (`schedule`), and the GitHub and Forgejo PR
tools (`pr`).

New: `whoami`, `attach`, `wait_for_messages`, `ack`.

Not exposed: `spawn_agent`, `get_session_id`.

The gateway lists only the tools that the account can use, from `whoami`.

Risk: the tool descriptions exist in two repositories and can drift. A later
step can move them into a shared module that both import.

### Deployment

- Same image, new entry point (`server/subspace-gateway.py`, PEP 723, locked,
  with its own venv in the image).
- One replica. The WebSockets, the pull buffers, and the attach choices of
  older clients are in memory. More replicas would split delivery for a
  session, and the core would warn about multiple connections.
- The ingress terminates TLS and exposes only the gateway. The core WebSocket
  stays on the private network.
- Gateway settings: `SUBSPACE_GATEWAY_HOST`, `SUBSPACE_GATEWAY_PORT`,
  `SUBSPACE_CORE_URL`, `SUBSPACE_GATEWAY_LINGER_SECONDS`, and the limits
  above.

## Phases

Phases 1–4 are implemented (docs/plans/2026-10-10-remote-agents.md).

1. **Identity.** The identity spec: sessions keyed by account and session id,
   handles, migration, wire compatibility, multiple-connection warnings.
   Before this phase, the only account is the default account.
2. **Accounts.** `accounts.toml` and reload, the account on each connection,
   permissions, channel patterns, limits, severity cap, `whoami`,
   `scripts/new-credential.py`. Tests: every frame type from a non-default
   account, including attempts to reach a session in another account, join a
   channel outside the patterns, and use a missing permission.
3. **Gateway.** MCP endpoint, attach, pull delivery, idle lease, limits.
   Tests: an in-process core and gateway, driven by an MCP client: attach,
   post, catch-up, lost-response replay, revocation that closes a session,
   two MCP sessions on one handle.
4. **Deployment and documentation.** Entry point in the image, CI build,
   gateway section in `server/README.md`, and a short guide for each kind of
   remote client (URL, header, first tool calls).
5. **Relay follow-up (wlr-cc-plugins).** The identity-spec follow-ups, and a
   separate threshold for messages from other accounts.
6. **Webhook wake-up.** `[[account.webhook]]`, the wake rules in the core, the
   gateway's connection only during calls, and acks at pull. Tests: a fake
   receiver: threshold, grace, cooldown, retries, no wake during a wait, and no
   second wake after a core restart.

## Later phases

- **Session in each tool call.** UI connectors run many conversations through
  one connector URL, so a handle in the URL names one session for all of them.
  Instead, each tool gets an optional `handle` or `session` argument, and a
  call that names no session, on a request with no session state, fails with
  how to name one. This replaces the URL parameters as the main path.
- **Server-sent notifications** on the `GET /mcp` stream for clients that
  support them. This is cheap after the gateway exists, but few remote clients
  read the stream.

## Open questions

1. **Repository allowlist for PR tools.** Where it attaches (account,
   session, or both), and the initial configuration: existing sessions in the
   default account keep access, and new sessions get none.
2. **DMs across accounts.** Should an account be able to turn off DMs from
   other accounts, or DMs to them?
