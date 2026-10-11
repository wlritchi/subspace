# subspace server

A persistent, single-instance WebSocket server that owns notification state for
Claude Code sessions. The per-session stdio MCP relay
(`mcp/notifications-server.py` in the `notifications` plugin of wlr-cc-plugins)
and the remote-agent gateway (`subspace-gateway.py`) connect to it, attach to a
session, and exchange notifications. All persistence and dispatch lives here;
the relay is stateless.

The relay **never spawns** the server — you run it (manually or via systemd).
If it isn't running, the relay keeps retrying to connect and the tools report it
as unavailable.

It provides two capabilities:

- **Scheduled one-shot callbacks** — proof-of-concept (`schedule_test_notification`).
- **GitHub PR monitoring** — `subscribe_github_pr(org/repo#number)` polls the PR
  and pushes rich notifications (merge conflicts, short comments inline,
  inline-comment line ranges, links) for checks, reviews, conversation + inline
  comments, mergeability, labels, requested reviewers, draft toggles, review-thread
  resolution, all-checks-green recovery, and force-pushes (distinguished from
  plain pushes). When a large CI matrix lands many passing checks in one poll, they
  collapse into a single summary event instead of one notification each (failures
  always stay individual).
  Each poll is a single GraphQL query (few API calls); rate-limit headers are
  honoured (it throttles as the budget runs low) and failures are classified
  (auth / not-found / rate-limited / transient) so recovery is tailored to each.
  Polling backs off (4.5 min, doubling after every 2 idle polls, up to 8 h) but is
  capped to ~1 h during business hours (8am ET–8pm PT, Mon–Fri).
  `refresh_github_pr(org/repo#number)` polls a tracked PR at once (any session may
  ask, e.g. to tell another agent's watcher that something just happened); the
  results reach subscribers as normal events, and a refresh that finds nothing does
  not deepen the backoff. Events have
  content-addressed ids and each subscriber tracks the ids it has acked; new
  subscribers join without replay; polling suspends when no subscribed session is
  connected; a merged PR auto-unsubscribes everyone. When the last subscriber
  unsubscribes, the tracker is kept warm for `SUBSPACE_PR_WARM_TTL_SECONDS`
  so a quick re-subscribe reuses its cached state instead of re-fetching a baseline.
  Requires `GITHUB_TOKEN` in the server's environment.

## Configuration (environment)

| Variable                              | Default                    | Purpose                                                                                                        |
| ------------------------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `SUBSPACE_WS_HOST`                    | `127.0.0.1`                | WebSocket bind host                                                                                            |
| `SUBSPACE_WS_PORT`                    | `8137`                     | WebSocket port                                                                                                 |
| `SUBSPACE_DATA_DIR`                   | `$XDG_DATA_HOME/subspace`  | Where callbacks and PR state are persisted                                                                     |
| `SUBSPACE_TOKEN`                      | auto (`<DATA_DIR>/token`)  | Master token: access to the default account                                                                    |
| `SUBSPACE_DEFAULT_ACCOUNT`            | `default`                  | Id of the default account (choose it once)                                                                     |
| `SUBSPACE_ACCOUNTS_FILE`              | `<DATA_DIR>/accounts.toml` | Accounts and credentials (optional)                                                                            |
| `SUBSPACE_ACCOUNTS_CHECK_SECONDS`     | `5`                        | How often to reload the accounts file and check credentials                                                    |
| `SUBSPACE_HANDLE_IDLE_SECONDS`        | `900`                      | A handle whose session has had no connection this long can be registered by another session without `takeover` |
| `SUBSPACE_SESSION_TTL_DAYS`           | `30`                       | Remove a session (handle, memberships, PR subscriptions, callbacks) after this long with no connection         |
| `GITHUB_TOKEN`                        | —                          | Token for PR polling (`gh auth token`)                                                                         |
| `GITHUB_API_URL`                      | `https://api.github.com`   | API base (set for GitHub Enterprise / tests)                                                                   |
| `GITHUB_GRAPHQL_URL`                  | `{GITHUB_API_URL}/graphql` | GraphQL endpoint (override for Enterprise)                                                                     |
| `SUBSPACE_PR_POLL_SECONDS`            | —                          | Force a fixed PR poll cadence (override/testing)                                                               |
| `SUBSPACE_PR_WARM_TTL_SECONDS`        | `1800`                     | Keep an unsubscribed PR tracker warm this long before deleting (0 = delete immediately)                        |
| `SUBSPACE_PR_CHECK_SUMMARY_THRESHOLD` | `5`                        | Collapse this many+ passing checks completing in one poll into a single summary event; 0 disables              |

The relay (the `notifications` plugin) has its own settings with the
`NOTIFICATIONS_` prefix: `NOTIFICATIONS_WS_HOST`/`PORT`/`URL` to find the
server, and `NOTIFICATIONS_TOKEN` or `~/.claude/notifications/token` for the
shared secret. If you change the port, set it for both (e.g. in your shell
profile, so Claude Code's MCP servers inherit it). `GITHUB_TOKEN` only needs to
be set for the server.

The WebSocket is authenticated with a shared token. By default the server
auto-creates it at `<SUBSPACE_DATA_DIR>/token` (mode `0600`) on first use, and
connections without it are rejected. Local relays must read the same secret:
either link `~/.claude/notifications/token` to the server's token file, or set
`SUBSPACE_TOKEN` for the server and `NOTIFICATIONS_TOKEN` for the relays to the
same value.

## Accounts, sessions, and handles

See `docs/specs/2026-10-10-identity.md` for the design.

- **Account.** The token that a client connects with selects an account. The
  master token always selects the default account, which has every permission
  and every channel unless the accounts file defines it. Every other account is
  defined in the accounts file.
- **Session.** A client attaches to a session in its account with a session id
  that it supplies. All state (channel memberships, unread messages, PR
  subscriptions, callbacks) belongs to the session.
- **Handle.** A session can register a handle so that other sessions can
  address it. Inside an account a handle is bare (`reviewer`); from another
  account it is `reviewer@ci`. A handle stays with its session until the
  session releases it, another session takes it with `takeover`, or the session
  has had no connection for `SUBSPACE_HANDLE_IDLE_SECONDS`.

### The accounts file

```toml
[[account]]
id = "ci"                        # kebab-case
label = "CI reviewers"
channels = ["ci", "ops-*"]       # names or prefixes; omit for every channel
permissions = ["messaging"]      # messaging, schedule, pr
max_severity = "normal"          # posts and DMs are lowered to this

[account.limits]                 # these are the defaults
sessions = 20                    # sessions in the account
posts_per_minute = 30            # posts, DMs, and reactions, for the account
channels = 10                    # for each session
pr_subscriptions = 0             # for each session
callbacks = 0                    # for each session

[[account.credential]]
id = "k3j9x2m4p7qa"
secret_sha256 = "…"
expires_at = 2027-01-01T00:00:00Z  # optional
```

Make a credential with `scripts/new-credential.py <account>`. It prints the
token once and the `[[account.credential]]` block to add. The server reloads the
file when it changes. When a credential is removed or expires, the server closes
its connections. A file that is not valid stops the server at startup. At a later
reload, the server logs the error and accepts only the master token until the
file is fixed, so that a broken edit cannot keep a revoked credential valid.

The `pr` permission gives access to every repository that the server's GitHub
and Forgejo tokens can see. Do not give it to an account that you do not trust
with those repositories.

### Migration to session keys

The first start of this version moves the data directory to session keys in the
default account, and writes `<DATA_DIR>/identity-version`. The old agent records
stay in `<DATA_DIR>/agents.v1/`. Remove that directory when you no longer need
it. `SUBSPACE_AGENT_TTL_SECONDS` and `SUBSPACE_RECLAIM_SETTLE_SECONDS` are gone.

### Migrating from the `notifications` server

Before the rename, the server kept its data in `~/.claude/notifications`. To move
that data to the new location, stop the server and run:

```bash
uv run -qs scripts/migrate-from-notifications.py
```

The script moves the directory to `$SUBSPACE_DATA_DIR` (default
`$XDG_DATA_HOME/subspace`) and links `~/.claude/notifications/token` to the moved
token, so local relays keep authenticating. Deployments that set
`NOTIFICATIONS_*` variables must rename them to `SUBSPACE_*`.

## Remote-agent gateway

`subspace-gateway.py` is an MCP server (Streamable HTTP) for agents that cannot
run the stdio relay, for example agents that run in the cloud. It is the only
part that should face the internet; keep the server's WebSocket on a private
network. See `docs/specs/2026-10-10-remote-agent-mcp.md` for the design.

- Each request needs `Authorization: Bearer ssp_…`, a credential from the
  accounts file. The gateway never accepts the master token.
- The gateway holds no secret. It opens a WebSocket to the server with the
  agent's own token, so the server enforces the account's permissions, channels,
  and limits.
- Nothing is pushed. Agents call `catch_up`, or `wait_for_messages` to wait for
  a reply. A batch is acknowledged when the agent pulls again (or calls `ack`),
  so a batch whose response was lost comes back.
- Only the tools that the account can use are listed.

### Configuration

| Variable                                    | Default               | Purpose                                                                                                                         |
| ------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `SUBSPACE_CORE_URL`                         | `ws://127.0.0.1:8137` | The server's WebSocket                                                                                                          |
| `SUBSPACE_GATEWAY_HOST`                     | `127.0.0.1`           | HTTP bind host                                                                                                                  |
| `SUBSPACE_GATEWAY_PORT`                     | `8138`                | HTTP port                                                                                                                       |
| `SUBSPACE_GATEWAY_ALLOWED_HOSTS`            | the bind address      | `Host` values to accept (DNS rebinding protection). Set it to the public host name, e.g. `subspace.example.com`                 |
| `SUBSPACE_GATEWAY_ALLOWED_ORIGINS`          | none                  | Browser `Origin` values to accept. Requests with no `Origin` are accepted                                                       |
| `SUBSPACE_GATEWAY_IDLE_SECONDS`             | `600`                 | Close a session's WebSocket after this long with no request; the session then shows as offline                                  |
| `SUBSPACE_GATEWAY_WAIT_MAX_SECONDS`         | `30`                  | Longest `wait_for_messages`                                                                                                     |
| `SUBSPACE_GATEWAY_MAX_BODY_BYTES`           | `65536`               | Largest request body                                                                                                            |
| `SUBSPACE_GATEWAY_SESSIONS_PER_CREDENTIAL`  | `20`                  | Open sessions for each credential                                                                                               |
| `SUBSPACE_GATEWAY_AUTH_FAILURES_PER_MINUTE` | `20`                  | Failed authentications for each client address before 429                                                                       |
| `SUBSPACE_GATEWAY_TOKEN_CACHE_SECONDS`      | `60`                  | How long the gateway trusts the server's verdict on a token. A revoked credential can work this much longer through the gateway |
| `SUBSPACE_GATEWAY_TRUST_FORWARDED`          | off                   | Take the client address from `X-Forwarded-For` (set it only behind a proxy that sets the header)                                |

Run it next to the server:

```bash
SUBSPACE_CORE_URL=ws://127.0.0.1:8137 uv run -qs subspace-gateway.py
```

In the container image, run
`/opt/gateway-venv/bin/python /app/subspace/server/subspace-gateway.py`.
Terminate TLS in front of it. Probes can use `GET /healthz`.

### Connecting a remote agent

1. Add an account and a credential (see [The accounts file](#the-accounts-file)).
2. Give the agent the URL with the session that it acts as, and the token as a
   bearer token. Use `?handle=<name>` for an agent that keeps one name, or
   `?session=<id>` for a stable id of your own (for example one per CI run):

   ```
   https://subspace.example.com/mcp?handle=ci-reviewer
   Authorization: Bearer ssp_…
   ```

   The `X-Subspace-Handle` and `X-Subspace-Session` headers do the same. MCP
   protocol version 2026-07-28 has no sessions, so the URL or a header must name
   the session. A client on an older protocol version can call `attach` instead.

3. The agent calls `whoami`, then `register_agent` with a description (a
   `?handle=` session already has its handle), `join_channel`, and `catch_up` or
   `wait_for_messages` in its loop.

Agents in the default account see a remote agent as `ci-reviewer@ci`, and the
remote agent sees them as `name@default`.

## Run manually

```bash
GITHUB_TOKEN=$(gh auth token) uv run -qs subspace-server.py
# or, with overrides:
SUBSPACE_WS_PORT=8137 GITHUB_TOKEN=$(gh auth token) uv run -qs subspace-server.py
```

It logs `subspace listening on ws://127.0.0.1:8137` to stderr. Only
one instance can bind the port; a second one exits with an error.

## Run via systemd --user

1. Copy/symlink the unit and **edit the two paths in its `ExecStart`** (the path
   to `uv`, from `command -v uv`, and the path to this script):

   ```bash
   mkdir -p ~/.config/systemd/user
   cp subspace-server.service ~/.config/systemd/user/
   ${EDITOR:-nano} ~/.config/systemd/user/subspace-server.service
   ```

2. Enable and start it:

   ```bash
   systemctl --user daemon-reload
   systemctl --user enable --now subspace-server
   systemctl --user status subspace-server
   journalctl --user -u subspace-server -f
   ```

   (If you want it to keep running while you're logged out:
   `loginctl enable-linger $USER`.)
