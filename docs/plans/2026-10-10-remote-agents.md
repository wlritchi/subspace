# Plan: identity, accounts, and the remote-agent gateway

Date: 2026-10-10. Specs: [identity](../specs/2026-10-10-identity.md),
[remote-agent MCP gateway](../specs/2026-10-10-remote-agent-mcp.md).

Work happens on one branch, with one commit for each step. Every step keeps
the test suite green.

## Step 1: session keys and the session registry (core)

- `lib/sessions.py` replaces `lib/agent_registry.py`. A `SessionRecord` is
  keyed by the session key `"<account>/<session id>"`, and holds `handle`,
  `description`, `capabilities`, `default_threshold`, `registered_at`,
  `last_seen`, `last_acked`. The registry keeps a handle index for each
  account and applies the handle ownership rules (`takeover`, idle release).
  The clock and liveness are injected, as before.
- `storage.key_filename(key)`: an injective filename for a session key
  (percent-encodes everything outside `[A-Za-z0-9_.-]`). `storage.safe_name`
  is lossy, and session ids are arbitrary visible ASCII.
- Session id validation: 1–128 characters in 0x21–0x7E.

## Step 2: server on session keys

- `CONNECTIONS`: session key -> list of connections.
- Every handler resolves the session key from the connection's account and
  the session id (`register`, or the frame's `session_id`).
- Topic members, acked sets, thresholds, PR subscribers, and callback
  directories use session keys.
- `Message` gets `sender_key` and `sender_account`. `mentions` holds session
  keys for resolved handles, and raw strings for others.
- Every output that names a session is rendered relative to the reader:
  bare handle in the same account, `handle@account` otherwise. This includes
  rendered lines, `notify` meta, `channel_joined`, `message_status`,
  `list_agents`.
- DMs find an existing thread by the set of participant keys. A new thread's
  key is `dm:` plus the sorted qualified handles, with a `~N` suffix if that
  key is already used.
- Attach by handle: `register` with session id `@<handle>`.
- Remove reclaim keys, the settle time, generations, the succession post, the
  heir notice, and `SUPERSEDED`. Deprecated wire fields get stub values.
- `unregister_agent` releases the handle and leaves every topic.
- A session that loses its handle to `takeover` gets a `notify` with
  `meta.kind = "handle_lost"`.
- Multiple connections: a `multiple_connections` notice to every connection
  when the count goes above one or back to one, and `meta.connections` on
  every `notify` while it is above one.
- Session expiry in the reaper (`SUBSPACE_SESSION_TTL_DAYS`, default 30).
- `last_seen`: in memory on each frame; written on attach and detach.

## Step 3: migration

`lib/migrate_identity.py`, run once at startup, guarded by
`<data>/identity-version`. Converts agent records, topic members and message
senders and mentions, PR subscriber files, and callback directories, as in
the spec. Tests build a data directory in the old format by hand.

## Step 4: protocol package

Deprecation comments, new optional fields (`account`, `takeover`,
`connections`), new meta types (`multiple_connections`, `handle_lost`), and
the `whoami` frame. `scripts/protocol-compat.py` must report minor.

## Step 5: accounts (core)

- `lib/accounts.py`: parse `accounts.toml` (stdlib `tomllib`), token parsing
  and verification, the built-in default account, permissions, channel
  patterns, limits, `max_severity`.
- Auth at connect; reload on mtime change (checked at connect and every few
  seconds); close connections whose credential is gone or expired.
- Permission checks in dispatch, channel patterns on join, post, and
  set_channel_topic, limits, body size, severity cap, `account` in meta,
  `whoami`.
- `scripts/new-credential.py`.

## Step 6: end-to-end tests for the core

Spawn the server (the harness already supports this) and drive it with
WebSocket clients: attach, handles, takeover, cross-account DMs and
rendering, permissions, revocation, multiple connections, migration of a
real old-format data directory.

## Step 7: gateway

`server/subspace-gateway.py` (PEP 723, locked): MCP SDK v2 `MCPServer`,
Streamable HTTP under Starlette and uvicorn. Bearer and Origin middleware,
body limit, attach, pull delivery with at-least-once acks,
`wait_for_messages`, idle lease, tool set from `whoami`. Tests: in-process
gateway against a spawned core, with the SDK's MCP client.

## Step 8: image and docs

A second venv and entry point in the Dockerfile, `server/README.md`
sections for accounts and the gateway, and a guide for remote clients.

## Out of scope here

- Relay changes in wlr-cc-plugins (identity follow-ups and the
  other-account threshold).
- The repository allowlist for PR tools. Until it exists, the `pr`
  permission gives access to every repository that the server's tokens can
  see, and accounts in the file default to no `pr` permission.
