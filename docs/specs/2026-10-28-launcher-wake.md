# Launcher wake-up

Status: draft. Date: 2026-10-28.

Extends "Webhook wake-up" in
[2026-10-10-remote-agent-mcp.md](2026-10-10-remote-agent-mcp.md). On the
hyperspace side, it replaces the transport in "Messages to a session" of
hyperspace's `docs/specs/2026-10-15-session-status.md`.

## Problem

Subspace can wake an offline session through a webhook, but only for a handle
that `accounts.toml` lists. Local Claude Code sessions take their handles at run
time, so no static list covers them.

Hyperspace's launcher can resume a stopped Claude Code session
(`claude --bg --resume <id>`), but it does not know when a message arrives. It
makes only outbound requests: runner pods have no Service, and workstations can
be behind NAT. So the launcher cannot be the webhook itself.

## Design

```
subspace core ──POST wake──▶ hyperspace server ──session state──▶ launcher
  (offline session,           (records a wake          (next tick: resume
   unread message)             request on the            the session)
                               session)
```

The hyperspace server receives the webhook, because it already has an ingress
and knows the session's status and lease. It records a _wake request_ on the
session. The launcher of the host that holds the lease sees the request in the
session list that it already reads on each tick, and resumes the session. The
resumed session connects to subspace through the relay, which puts the messages
into its first turn.

The session id is the key across both systems. For a relay session, the subspace
session id is the Claude Code session id, which is also hyperspace's session id.
No handle-to-session map is necessary.

**Requirement: a resume keeps the session id.** Subspace keeps a session's state
(channels, acks, thresholds, handle, wake state) under its id. A wake is useful
only if the resumed process connects under the same id. Hyperspace is
responsible for this in each resume that it does, on every host kind and in
every launcher mode. Today `claude --bg --resume <sid>` keeps the id (checked
with Claude Code 2.1.301, see hyperspace's session-status plan). If a later
Claude Code version breaks this, hyperspace must fix its resume, not subspace. A
user's `/background` in a running session makes a new id; that is not a resume
that hyperspace does, and subspace sees a new session.

## Subspace changes

**Account webhook.** An `[[account.webhook]]` entry with no `handle` is the
account's webhook: it applies to each session in the account. An account has at
most one. An entry with a `handle` continues to apply to the session that holds
that handle, and takes precedence over the account webhook.

```toml
[[account]]
id = "default"

[[account.webhook]]   # no handle: every session in the account
url = "https://hyperspace.example/v1/wake/claude-code"
key_file = "/etc/subspace-keys/hyperspace"
```

A session can match the account webhook with no handle. Today a session without
a handle cannot have a message that counts for a wake (channels and DMs need a
handle), so behavior does not change. The config does not stop a later change
that lets other events (PR events, callbacks) cause a wake.

**Body.** The wake body gets `session`, the session id. `handle` stays, and is
`""` if the session has none. A receiver should use `session` to find the agent.

```json
{
  "event": "wake",
  "account": "default",
  "session": "5f0c…",
  "handle": "reviewer",
  "pending": 2,
  "items": [ … ]
}
```

**Presence.** `wakeable` in `list_agents` is true if the session's handle has a
webhook or the account has one. It means that a webhook exists, not that the
receiver can start the agent: for example, a workstation with no `--stay`
launcher accepts the wake but does not resume the session.

**Responses.** No change in the rules: a 2xx is accepted, a 5xx, 429, or network
error is tried again, and any other status is a refusal. The core logs the
body's `message` for a refusal. Notices to the sender (not delivered, queued)
stay a later step.

Protocol: `session` in the body is a minor addition. The wire frames do not
change.

## Hyperspace changes

### Endpoint

`POST /v1/wake/{harness}`, with the subspace wake body. It needs a credential
with a new scope, `wake`, which allows only this endpoint. The session is
`(credential's account, harness, body.session)`. The subspace `account` and
`handle` fields are for logs only.

| Session state                   | Response                                          |
| ------------------------------- | ------------------------------------------------- |
| Not found                       | `404`                                             |
| `done`                          | `409`, `message: "session done: <note>"`          |
| Lease `free` (no host holds it) | `409`, `message: "no host holds the session"`     |
| `live` on a host                | `200`, `{"verdict": "live"}`; records a request   |
| Else                            | `200`, `{"verdict": "waking"}`; records a request |

The server records a request for a `live` session too, because liveness can be
one heartbeat old: a session that stopped without its SessionEnd hook shows
`live` until the next tick. A `reserved` lease (a teleport in flight) also gets
a request: the launcher of the target host acts on it when that host holds the
lease.

Each status apart from `done` gets a wake. `ready` is included: the agent ended
its turn, and the message can be what it waits for. Stopping a session from
waking is what `done` is for.

### Wake request

The server keeps at most one wake request for each session: `{received, count}`
(the wake's `pending`). A new wake replaces it. The request is _pending_ while
`received` is later than each time that a host listed the session in a heartbeat
(`last_listed`), and the session is not `done`. The session summary shows
`wake_request` only while it is pending.

Pending is a value that the server calculates when it reads the session, as it
does for liveness, so nothing clears a request. A process that runs, or that the
resume started, is listed at the next heartbeat, and the request is then not
pending. The launcher does not acknowledge a request. Status events have no
effect: a client can send a queued status write from before the wake after the
wake. A request that the launcher cannot act on (for example, no local copy)
stays pending, and the launcher tries again on later ticks within its resume
limit.

### Launcher

The launcher lists the sessions that its host holds in runner mode, as today,
and also in workstation mode with `--stay`. In workstation mode it resumes only
for a pending wake request, not for `working` or `waiting` + `wake: local`.

`needs_process` resumes a held, non-live session with a pending wake request,
whatever its status, and also if it has no status. The prompt is the status
prompt if the session also needs a process for its status (runner mode,
`working` or `waiting` + `wake: local`), and then a fixed suffix, for example:
"[hyperspace] Messages arrived while this session was stopped." The prompt does
not hold the messages and does not tell the agent to fetch them: the relay
delivers them in the same turn (see "Delivery at resume").

The existing limits apply: the launcher does not resume a session that runs,
whose restore is pending, or that it resumed 5 times in the last hour.

Hyperspace's session-status spec states the id requirement above as a launcher
rule, next to the resume command. The launcher always loads the relay as a
channel in a resumed process: runner images do this today through
`launcher_claude_args`.

## Delivery at resume

The agent gets the pending messages in its first turn after the resume, before
it acts, with no tool call. Each message gets into the context once.

The relay (the notifications plugin) does this, not hyperspace. Hyperspace does
not read or carry message content, and it has no subspace credential; the relay
already holds the session's connection and its acks.

- When the relay connects, it sends `register` and then `whoami`. The core sends
  the `whoami` reply after the pending items, so the relay then has the whole
  backlog.
- The relay holds this first backlog for a short time (some seconds) and does
  not send it as channel notifications yet.
- The relay's SessionStart hook, for a resumed session, asks the relay process
  for the held backlog (local IPC, found as the hook finds the session id
  today). It waits for the backlog for a short time. It returns the messages as
  `additionalContext`, with a header that says that they arrived while the
  session was stopped. The relay then acks them.
- Messages that the hook did not take (no hook, a timeout) go out as channel
  notifications as usual, after the hold.

The hook connects to the relay, not to the core: each connection to a session
gets each unacked message, so a second connection would put the messages in the
context twice.

This works for each resume, also an interactive `claude --resume` by a user.

To check before implementation: in `claude --bg --resume`, Claude Code starts
the relay's MCP server while the SessionStart hook runs, not after it. If it
starts it after, the hook does the same work on the first UserPromptSubmit
instead.

## How the rules fit

- **Grace and tick.** Subspace waits `grace_seconds` (60) after the session
  disconnects. The launcher acts on its next tick (up to 60 s). A wake thus
  takes up to about 2 minutes. A session that a user stopped, and that gets a
  message during the grace time, resumes.
- **One wake for a burst.** Subspace sends at most one wake for each
  `cooldown_seconds` (300) while the session stays offline, and the server keeps
  one request. A burst of messages gives one resume.
- **Covered messages.** Subspace treats a message as covered after the POST and
  its retries, whatever the response. If no POST is accepted, the next new
  message gives a new wake; the old ones wait until the session connects for
  another reason. After an accepted POST, the request stays on the hyperspace
  server while it is pending (see "Wake request").
- **Two copies.** The launcher's existing checks (not `live` on the server, not
  running locally) stop a resume that would make a second copy.

## Later: events other than messages

This spec wakes a session only for messages. Wakes for other events will follow,
with the same rules (offline, threshold, grace, cooldown, covered):

- GitHub and Forgejo PR events;
- cron-style wake-ups that the subspace server holds;
- Slack messages in followed channels, threads, and DMs;
- scopes that a private deployment adds as plugins.

Slack and plugin scopes have their own sender and recipient identities. Thus
subspace will probably need a more pluggable message and event model. This spec
does not design it. Its parts are not specific to messages: the account webhook
also matches a session with no handle, the wake body lists pointers by
`context`, and delivery at resume takes what the relay holds, whatever its kind.

**PR polling.** Today the core polls a PR only while a subscriber is connected
(`_tracker_loop`), so it does not see an event for an offline session. With PR
event wakes, a subscriber that has a webhook counts as connected for this check.
To stop polling for a session that will not wake, the core uses the webhook's
answer, not a copy of the hyperspace state: a refusal (for example `409` for a
`done` session) makes the session _dormant_, which is not wakeable and does not
count for polling, until it connects again. The cost is one polled event for
each `done` session. The session expiry (30 days) limits the rest.

## Not in this spec

- Notices to the sender, and `reopen` for a `done` session.
- Session ids as the primary identity in subspace. This design does not depend
  on handles, so it does not block that change.
- Starting a runner that does not run (scale to zero).
