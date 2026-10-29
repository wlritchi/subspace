# Launcher wake-up

Status: draft. Date: 2026-10-28.

Extends "Webhook wake-up" in
[2026-10-10-remote-agent-mcp.md](2026-10-10-remote-agent-mcp.md). On the
hyperspace side, see hyperspace's `docs/specs/2026-10-28-wake.md`.

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

A message wake still needs a session that holds a handle, as today. A session
whose handle another session took keeps its channel memberships and unread
messages, so without this rule each old run would resume for every `@here` in
its channels. The account webhook itself does not depend on a handle, so a later
change can let other events (PR events, callbacks) wake a session with no
handle.

**Body.** The wake body gets `session`, the session id. `handle` is the
session's current handle, not the webhook entry's. A receiver should use
`session` to find the agent.

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
body's `verdict`, `detail`, or `message`. Notices to the sender (not delivered,
queued) stay a later step.

**Session on each notify.** Each `notify` frame gets `session_id`: the session
that the frame is for, in the form that the client registered. A client that
registers another session on its connection (the relay after `/resume` or
`/clear`) can then drop a frame for the earlier session that was still in
transit. That frame stays unacked under its own session.

Protocol: `session` in the wake body and `session_id` on `notify` are minor
additions.

## Hyperspace side

Hyperspace's spec, `docs/specs/2026-10-28-wake.md` in the hyperspace repository,
owns the server and launcher details. Subspace depends only on this:

- `POST /v1/wake/{harness}` with the body above, and a credential with the
  `wake` scope (the key file). Hyperspace finds the session by the credential's
  account, the harness, and `session`.
- `200` with a `verdict` means accepted. `waking`: a launcher that acts on wakes
  holds the lease. `queued`: the request is recorded, but no such launcher runs
  there now (later, the sender gets a "queued" notice). `live`: the session
  shows as running. Subspace treats all three the same.
- `409` means that the session's state refuses this wake (`done`, or no host
  holds the session). It refuses this wake only: the state can change with no
  subspace connection (a `reopen` or a restore from a shell), so a later message
  gives a new wake after the cooldown. `404`: hyperspace does not know the
  session.
- An accepted wake stays as a request until a new run of the session starts, so
  a run that was live with a disconnected relay does not use it up. The launcher
  resumes the session with its status prompt, if any, and a fixed suffix that
  says that messages arrived while it was stopped. The prompt has no message
  content. The launcher always loads the relay as a channel.

Hyperspace never reads or keeps message content: it ignores `items`.

## Delivery at resume

The agent gets the pending messages in its first turn after the resume, before
it acts, with no tool call. This is best effort: a message that misses the
handoff arrives as a channel notification, as today. In normal operation each
message gets into the context once.

The relay (the notifications plugin) does this, not hyperspace. Hyperspace does
not read or carry message content, and it has no subspace credential; the relay
already holds the session's connection and its acks.

- **Hold.** At start, and at each change of the session that it registers, the
  relay does not send messages as channel notifications (its push flush
  included) until its SessionStart hook claims them or declines. If no hook
  contacts it within about 15 s, it stops the hold. After a change, the relay
  drops each `notify` whose `session_id` is the earlier session's. At start, the
  relay holds only if the hook has not yet written the session state file, or
  wrote it recently: a relay that an MCP reconnect restarts later has no hook
  coming. The age of the process does not tell this, because a hot-spare worker
  is old when its relay starts. A claim also starts a hold before the relay
  connects. A change of session also discards the local buffers of the earlier
  session; their messages stay unacked in the core. This applies to the change
  that a claim makes and to the change that the existing session-id watcher
  makes.
- **Claim.** The relay listens on a local socket at a path keyed by the Claude
  Code process id, which the hook and the relay both find today for the session
  state file. The hook sends its session id there, and waits up to about 10 s:
  the relay sleeps 3 s before it connects, and `whoami` can take 5 s. If the id
  is not the one that the relay registered (after `/resume` in a running
  session, the relay started under the earlier session's id), the relay
  registers it. If the watcher already made that change, the claim takes its
  hold. The relay then sends `whoami`; the core sends the reply after the items
  that were pending, or after 5 s. The relay answers the hook with the held
  messages that pass the session's threshold. Messages below the threshold stay
  held, as today. The claim takes the messages out of channel delivery, so the
  two paths cannot both send one message.
- **Output.** The hook returns the messages as `additionalContext`, with a
  header that says that they arrived while the session was stopped. After it has
  written its output, it tells the relay, and the relay acks them. If the IPC
  closes before that, the relay keeps the messages unacked and sends them as
  channel notifications.
- **Decline.** For a start that is not a resume, the hook declines, and the
  relay stops the hold at once.

The hook connects to the relay, not to the core: each connection to a session
gets each unacked message, so a second connection would put the messages in the
context twice.

This works for each resume, also an interactive `claude --resume` by a user.

Checked on 2026-10-28 with Claude Code 2.1.301, for `claude --bg --resume`
(cold, and into a `bg-spare` worker), `claude --resume <sid>`, and `/resume` in
a running session:

- Claude Code starts the MCP servers while the SessionStart hook runs, and the
  first prompt waits until the hook ends. So the hook can wait for the relay,
  and its `additionalContext` comes before the first turn.
- UserPromptSubmit runs for the prompt on the command line.
- The relay's environment has the resumed session id in each case except
  `/resume` in a running session: there the MCP servers keep the id of the
  session that started the process. The hook payload has the right id in each
  case, so the claim carries it.

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
  message gives a new wake after the cooldown; the old ones wait until the
  session connects for another reason. After an accepted POST, the request stays
  on the hyperspace server until a new run of the session starts.
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
`context`, and delivery at resume takes the held items that pass the threshold,
whatever their kind.

**PR polling.** Today the core polls a PR only while a subscriber is connected
(`_tracker_loop`), so it does not see an event for an offline session. With PR
event wakes, a subscriber that has a webhook counts as connected for this check,
unless the last wake of that session got a `409`. The core uses the webhook's
answer, not a copy of the hyperspace state. A `409` refuses that wake only, so
message wakes continue after it; a later accepted wake, or a connect, makes the
session count for polling again. The cost is one polled event for each `done`
session. The session expiry (30 days) limits the rest.

Only `409` has this effect: it says that the session's state refuses the wake.
Other refusals (`401`, `403`, `404`) are usually configuration errors that an
operator corrects. An infrastructure failure does not cause a `409`: a runner
that stops keeps its leases, so its wakes wait as requests, and a hyperspace
server that stops gives errors that the core tries again. Thus no sender needs
to force a wake; a sender that must reach a `done` session uses `reopen` (see
"Not in this spec").

## Not in this spec

- Notices to the sender, and `reopen` for a `done` session.
- Session ids as the primary identity in subspace. This design does not depend
  on handles, so it does not block that change.
- Starting a runner that does not run (scale to zero).
