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

A session that holds a handle wakes for its messages in channels and DMs. A
session with no handle wakes for **DM messages only**. A session whose handle
another session took keeps its channel memberships and unread messages, so
without this rule each old run would resume for every `@here` in its channels.
It keeps its DM threads too, but a new message reaches an old thread only when a
sender addresses the session by its id (`session:<id>`, see the identity spec).
That is the case this rule is for: a session can DM another session in its
account by id, for example to ask an old session to accept a handoff, and the
wake starts it although it has no handle. The wake body then has
`"handle": ""`. The account webhook itself does not depend on a handle, so a
later change can let other events (PR events, callbacks) wake a session with no
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

The claim is made when a turn starts, on the first prompt, and not at
SessionStart. Hook context reaches the model only with a turn. A resume can
start with no prompt: a fleet view that reopens a stopped background session,
or `claude --resume <sid>` with nobody at the keyboard. A claim at SessionStart
would ack the messages, so the core would show them as delivered, but no turn
comes and the model never sees them. Nothing is then unacked, so no wake
follows, and a stop of the session loses them.

- **Hold.** At start, and at each change of the session that it registers, the
  relay does not send messages as channel notifications (its push flush
  included) until its SessionStart hook declines or a claim ends. If no hook
  contacts it within about 15 s, it stops the hold. After a change, the relay
  drops each `notify` whose `session_id` is the earlier session's. At start, the
  relay holds only if the hook has not yet written the session state file, or
  wrote it recently: a relay that an MCP reconnect restarts later has no hook
  coming. The age of the process does not tell this, because a hot-spare worker
  is old when its relay starts. A change of session also discards the local
  buffers of the earlier session; their messages stay unacked in the core. This
  applies to the change that a resume or a claim makes and to the change that
  the existing session-id watcher makes. A shorter hold never cuts a longer one
  that is on.
- **Resume.** The relay listens on a local socket at a path keyed by the Claude
  Code process id, which the hooks and the relay all find for the session state
  file. On a resume, the SessionStart hook writes a resume mark (a file in the
  state directory, with the session id) and then sends `resume` with its
  session id on the socket. It claims nothing and writes no context. The relay
  holds for about 30 s from then, to wait for the first prompt: a prompt on
  the command line comes when all SessionStart hooks end, which can take some
  seconds. If the id is not the one that the relay registered (after `/resume`
  in a running session, the relay started under the earlier session's id), the
  relay registers it at once, so the core sends that session's pending items
  during the hold. A relay that is not yet connected connects at once, not
  after its startup sleep.
- **Claim.** The UserPromptSubmit hook runs on each prompt. It takes the resume
  mark away; if there was no mark, or the mark is of another session, it does
  nothing. So only the first prompt after a resume contacts the relay, and the
  other prompts cost a file check. The hook sends its session id on the socket
  and waits up to about 10 s: the relay can still be connecting, and `whoami`
  can take 5 s. This wait blocks only that first prompt. If the session is a
  channel and its hold has ended, the relay answers at once with nothing: the
  messages went out as channel notifications. Otherwise the relay registers the
  id if it is not the registered one, sends `whoami` (the core sends the reply
  after the items that were pending, or after 5 s), and answers the hook with
  the held messages that pass the session's threshold. Messages below the
  threshold stay held for `catch_up`. The claim takes the messages out of channel
  delivery, so the two paths cannot both send one message.
- **Output.** The hook returns the messages as UserPromptSubmit
  `additionalContext`, with a header that says that they arrived while the
  session was stopped. After it has written its output, it tells the relay, and
  the relay acks them. If the IPC closes before that, the relay keeps the
  messages unacked and sends them as channel notifications.
- **No prompt.** If no prompt comes before the hold ends, nothing claims, and
  the relay sends the messages as channel notifications. A channel notification
  starts a turn, so the agent sees them. The relay acks them only then. In a
  session that is not a channel they stay held for `catch_up`, and the first
  prompt still claims them.
- **Decline.** For a start that is not a resume, the SessionStart hook removes
  any resume mark and declines, and the relay stops the hold at once.

The hooks connect to the relay, not to the core: each connection to a session
gets each unacked message, so a second connection would put the messages in the
context twice.

This works for each resume, also an interactive `claude --resume` by a user.
When the user types the first prompt within the hold, the messages come with
it; later, they come as channel notifications.

Checked on 2026-10-28 with Claude Code 2.1.301, for `claude --bg --resume`
(cold, and into a `bg-spare` worker), `claude --resume <sid>`, and `/resume` in
a running session:

- Claude Code starts the MCP servers while the SessionStart hook runs, and the
  first prompt waits until the hook ends.
- UserPromptSubmit runs for the prompt on the command line, which is how the
  launcher resumes a session.
- The relay's environment has the resumed session id in each case except
  `/resume` in a running session: there the MCP servers keep the id of the
  session that started the process. The hook payloads have the right id in each
  case, so the resume and the claim carry it.

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

## Done sessions

A session that is `done` takes no messages. The launcher stops a done session's
process when its turn ends, so every message to it goes through the wake.
Hyperspace refuses that wake with:

```
409 {"error": "session_done", "verdict": "done",
     "detail": "session done: <note>", "note": "<note>"}
```

`note` is absent when the done has no note. No other `409` has a `verdict`.

**Bounce.** On that refusal, subspace posts one system line in each DM thread
that the wake covered:
`#N to session:<id> not delivered yet: session done: <note>`. The senders learn
where the work went (the note is often a handoff, for example "handed off to
de7c9105"). The line is a message in the thread, not a one-time notice, so a
sender that is offline gets it when it connects. The done session does not get
the line, so the line cannot cause another wake. Channel messages do not bounce:
they are not addressed to the session. Each message bounces at most once,
because the wake covers it. Any other `409` is only logged.

**The messages stay.** Subspace does not acknowledge a message for its
recipient. If the session is reopened, it gets the messages that it was sent
while it was done. If it is never reopened, the session expiry removes them.

**Reopen.** A DM can carry `reopen`: a one-line reason (at most 200
characters) to reopen the recipient. It needs exactly one recipient, in the
sender's account. When a wake covers such a message, the body gets:

```json
"reopen": {"reason": "<the dm's reopen>", "by": "<the sender's session id>"}
```

If the wake covers more than one such message, the newest one gives the
`reopen`. A wake with `reopen` does not wait for the cooldown, because the
refusal that it answers usually started one. For a done session, hyperspace
records an explicit reopen (status `ready`, source agent, session `by`, note
`reopened: <reason>`), then handles the wake as usual. For a session that is
not done, it ignores `reopen`. The wake credential needs the `reopen` scope as
well as `wake`; without it, hyperspace answers `403`, which subspace only logs.

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
