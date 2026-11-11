# Relay credentials

Status: draft. Date: 2026-11-11.

This spec changes how the relay (the `subspace` plugin) gets the server URL
and its credential. Read [2026-10-10-identity.md](2026-10-10-identity.md) for
accounts and credentials.

## Problem

The relay reads the server URL and its credential only from environment
variables and one file:

- `NOTIFICATIONS_WS_URL`, or `NOTIFICATIONS_WS_HOST` and
  `NOTIFICATIONS_WS_PORT`, give the URL. The default is
  `ws://127.0.0.1:8137`.
- `NOTIFICATIONS_TOKEN` gives the credential. If it is not set, the relay
  reads `<NOTIFICATIONS_DATA_DIR>/token`. The default data directory is
  `~/.claude/notifications`.

This causes three problems:

1. **The credential is in the environment of every process.** An MCP
   server gets its environment from Claude Code. So the variable must be
   set for Claude Code itself, in a shell profile or in a container's
   environment. Every command that the agent runs inherits it. A command
   that prints the environment, or a child process that logs it, exposes
   the credential.
2. **The names are from before the rename.** The server uses the
   `SUBSPACE_` prefix. The relay still uses `NOTIFICATIONS_`.
3. **A missing credential fails silently.** If the token file does not
   exist, the relay creates it with a random value. The server then refuses
   the connection. The relay does not handle a refusal, so the user sees
   "the subspace server is not reachable". Nothing tells the user that the
   credential is missing or wrong.

## Goals

- Claude Code stores the URL and the credential for the plugin. It gives
  them only to the relay process.
- The two settings get `SUBSPACE_` names.
- Existing installations keep working for a deprecation period.
- A missing or refused credential gives an error that tells the user what
  to set.
- A relay and a server on one machine need no setup.

## Non-goals

- **OAuth.** Claude Code supports OAuth only for MCP servers that it
  reaches over HTTP. A channel server is a local subprocess that Claude Code
  reaches over stdio. So the relay cannot use Claude Code's OAuth. The
  gateway's token-paste OAuth (see
  [2026-10-10-remote-agent-mcp.md](2026-10-10-remote-agent-mcp.md)) is for
  remote clients. It does not change.
- **Direct access to the system keyring from the relay.** Claude Code's
  secure storage (next section) is the store. A keyring client in the relay
  needs its own prompt handling, so that a locked keyring cannot stop the
  relay at startup.
- **Other relay settings.** The other `NOTIFICATIONS_` settings (data
  directory, debounce, catch-up limits, and others) keep their names. The
  server already uses `SUBSPACE_DATA_DIR`. So a new name for the relay's
  data directory setting needs a different prefix. That is a later change.
- **Gateway clients.** No change.

## Plugin options

The plugin manifest declares two options in `userConfig`:

```json
"userConfig": {
  "server_url": {
    "type": "string",
    "title": "Server URL",
    "description": "WebSocket URL of the subspace server, for example wss://subspace.example.com. Leave empty for a server on this machine.",
    "required": false
  },
  "token": {
    "type": "string",
    "title": "Credential",
    "description": "The token that the relay connects with: an ssp_ credential, or the server's master token.",
    "required": false,
    "sensitive": true
  }
}
```

The plugin's `.mcp.json` gives the values to the relay under internal
names:

```json
"env": {
  "SUBSPACE_OPTION_WS_URL": "${user_config.server_url}",
  "SUBSPACE_OPTION_TOKEN": "${user_config.token}"
}
```

The internal names are not for users. Claude Code always sets them. If an
option is not set, Claude Code sets its variable to an empty string. A
variable in a server's `env` replaces a variable of the same name from
Claude Code's environment. So if `.mcp.json` mapped an option to
`SUBSPACE_TOKEN`, an unset option would erase a `SUBSPACE_TOKEN` that the
user set.

### Storage

Claude Code stores the values as follows:

- **Sensitive (`token`):**
  - On Linux, in `.credentials.json` in the Claude Code configuration
    directory (`~/.claude`, or `CLAUDE_CONFIG_DIR`), mode `0600`. That file
    is plain text. Claude Code's own login is in the same file. Measured on
    Claude Code 2.1.301.
  - On macOS, in the Keychain, according to Claude Code's documentation.
    Not measured.
- **Not sensitive (`server_url`):** in the user `settings.json`, under
  `pluginConfigs["subspace@subspace"].options`. Measured on Claude Code
  2.1.301.

### Setup

- **Interactive:** Claude Code asks for the options when the plugin is
  enabled. `/plugin configure subspace@subspace` changes them later.
- **Not interactive:** this command reads a JSON object of strings from
  stdin, for example `{"server_url": "wss://…", "token": "ssp_…"}`.
  Measured on Claude Code 2.1.301.

  ```bash
  claude plugin configure subspace@subspace --values-stdin
  ```

- A change takes effect when Claude Code restarts.

## Resolution order

The relay takes the first value that is set and not empty. It resolves the
sources again before each connection attempt, so a change to a file or a
link applies at the next reconnect.

**URL:**

1. `SUBSPACE_OPTION_WS_URL` (the `server_url` option)
2. `SUBSPACE_WS_URL`
3. `NOTIFICATIONS_WS_URL` (deprecated)
4. `NOTIFICATIONS_WS_HOST` and `NOTIFICATIONS_WS_PORT` (deprecated). Either
   one selects this step, and the other takes its default.
5. `ws://127.0.0.1:8137`

**Credential:**

1. `SUBSPACE_OPTION_TOKEN` (the `token` option)
2. `SUBSPACE_TOKEN`
3. `NOTIFICATIONS_TOKEN` (deprecated)
4. `<NOTIFICATIONS_DATA_DIR>/token` (deprecated). The relay uses the file
   only if it is a regular file after it follows links, the relay can read
   it, and it is not empty. A dangling link counts as no file.
5. The server's token file, only if no source gives a URL. See
   [The server's token file](#the-servers-token-file).
6. None: see [A missing or refused credential](#a-missing-or-refused-credential).

### The server's token file

A server and a relay on one machine need no setup. When the server has no
`SUBSPACE_TOKEN`, it creates its master token in `<data dir>/token`. The
relay finds that file with the server's rule: `SUBSPACE_DATA_DIR`, else
`$XDG_DATA_HOME/subspace`, else `~/.local/share/subspace`. The file rules
of step 4 apply.

The relay uses this step only if no source gives a URL, so that the URL is
the default `ws://127.0.0.1:8137`. If the user sets any URL, the user also
sets a credential. A check of the host name is not sufficient:

- A name such as `localhost` can resolve to an address on another machine.
- A local port can be a forward to a remote server.
- Two servers on one machine can listen on different ports, and the file
  belongs to only one of them.

In each case, the master token would go to a server that does not own it.
The default URL is a literal address, so no name is resolved. The default
port is the port that the server listens on when it has no configuration.

For a connection that uses this step, the relay does not use a proxy. The
`websockets` client otherwise reads a proxy from the environment.

This step is not deprecated. The file holds the master token, so the relay
acts in the default account. That is the expected setup on a machine with
one user.

This step trusts the process that listens on `127.0.0.1:8137`. The relay
does not authenticate that process as the server that owns the file. Any
local process can bind the port before the server starts, and then it gets
the master token. On a machine where other users or untrusted programs run,
set a credential explicitly and set the URL. A URL setting turns this step
off.

The server creates the file when it starts. If the relay starts first, the
file is not there yet. The relay resolves the sources again before each
connection attempt, so it finds the file at a later attempt.

### Why the options come first

A plugin option applies only to this plugin. An environment variable is
inherited, and can be meant for a different program. The server reads
`SUBSPACE_TOKEN` as its master token. Suppose a user exports the master
token in a shell to start a server, and then starts Claude Code from the
same shell. If the environment came first, the relay would connect with the
master token in place of the account credential in the option. It would
then act in the default account, with every permission.

To override an option, change the option (`/plugin configure` or
`--values-stdin`), or clear it.

### Logs and redirects

At startup, and when a source changes, the relay logs the source of each
value. For example: `subspace relay: URL from the server_url option,
credential from SUBSPACE_TOKEN`. These rules apply:

- The relay never logs a credential.
- The relay does not follow a redirect from the server. A redirect sends
  the credential again, to an address that the user did not configure. The
  `websockets` client follows redirects by default. The relay treats a
  redirect as a failed connection, and logs the status without the
  `Location` value.
- Logs and tool errors show a URL without its user information, query, and
  fragment.
- If the URL and the credential come from different kinds of source (one
  from an option, the other from the environment or a token file), the
  relay logs a warning. Then a credential is not sent to an address that
  was configured for a different one without a notice. The default URL
  does not count as a source.
- A deprecated name logs one line that gives its replacement. For
  `NOTIFICATIONS_WS_HOST` and `NOTIFICATIONS_WS_PORT`, the line gives the
  equivalent URL, for example `use SUBSPACE_WS_URL=ws://127.0.0.1:9000`.

### Shared names with the server

`SUBSPACE_TOKEN` has the same name on the relay and on the server, with a
related meaning: the credential that this process uses. On the server it is
the master token. On the relay it is any credential. That can be the master
token, or an `ssp_` credential of an account.

The relay does not read `SUBSPACE_WS_HOST` or `SUBSPACE_WS_PORT`. On the
server, those names give the address that the server listens on. The
address that the relay connects to can be different. So the relay takes
only the full URL.

The relay reads `SUBSPACE_DATA_DIR` only to find the server's token file.
That is the same meaning as on the server.

## A missing or refused credential

The relay no longer creates its token file. It reads the file only under
the rules in step 4. A moved local installation can have that file as a
link to the server's token (`scripts/migrate-from-notifications.py` makes
the link). That setup keeps working. With step 5, a local setup does not
need the link.

**No credential.** If no source gives a credential, the relay does not
connect. It logs the error below at startup, so a user who never calls a
tool can also find it. Each tool call returns the same error:

```
subspace: no credential. Set the plugin's token option
(/plugin configure subspace@subspace), or SUBSPACE_TOKEN.
The relay also read the server's token file
(/home/user/.local/share/subspace/token): it does not exist.
For a server on this machine, start the server first.
```

The last three lines appear only if step 5 applies. They give the path
that the relay read and the reason that the file was not usable: it does
not exist, it is a dangling link, it is not a regular file, it cannot be
read, or it is empty. The advice to start the server appears only if the
file does not exist.

**Refused credential.** The relay treats two events as a refusal:

- The handshake fails with `401` or `403`. A proxy or a gateway in front of
  the server can refuse this way.
- The server closes the connection with its unauthorized close code. The
  server accepts the handshake, and then closes with this code if it refuses
  the credential. It also closes with this code if it revokes the credential
  later.

For a refusal, the relay logs the refusal and the source of the credential.
Each tool call then returns, for example:

```
subspace: the server at wss://subspace.example.com refused the
credential from the token file (~/.claude/notifications/token).
```

The relay keeps its reconnect loop, so a corrected file or link applies at
the next attempt. These two errors replace the "server is not reachable"
error for their cases. The "not reachable" error names `SUBSPACE_WS_URL`
and the `server_url` option in place of `NOTIFICATIONS_WS_URL`.

An existing installation can have a token file with a random value that an
earlier relay created. That installation gets the "refused" error, which
names the file.

## Exposure

If the credential is set only in the plugin option, it is not in the
environment of the agent's commands, of hooks, or of other MCP servers.
This prevents accidental exposure: in an environment dump, in a child
process's logs, or in an error report.

The option does not remove a variable from the environment. If
`SUBSPACE_TOKEN` or `NOTIFICATIONS_TOKEN` is still set for Claude Code, the
credential is still exposed, although the relay does not use it. To get the
benefit, a deployment sets the option and removes both variables from
Claude Code's environment. Then it restarts Claude Code.

The option is not a boundary against the agent itself. The agent's
commands run as the same user as Claude Code, and can read
`.credentials.json`. On macOS, the same user can usually read the Keychain
item too. A credential that an agent must not read needs a process that the
agent cannot control. That is out of scope.

## Deprecation

1. **This change:** the new names, the options, and the error handling. The
   deprecated names and the token file keep working. Each logs a
   deprecation line.
2. **Deployments switch:**
   - Each deployment sets the options when it provisions Claude Code, or
     sets `SUBSPACE_WS_URL` and `SUBSPACE_TOKEN`.
   - It stops setting the `NOTIFICATIONS_` names.
   - To keep the credential out of the environment, it uses the option and
     sets no token variable.
3. **A local installation** needs no change if the server keeps its
   default port and data directory, and the relay has no URL setting: step
   5 finds the token. If the server uses `SUBSPACE_DATA_DIR`, set the same
   value for Claude Code. If the relay has a URL setting, for a different
   port, also set the token option or `SUBSPACE_TOKEN`.
4. **A later release** removes the deprecated names and step 4. It needs a
   date only after deployments have switched.

## Tests

**Unit, resolution order:**

- Each step for the URL and the credential.
- The option alone gives the value.
- An empty option with `SUBSPACE_TOKEN` set gives `SUBSPACE_TOKEN`. An empty
  value counts as not set.
- An option with `SUBSPACE_TOKEN` also set gives the option.
- Mixed sources log the warning.
- A deprecated name selects its step and logs its replacement.

**Unit, token file:** a custom `NOTIFICATIONS_DATA_DIR`. A link to an
existing file. A dangling link. An empty file. An unreadable file.

**Unit, the server's token file:**

- The default URL with no other source uses the file.
- `SUBSPACE_DATA_DIR` and `XDG_DATA_HOME` move it.
- A URL from any source does not use the file, even if the file exists and
  the URL is `ws://127.0.0.1:8137` or `ws://localhost:8137`. The result is
  the "no credential" error without the step 5 lines.
- With a proxy set in the environment, a step 5 connection does not use
  it.
- Step 4 comes before step 5.
- A file that appears after the relay starts is used at the next attempt.
- The error gives the right reason for a missing file, a dangling link, a
  directory, an unreadable file, and an empty file.

**Unit, redirects:** a server that answers the handshake with a redirect.
The relay does not connect to the `Location`, does not send the credential
again, and does not log the `Location` value.

**Unit, errors:**

- No credential: the relay does not create the file and does not connect.
  The startup log and a tool call give the error.
- A `401` handshake gives the "refused" error with the right source.
- A close with the server's unauthorized close code gives the same error:
  right after connect, and later on a live connection.

**Unit, secrets:** a sentinel credential, and a URL with user information
and a query. Neither the credential nor those URL parts appear in the logs
or in tool errors.

**Relay harness:**

- Most tests set `SUBSPACE_WS_URL` and `SUBSPACE_TOKEN`.
- One end-to-end test connects with only `SUBSPACE_OPTION_WS_URL` and
  `SUBSPACE_OPTION_TOKEN`.
- One keeps the deprecated `NOTIFICATIONS_WS_PORT` and
  `NOTIFICATIONS_TOKEN`.

**Manifest:**

- `.mcp.json` `env` maps exactly `SUBSPACE_OPTION_WS_URL` to
  `${user_config.server_url}` and `SUBSPACE_OPTION_TOKEN` to
  `${user_config.token}`. It sets no `SUBSPACE_TOKEN`, `SUBSPACE_WS_URL`, or
  `NOTIFICATIONS_` name.
- Each referenced option is in `userConfig`.
- The token option is sensitive.

## Documentation

- `server/README.md`:
  - The relay section gives the new names, the options, and
    non-interactive setup. It says that a relay and a server on one machine
    need no setup.
  - The token section drops the advice to set `SUBSPACE_TOKEN` and
    `NOTIFICATIONS_TOKEN` to the same value.
  - The migration section drops the advice to rename every `NOTIFICATIONS_`
    variable to `SUBSPACE_`, because only the two relay settings in this
    spec change.
- `scripts/migrate-from-notifications.py`: the docstring says that the link
  is a deprecated credential source, not the only one.
- `tests/relay/_harness.py`: the comment on the master token gives the new
  name.

## Open questions

1. **When to remove the deprecated names.** This spec gives no date.
