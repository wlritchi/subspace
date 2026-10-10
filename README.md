# subspace

A persistent WebSocket server that owns notification
state for Claude Code sessions. It schedules callbacks, monitors GitHub and
Forgejo pull requests, and carries agent-to-agent messages. The per-session stdio
MCP relay in the `notifications` plugin
([wlr-cc-plugins](https://github.com/wlritchi/wlr-cc-plugins)) connects to it.

- `server/` — the server script (PEP 723, locked deps), systemd unit, and
  configuration reference ([server/README.md](server/README.md))
- `lib/` — modules the server imports from `../lib`
- `protocol/` — the `subspace-protocol` package: the wire protocol as typed
  definitions, for the server and its clients ([protocol/README.md](protocol/README.md))
- `scripts/` — one-off maintenance scripts
- `tests/` — unit tests
- `Dockerfile` — container image, built from the repository root

## Tests

```bash
uv run -qs tests/run.py
```

## Run

```bash
GITHUB_TOKEN=$(gh auth token) uv run -qs server/subspace-server.py
```

## Releases

Each merge to `main` releases the server image. CI picks the next `X.Y.Z` from
the [Conventional Commits](https://www.conventionalcommits.org/) subjects since
the last release (`feat` is minor, `!` or `BREAKING CHANGE` is major, anything
else is a patch), pushes the tag, and then builds and pushes the image. Before
1.0.0, a breaking change bumps the minor version; push a `1.0.0` tag by hand to
leave 0.x. To merge without a release, put `[skip release]` in the merge commit
message.

To build a release again, run the workflow by hand on the release tag, or push
an `X.Y.Z` tag yourself.

The `subspace-protocol` package has its own `protocol-v*` tags, which do not
trigger an image build. When a merge to `main` changes the package, CI runs
`scripts/protocol-compat.py` against the last protocol tag and pushes the next
one: major for a breaking change, minor for a compatible change to the types,
patch otherwise. A commit that touches `protocol/` and is marked breaking
forces a major bump. Check a change locally with
`scripts/protocol-compat.py protocol-v<last>`.

CI needs a `REGISTRY` repository variable (the image goes to
`<REGISTRY>/<owner>/<repo>`) and `REGISTRY_USERNAME` / `REGISTRY_PASSWORD`
secrets.
