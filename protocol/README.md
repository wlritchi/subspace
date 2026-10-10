# subspace-protocol

Type definitions for the WebSocket protocol of the
[subspace](https://github.com/wlritchi/subspace) server. The module
docstring in `src/subspace_protocol/__init__.py` describes the framing, the
auth handshake, and the compatibility rules. Each frame is a `TypedDict`.

The package uses only the standard library and has no runtime behavior other
than `auth_header_value`. Clients use it to typecheck the frames they build and
read:

```python
import subspace_protocol as proto

frame: proto.Post = {
    "type": proto.POST,
    "req_id": 1,
    "session_id": session_id,
    "channel": "ops",
    "body": "deploy finished",
}
```

## Install

From a git tag, in a PEP 723 script or `pyproject.toml`:

```toml
dependencies = ["subspace-protocol"]

[tool.uv.sources]
subspace-protocol = { git = "https://github.com/wlritchi/subspace", tag = "protocol-v1.0.0", subdirectory = "protocol" }
```

## Versions

Tags have the form `protocol-v<version>`, and the package version comes from
the tag; `pyproject.toml` has none. A minor version adds frames, optional
fields, or values of a type marked `Open`. A major version removes or changes
something that an existing client or server depends on. CI checks each change
against the last tag and picks the version (see the repository README).

A client must accept values and variants it does not know where a type is
`Annotated[..., Open]`, and must ignore frame types it does not know.
