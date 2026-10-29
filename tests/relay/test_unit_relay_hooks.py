# vim: filetype=python
"""Unit tests for the plugin's hook scripts: the PostCompact compaction-audit hook
and the SessionStart hook's handoff of messages that arrived while the session
was stopped (a fake relay socket stands in for the MCP server)."""

import asyncio
import importlib.util
import io
import json
import tempfile
from pathlib import Path
from typing import Self

import pytest

import session_state

HOOKS = Path(__file__).resolve().parents[2] / "plugin" / "subspace" / "hooks"


def _load_hook(name: str):
    path = HOOKS / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hook = _load_hook("compaction-audit")
start = _load_hook("session-start")


def test_build_record_full_payload():
    payload = {
        "hook_event_name": "PostCompact",
        "session_id": "sid-1",
        "trigger": "auto",
        "transcript_path": "/somewhere/t.jsonl",  # present but not read (post-hook race)
    }
    assert hook.build_record(payload, now=1234.5) == {
        "event": "PostCompact",
        "ts": 1234.5,
        "session_id": "sid-1",
        "trigger": "auto",
    }


def test_build_record_matcher_fallback_and_defaults():
    # No hook_event_name -> defaults to PostCompact; "matcher" read when "trigger" absent.
    record = hook.build_record({"matcher": "manual", "session_id": "s"}, now=1.0)
    assert record["event"] == "PostCompact"
    assert record["trigger"] == "manual"
    assert record["session_id"] == "s"


def test_build_record_missing_trigger_is_none():
    record = hook.build_record({"session_id": "s"}, now=1.0)
    assert record["trigger"] is None  # neither trigger nor matcher present


def test_append_record_creates_dir_and_appends(tmp_path):
    audit = tmp_path / "nested" / "audit.jsonl"  # parent does not exist yet
    hook.append_record(audit, {"event": "PostCompact", "n": 1})
    hook.append_record(audit, {"event": "PostCompact", "n": 2})
    lines = audit.read_text().splitlines()
    assert [json.loads(line)["n"] for line in lines] == [1, 2]  # ordered append


def test_audit_path_env_override(monkeypatch, tmp_path):
    override = tmp_path / "custom-audit.jsonl"
    monkeypatch.setenv("NOTIFICATIONS_COMPACTION_AUDIT_FILE", str(override))
    assert hook._audit_path() == override


def test_audit_path_default_under_home(monkeypatch):
    monkeypatch.delenv("NOTIFICATIONS_COMPACTION_AUDIT_FILE", raising=False)
    assert hook._audit_path().name == "notifications-compaction-audit.jsonl"
    assert ".claude" in hook._audit_path().parts


# --------------------------------------------------------------------------- #
# SessionStart: handoff of the messages that arrived while the session was stopped
# --------------------------------------------------------------------------- #


@pytest.fixture
def short_dir():
    """Unix socket paths are limited to about 100 bytes; pytest's tmp_path is
    often longer than that."""
    directory = Path(tempfile.mkdtemp(prefix="nt-"))
    yield directory
    for entry in directory.rglob("*"):
        if not entry.is_dir():
            entry.unlink()
    for entry in sorted(directory.rglob("*"), reverse=True):
        entry.rmdir()
    directory.rmdir()


class FakeRelay:
    """The relay's side of the hook socket: records what the hook sends and
    answers a claim with `reply`."""

    def __init__(self, path: Path, reply: dict | None) -> None:
        self.path = path
        self.reply = reply
        self.received: list[dict] = []
        self.closed = asyncio.Event()
        self._server: asyncio.AbstractServer | None = None

    async def __aenter__(self) -> Self:
        self._server = await asyncio.start_unix_server(self._serve, str(self.path))
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while line := await reader.readline():
                message = json.loads(line)
                self.received.append(message)
                if message.get("type") == "claim" and self.reply is not None:
                    writer.write(json.dumps(self.reply).encode() + b"\n")
                    await writer.drain()
        finally:
            writer.close()
            self.closed.set()


def test_resume_claims_and_confirms_after_writing(short_dir):
    path = short_dir / "1.sock"
    emitted: list[str] = []
    reply = {
        "type": "claimed",
        "count": 2,
        "content": "#room alice: hi\n\n[dm] bob: hey",
    }

    async def scenario() -> bool:
        async with FakeRelay(path, reply) as relay:
            done = await start.handoff(path, "sid-1", "resume", emitted.append)
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert relay.received == [
                {"type": "claim", "session_id": "sid-1", "source": "resume"},
                {"type": "done"},
            ]
            return done

    assert asyncio.run(scenario()) is True
    assert len(emitted) == 1
    output = json.loads(emitted[0])
    context = output["hookSpecificOutput"]["additionalContext"]
    assert output["hookSpecificOutput"]["hookEventName"] == "SessionStart"
    assert "while this session was stopped" in context
    assert context.endswith("#room alice: hi\n\n[dm] bob: hey")


def test_resume_with_nothing_held_writes_nothing(short_dir):
    path = short_dir / "1.sock"
    emitted: list[str] = []
    reply = {"type": "claimed", "count": 0, "content": ""}

    async def scenario() -> bool:
        async with FakeRelay(path, reply) as relay:
            done = await start.handoff(path, "sid-1", "resume", emitted.append)
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert [m["type"] for m in relay.received] == ["claim"]  # no "done"
            return done

    assert asyncio.run(scenario()) is False
    assert emitted == []


def test_other_starts_decline(short_dir):
    path = short_dir / "1.sock"
    emitted: list[str] = []

    async def scenario() -> bool:
        async with FakeRelay(path, None) as relay:
            done = await start.handoff(path, "sid-1", "startup", emitted.append)
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert relay.received == [
                {"type": "decline", "session_id": "sid-1", "source": "startup"}
            ]
            return done

    assert asyncio.run(scenario()) is False
    assert emitted == []


def test_no_relay_socket_gives_up_in_time(short_dir, monkeypatch):
    monkeypatch.setattr(start, "CLAIM_WAIT_SECONDS", 0.3)
    emitted: list[str] = []
    done = asyncio.run(
        start.handoff(short_dir / "none.sock", "sid-1", "resume", emitted.append)
    )
    assert done is False
    assert emitted == []


def test_relay_that_never_answers_gives_up_in_time(short_dir, monkeypatch):
    monkeypatch.setattr(start, "CLAIM_WAIT_SECONDS", 0.3)
    path = short_dir / "1.sock"
    emitted: list[str] = []

    async def scenario() -> bool:
        async with FakeRelay(path, None):
            return await start.handoff(path, "sid-1", "resume", emitted.append)

    assert asyncio.run(scenario()) is False
    assert emitted == []


def test_main_writes_state_file_and_context(short_dir, monkeypatch, capsys):
    """The whole hook: the state file for the relay's session id, then the claim
    on the socket keyed by the same Claude Code pid."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(short_dir))
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    monkeypatch.setattr(session_state, "reap_stale", lambda: None)
    path = session_state.ipc_path(4242)
    path.parent.mkdir(parents=True, mode=0o700)
    payload = {"session_id": "sid-9", "source": "resume", "hook_event_name": "x"}
    monkeypatch.setattr(start.sys, "stdin", io.StringIO(json.dumps(payload)))
    reply = {"type": "claimed", "count": 1, "content": "#room alice: hi"}

    # The fake relay must share the loop with main()'s asyncio.run, so run the
    # hook in a worker thread while the loop serves.
    async def scenario() -> list[dict]:
        async with FakeRelay(path, reply) as relay:
            await asyncio.to_thread(start.main)
            await asyncio.wait_for(relay.closed.wait(), 5)
            return relay.received

    received = asyncio.run(scenario())
    assert [m["type"] for m in received] == ["claim", "done"]
    assert session_state.read_session(4242) == "sid-9"
    output = json.loads(capsys.readouterr().out)
    assert output["hookSpecificOutput"]["additionalContext"].endswith("alice: hi")


def test_main_skips_a_state_dir_that_is_not_private(short_dir, monkeypatch, capsys):
    """A socket in a directory another user could own may be a fake relay that
    injects context: the hook writes nothing and claims nothing."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(short_dir))
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    path = session_state.ipc_path(4242)
    path.parent.mkdir(parents=True, mode=0o755)
    payload = {"session_id": "sid-9", "source": "resume"}
    monkeypatch.setattr(start.sys, "stdin", io.StringIO(json.dumps(payload)))
    reply = {"type": "claimed", "count": 1, "content": "injected"}

    async def scenario() -> list[dict]:
        async with FakeRelay(path, reply) as relay:
            await asyncio.to_thread(start.main)
            return relay.received

    assert asyncio.run(scenario()) == []
    assert session_state.read_session(4242) is None
    assert capsys.readouterr().out == ""
