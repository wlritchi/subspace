# vim: filetype=python
"""Unit tests for the plugin's hook scripts: the PostCompact compaction-audit hook,
and the delivery at resume of the messages that arrived while the session was
stopped: the SessionStart hook says the start is a resume, and the
UserPromptSubmit hook claims them on the first prompt (a fake relay socket
stands in for the MCP server)."""

import asyncio
import importlib.util
import io
import json
import os
import tempfile
import time
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
prompt = _load_hook("prompt-submit")


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
# Delivery at resume: SessionStart says "resume", the first prompt claims
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
    """The relay's side of the hook socket: records what the hooks send and
    answers a claim with `reply`."""

    def __init__(self, path: Path, reply: dict | None) -> None:
        self.path = path
        self.reply = reply
        self.received: list[dict] = []
        self.connections = 0
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
        self.connections += 1
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


def test_resume_tells_the_relay_and_claims_nothing(short_dir):
    """SessionStart context reaches the model only with a turn, and a resume
    with no prompt starts none. So the hook does not claim at resume."""
    path = short_dir / "1.sock"

    async def scenario() -> bool:
        async with FakeRelay(
            path, {"type": "claimed", "count": 1, "content": "x"}
        ) as relay:
            sent = await start.handoff(path, "sid-1", "resume")
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert relay.received == [
                {"type": "resume", "session_id": "sid-1", "source": "resume"}
            ]
            return sent

    assert asyncio.run(scenario()) is True


def test_other_starts_decline(short_dir):
    path = short_dir / "1.sock"

    async def scenario() -> bool:
        async with FakeRelay(path, None) as relay:
            sent = await start.handoff(path, "sid-1", "startup")
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert relay.received == [
                {"type": "decline", "session_id": "sid-1", "source": "startup"}
            ]
            return sent

    assert asyncio.run(scenario()) is True


def test_session_start_without_relay_gives_up_in_time(short_dir, monkeypatch):
    monkeypatch.setattr(start, "NOTIFY_WAIT_SECONDS", 0.3)
    began = time.monotonic()
    sent = asyncio.run(start.handoff(short_dir / "none.sock", "sid-1", "resume"))
    assert sent is False
    assert time.monotonic() - began < 2.0


def _run_session_start(monkeypatch, payload: dict) -> None:
    monkeypatch.setattr(start.sys, "stdin", io.StringIO(json.dumps(payload)))
    start.main()


@pytest.fixture
def state_dir(short_dir, monkeypatch):
    """A private state directory for the Claude Code process 4242 (this test
    process stands in for it, so the pid-recycling guard sees the same start
    time on both sides)."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(short_dir))
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    monkeypatch.setattr(session_state, "reap_stale", lambda: None)
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 111)
    monkeypatch.setattr(start, "NOTIFY_WAIT_SECONDS", 0.2)
    path = session_state.ipc_path(4242)
    path.parent.mkdir(parents=True, mode=0o700)
    return path


def test_session_start_main_marks_a_resume(state_dir, monkeypatch, capsys):
    """The whole SessionStart hook: the state file for the relay's session id,
    the resume mark for the first prompt, then "resume" on the socket keyed by
    the same Claude Code pid. It writes no context."""

    async def scenario() -> list[dict]:
        async with FakeRelay(state_dir, None) as relay:
            payload = {"session_id": "sid-9", "source": "resume"}
            await asyncio.to_thread(_run_session_start, monkeypatch, payload)
            await asyncio.wait_for(relay.closed.wait(), 5)
            return relay.received

    received = asyncio.run(scenario())
    assert [m["type"] for m in received] == ["resume"]
    assert session_state.read_session(4242) == "sid-9"
    assert capsys.readouterr().out == ""
    mark = session_state.take_resume(4242)
    assert mark is not None and mark[0] == "sid-9"


def test_session_start_main_clears_the_mark_for_other_starts(state_dir, monkeypatch):
    session_state.mark_resume(4242, "sid-9")
    _run_session_start(monkeypatch, {"session_id": "sid-9", "source": "clear"})
    assert session_state.take_resume(4242) is None


def test_session_start_skips_a_state_dir_that_is_not_private(
    short_dir, monkeypatch, capsys
):
    """A socket in a directory another user could own may be a fake relay: the
    hook writes nothing and sends nothing."""
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(short_dir))
    monkeypatch.setattr(session_state, "resolve_claude_pid", lambda: 4242)
    path = session_state.ipc_path(4242)
    path.parent.mkdir(parents=True, mode=0o755)

    async def scenario() -> list[dict]:
        async with FakeRelay(path, None) as relay:
            payload = {"session_id": "sid-9", "source": "resume"}
            await asyncio.to_thread(_run_session_start, monkeypatch, payload)
            return relay.received

    assert asyncio.run(scenario()) == []
    assert session_state.read_session(4242) is None
    assert not (path.parent / "4242.resume").exists()
    assert capsys.readouterr().out == ""


def test_prompt_claims_and_confirms_after_writing(short_dir):
    path = short_dir / "1.sock"
    emitted: list[str] = []
    reply = {
        "type": "claimed",
        "count": 2,
        "content": "#room alice: hi\n\n[dm] bob: hey",
    }

    async def scenario() -> bool:
        async with FakeRelay(path, reply) as relay:
            done = await prompt.claim(path, "sid-1", emitted.append)
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert relay.received == [
                {"type": "claim", "session_id": "sid-1", "source": "prompt"},
                {"type": "done"},
            ]
            return done

    assert asyncio.run(scenario()) is True
    assert len(emitted) == 1
    output = json.loads(emitted[0])
    context = output["hookSpecificOutput"]["additionalContext"]
    assert output["hookSpecificOutput"]["hookEventName"] == "UserPromptSubmit"
    assert "while this session was stopped" in context
    assert context.endswith("#room alice: hi\n\n[dm] bob: hey")


def test_prompt_with_nothing_held_writes_nothing(short_dir):
    path = short_dir / "1.sock"
    emitted: list[str] = []
    reply = {"type": "claimed", "count": 0, "content": ""}

    async def scenario() -> bool:
        async with FakeRelay(path, reply) as relay:
            done = await prompt.claim(path, "sid-1", emitted.append)
            await asyncio.wait_for(relay.closed.wait(), 5)
            assert [m["type"] for m in relay.received] == ["claim"]  # no "done"
            return done

    assert asyncio.run(scenario()) is False
    assert emitted == []


def test_prompt_without_relay_gives_up_in_time(short_dir, monkeypatch):
    monkeypatch.setattr(prompt, "CLAIM_WAIT_SECONDS", 0.3)
    emitted: list[str] = []
    began = time.monotonic()
    done = asyncio.run(prompt.claim(short_dir / "none.sock", "sid-1", emitted.append))
    assert done is False
    assert emitted == []
    assert time.monotonic() - began < 2.0


def test_prompt_long_after_the_resume_tries_the_relay_once(short_dir):
    """Long after the resume, a missing socket means no relay is coming: the
    prompt does not wait for one."""
    began = time.monotonic()
    done = asyncio.run(
        prompt.claim(short_dir / "none.sock", "sid-1", print, connect_seconds=-50.0)
    )
    assert done is False
    assert time.monotonic() - began < 0.5


def test_relay_that_never_answers_gives_up_in_time(short_dir, monkeypatch):
    monkeypatch.setattr(prompt, "CLAIM_WAIT_SECONDS", 0.3)
    path = short_dir / "1.sock"
    emitted: list[str] = []

    async def scenario() -> bool:
        async with FakeRelay(path, None):
            return await prompt.claim(path, "sid-1", emitted.append)

    assert asyncio.run(scenario()) is False
    assert emitted == []


def _run_prompt(monkeypatch, session_id: str) -> None:
    payload = {
        "session_id": session_id,
        "hook_event_name": "UserPromptSubmit",
        "prompt": "go on",
    }
    monkeypatch.setattr(prompt.sys, "stdin", io.StringIO(json.dumps(payload)))
    prompt.main()


def test_prompt_main_claims_once_after_a_resume(state_dir, monkeypatch, capsys):
    """The whole UserPromptSubmit hook: the first prompt after the resume mark
    claims on the socket keyed by the Claude Code pid; the next prompt does
    not contact the relay."""
    session_state.mark_resume(4242, "sid-9")
    reply = {"type": "claimed", "count": 1, "content": "#room alice: hi"}

    # The fake relay must share the loop with main()'s asyncio.run, so run the
    # hook in a worker thread while the loop serves.
    async def scenario() -> FakeRelay:
        async with FakeRelay(state_dir, reply) as relay:
            await asyncio.to_thread(_run_prompt, monkeypatch, "sid-9")
            await asyncio.wait_for(relay.closed.wait(), 5)
            await asyncio.to_thread(_run_prompt, monkeypatch, "sid-9")
            await asyncio.sleep(0.1)
            return relay

    relay = asyncio.run(scenario())
    assert [m["type"] for m in relay.received] == ["claim", "done"]
    assert relay.connections == 1
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    output = json.loads(lines[0])
    assert output["hookSpecificOutput"]["hookEventName"] == "UserPromptSubmit"
    assert output["hookSpecificOutput"]["additionalContext"].endswith("alice: hi")


def test_prompt_main_with_no_resume_is_a_fast_no_op(state_dir, monkeypatch, capsys):
    """No resume mark: the hook does not contact the relay, and does not wait
    for a relay that is not there."""
    monkeypatch.setattr(prompt, "CLAIM_WAIT_SECONDS", 5.0)

    async def scenario() -> tuple[FakeRelay, float]:
        async with FakeRelay(state_dir, {"type": "claimed", "count": 0}) as relay:
            began = time.monotonic()
            await asyncio.to_thread(_run_prompt, monkeypatch, "sid-9")
            return relay, time.monotonic() - began

    relay, elapsed = asyncio.run(scenario())
    assert relay.connections == 0
    assert elapsed < 0.5
    began = time.monotonic()
    _run_prompt(monkeypatch, "sid-9")  # no socket at all
    assert time.monotonic() - began < 0.5
    assert capsys.readouterr().out == ""


def test_prompt_main_ignores_a_mark_of_another_session(state_dir, monkeypatch):
    session_state.mark_resume(4242, "sid-old")

    async def scenario() -> FakeRelay:
        async with FakeRelay(state_dir, {"type": "claimed", "count": 0}) as relay:
            await asyncio.to_thread(_run_prompt, monkeypatch, "sid-9")
            return relay

    assert asyncio.run(scenario()).connections == 0
    assert session_state.take_resume(4242) is None  # used up


def test_resume_mark_of_a_recycled_pid_is_not_valid(state_dir, monkeypatch):
    session_state.mark_resume(4242, "sid-9")
    monkeypatch.setattr(session_state, "_proc_start_time", lambda pid: 222)
    assert session_state.take_resume(4242) is None


def test_take_resume_keeps_a_newer_mark(state_dir, monkeypatch):
    """A newer resume can replace the mark while a prompt takes the old one.
    The take moves the mark away before it reads it, so it never removes the
    newer mark."""
    session_state.mark_resume(4242, "sid-old")
    real_rename = os.rename

    def rename_then_resume(src: str | Path, dst: str | Path) -> None:
        real_rename(src, dst)
        session_state.mark_resume(4242, "sid-new")

    monkeypatch.setattr(session_state.os, "rename", rename_then_resume)
    mark = session_state.take_resume(4242)
    monkeypatch.setattr(session_state.os, "rename", real_rename)
    assert mark is not None and mark[0] == "sid-old"
    mark = session_state.take_resume(4242)
    assert mark is not None and mark[0] == "sid-new"
    assert session_state.take_resume(4242) is None
    assert list(state_dir.parent.glob("*.taken-*")) == []
