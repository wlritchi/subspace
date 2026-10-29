# vim: filetype=python
"""Shared session-id correlation between the subspace MCP relay and the
SessionStart hook.

Background: when a session is resumed (`/resume`, or `--resume` with the picker),
the CLAUDE_CODE_SESSION_ID handed to the already-spawned stdio MCP server is the
*temporary* id from before the user picked a session. There is no in-band way for
Claude Code to tell the running server the id changed.

The SessionStart hook *does* receive the real session id. It and the server share
exactly one correlatable thing: the Claude Code process they both descend from.
That pid is not exposed as an env var (the server's getppid() is `uv`, with
Claude Code as the grandparent), so each side discovers it by walking its own
parent chain until it finds the Claude Code process. The hook then writes the
session id to a per-instance state file keyed by that pid, and the server reads
it on demand, falling back to the env var.

stdlib only, so the hook can run without building a dependency environment.
"""

import json
import os
import stat
import subprocess
import tempfile
import time
from pathlib import Path

SESSION_ID_ENV_VAR = "CLAUDE_CODE_SESSION_ID"

_MAX_ANCESTOR_DEPTH = 16


def _read_proc(pid: int, name: str) -> bytes | None:
    """Read /proc/<pid>/<name>, or None if unavailable (non-Linux, gone, denied)."""
    try:
        return Path("/proc", str(pid), name).read_bytes()
    except OSError:
        return None


def _proc_start_time(pid: int) -> int | None:
    """Return the process start time (clock ticks since boot) for `pid`, or None
    if unavailable (non-Linux, gone, unreadable, or unparseable).

    Together with the pid this uniquely identifies a process, so it lets us detect
    a recycled pid: a state file written by a now-dead process holding `pid` will
    record a different start time than whatever process holds `pid` now.
    """
    stat = _read_proc(pid, "stat")
    if stat is None:
        return None
    content = stat.decode(errors="replace")
    # Field 2 (comm) is paren-wrapped and may itself contain spaces or parens, so
    # split *after* the final ')'. starttime is overall field 22; with field 3 at
    # index 0, that is index 19 (field N -> fields[N - 3]).
    try:
        rest = content[content.rindex(")") + 1 :]
    except ValueError:
        return None
    fields = rest.split()
    if len(fields) < 20:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


def _parent_pid(pid: int) -> int | None:
    """Return the parent pid of `pid`, via /proc with a `ps` fallback."""
    status = _read_proc(pid, "status")
    if status is not None:
        for line in status.decode(errors="replace").splitlines():
            if line.startswith("PPid:"):
                try:
                    return int(line.split()[1])
                except (IndexError, ValueError):
                    return None
        return None
    try:
        out = subprocess.run(
            ["ps", "-o", "ppid=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
        )
        return int(out.stdout.strip())
    except (OSError, ValueError):
        return None


def _looks_like_claude(pid: int) -> bool:
    """Best-effort: does `pid` look like the Claude Code CLI process?"""
    comm = _read_proc(pid, "comm")
    if comm is not None:
        if comm.decode(errors="replace").strip() == "claude":
            return True
        cmdline = _read_proc(pid, "cmdline")
        argv = cmdline.split(b"\x00") if cmdline else []
    else:
        try:
            res = subprocess.run(
                ["ps", "-o", "args=", "-p", str(pid)],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            return False
        argv = [part.encode() for part in res.stdout.split()]

    if not argv or not argv[0]:
        return False
    argv0 = os.path.basename(argv[0].decode(errors="replace"))
    joined = b" ".join(argv).decode(errors="replace")
    return argv0 == "claude" or "claude-code" in joined or "claude/cli" in joined


def resolve_claude_pid(start: int | None = None) -> int | None:
    """Walk up from `start` (default: this process) to the nearest Claude Code
    ancestor pid, or None if it can't be found (feature then degrades to no-op)."""
    cur = _parent_pid(os.getpid() if start is None else start)
    seen: set[int] = set()
    for _ in range(_MAX_ANCESTOR_DEPTH):
        if cur is None or cur <= 1 or cur in seen:
            return None
        if _looks_like_claude(cur):
            return cur
        seen.add(cur)
        cur = _parent_pid(cur)
    return None


def state_dir() -> Path:
    """Per-user directory holding the session-id state files."""
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isdir(xdg):
        return Path(xdg) / "claude-notifications"
    suffix = f"-{os.getuid()}" if hasattr(os, "getuid") else ""
    return Path(tempfile.gettempdir()) / f"claude-notifications{suffix}"


def secure_state_dir() -> Path | None:
    """The state directory, made if necessary, or None if it is not private to
    this user. The path is predictable (under the temp dir when XDG_RUNTIME_DIR
    is unset), so another local user could make it first and reach the relay's
    socket or the state files; a caller then does not use it at all."""
    if not hasattr(os, "getuid"):
        return None
    directory = state_dir()
    try:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = directory.lstat()
    except OSError:
        return None
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        return None
    return directory


def _state_path(claude_pid: int) -> Path:
    return state_dir() / f"{claude_pid}.json"


def ipc_path(claude_pid: int) -> Path:
    """The local socket on which the relay of this Claude Code process takes the
    SessionStart hook's claim of the messages that arrived while it was stopped."""
    return state_dir() / f"{claude_pid}.sock"


def write_session(
    claude_pid: int, session_id: str, source: str | None = None
) -> Path | None:
    """Atomically record `session_id` for the given Claude Code pid. Returns None,
    and writes nothing, if the state directory is not private to this user."""
    if secure_state_dir() is None:
        return None

    path = _state_path(claude_pid)
    payload: dict[str, object] = {
        "session_id": session_id,
        "claude_pid": claude_pid,
        "start_time": _proc_start_time(claude_pid),
    }
    if source:
        payload["source"] = source

    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, path)
    return path


def read_session(claude_pid: int) -> str | None:
    """Read the recorded session id for the given Claude Code pid, if any.

    Guards against pid recycling: if the file records the process start time and
    it no longer matches the process currently holding `claude_pid`, the file was
    written by a *different* process that previously held this pid, so we ignore
    it. A missing start time (older format) or an unreadable current start time
    (non-Linux) means no guard is available, so we accept the file as before.

    Refuses a directory that is not private to this user: another local user
    could plant a file there and choose the session the relay registers.
    """
    if secure_state_dir() is None:
        return None
    try:
        data = json.loads(_state_path(claude_pid).read_text())
    except (OSError, ValueError):
        return None
    recorded_start = data.get("start_time")
    if recorded_start is not None:
        current_start = _proc_start_time(claude_pid)
        if current_start is not None and current_start != recorded_start:
            return None  # stale: pid was recycled
    session_id = data.get("session_id")
    return session_id or None


def state_file_age(claude_pid: int) -> float | None:
    """Seconds since the hook wrote this pid's state file, or None if there is
    no valid file (the hook has not run yet, or the pid was recycled)."""
    if read_session(claude_pid) is None:
        return None
    try:
        written = _state_path(claude_pid).stat().st_mtime
    except OSError:
        return None
    return max(0.0, time.time() - written)


def effective_session_id() -> tuple[str | None, str]:
    """Resolve the best-known session id and a human-readable source label.

    State file (keyed by the Claude Code pid) wins over the env var, because on a
    resumed session the env var holds the stale temporary id.
    """
    claude_pid = resolve_claude_pid()
    if claude_pid is not None:
        session_id = read_session(claude_pid)
        if session_id:
            return session_id, f"state file (claude pid {claude_pid})"

    env = os.environ.get(SESSION_ID_ENV_VAR)
    if env:
        return env, f"env ({SESSION_ID_ENV_VAR})"
    return None, "unavailable"


def known_session_ids() -> list[str]:
    """Every id this process may run under: the state-file id and the env id.

    They differ on a resumed session, and the harness can tag its log lines with
    either one, so a caller that matches log lines should accept both."""
    ids: list[str] = []
    claude_pid = resolve_claude_pid()
    if claude_pid is not None:
        session_id = read_session(claude_pid)
        if session_id:
            ids.append(session_id)
    env = os.environ.get(SESSION_ID_ENV_VAR)
    if env and env not in ids:
        ids.append(env)
    return ids


def _pid_alive(pid: int) -> bool:
    if Path("/proc", str(pid)).exists():
        return True
    if _read_proc(pid, "comm") is not None:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def reap_stale() -> None:
    """Best-effort cleanup of state files for Claude Code pids that have exited."""
    directory = state_dir()
    if not directory.is_dir():
        return
    for entry in [*directory.glob("*.json"), *directory.glob("*.sock")]:
        try:
            pid = int(entry.stem)
        except ValueError:
            continue
        if not _pid_alive(pid):
            try:
                entry.unlink()
            except OSError:
                pass
