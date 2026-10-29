#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""PostCompact hook for the subspace plugin: a compaction audit breadcrumb.

Fires just after Claude Code compacts the session context. Appends one JSONL line
per compaction recording the trigger (auto vs manual), the session id, and a
timestamp — a lightweight, greppable, per-session trail of "a compaction fired,
by this trigger, at this time" that aggregates across the fleet WITHOUT parsing
multi-hundred-MB transcripts. Its most direct use is trend data on manual /compact
load (trigger="manual") vs everything the harness compacts on its own.

What it deliberately does NOT try to capture, and why:
  - Context-size / effectiveness numbers. The exact token counts a compaction moved
    live in the transcript's own `compactMetadata` event
    ({trigger, preTokens, postTokens, durationMs}), which Claude Code finalizes
    AFTER this PostCompact hook has already run (verified empirically on 2.1.170) —
    so the hook cannot read its own compaction's metadata. Those counts remain exact
    in each session transcript and can be joined to this trail by session_id when
    precise per-event effectiveness is wanted.
  - The transcript FILE size is NOT a context-size proxy: the transcript is an
    append-only log, so it only grows across a compaction (the summary is appended).

Note on trigger for compact_session: an agent-initiated compact_session rides Claude
Code's AUTO executor, so it records trigger="auto" (NOT "manual") — the same label a
threshold auto-compaction gets. The two are told apart only by the transcript's
compactMetadata.preTokens (a self-compact fires well below the auto threshold), not
by this hook. The trigger here reliably separates MANUAL (/compact) from AUTO.

Entirely best-effort: any failure is swallowed so a compaction never fails because
of us. Fleet observability only.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import json
import os
import sys
import time
from pathlib import Path


def _audit_path() -> Path:
    """Where the audit log lives. Env override wins (tests point it at a tmp file);
    default is a persistent, PVC-mounted location beside the plugin's other
    client-side state (e.g. agent-identity.json)."""
    override = os.environ.get("NOTIFICATIONS_COMPACTION_AUDIT_FILE")
    if override:
        return Path(override)
    return Path.home() / ".claude" / "notifications-compaction-audit.jsonl"


def build_record(payload: dict, now: float) -> dict:
    """The audit line for one compaction event. Pure — the clock is injected so the
    record is testable without patching time. Every field comes from the hook payload,
    which is reliable (unlike the transcript's post-hook compactMetadata)."""
    return {
        "event": payload.get("hook_event_name") or "PostCompact",
        "ts": now,
        "session_id": payload.get("session_id"),
        # "manual" (an explicit /compact) vs "auto" (Claude Code's threshold-driven
        # compaction AND the compact_session tool, which rides the auto executor).
        "trigger": payload.get("trigger") or payload.get("matcher"),
    }


def append_record(path: Path, record: dict) -> None:
    """Append one JSONL line, creating the parent dir. Append mode keeps the log an
    ordered, greppable audit trail across a session's compactions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError, OSError):
        return
    append_record(_audit_path(), build_record(payload, time.time()))


if __name__ == "__main__":
    try:
        main()
    except Exception:  # noqa: BLE001, S110
        # Never let a hook failure interfere with compaction.
        pass
