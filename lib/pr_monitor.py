# vim: filetype=python
"""GitHub PR monitoring: snapshot, diff -> events, notification formatting, and
a per-PR tracker with on-disk persistence.

The pure functions (snapshot_from_api, diff, summarize and the formatters) carry
the "enough information to act without opening GitHub" requirement and are
unit-tested directly. Each event's id is a hash of a stable source identity (a
timeline node id, an object id, or a synthesized state key) rather than its
rendered text, so dedup is idempotent across restarts yet recurring transitions
that render identically (e.g. a label removed then re-added) stay distinct.
PRTracker holds the mutable per-PR state
(subscribers, per-subscriber acked id sets, cached events, last snapshot); the
server drives its polling. Persistence is split (state / append-only event log /
per-subscriber files) to keep write amplification low.
"""

import asyncio
import hashlib
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

import settings
import storage

SHORT_RANGE_LINES = 10  # inline comments at/under this many lines include the code
SHORT_BODY_CHARS = 600  # comment/review bodies at/under this are included in full
MAX_CACHED_EVENTS = 200
CHECK_SUMMARY_THRESHOLD_DEFAULT = 5  # collapse this many+ green completions per poll

_REVIEW_LABEL = {
    "APPROVED": "approved",
    "CHANGES_REQUESTED": "requested changes",
    "COMMENTED": "left review comments",
    "DISMISSED": "had a review dismissed",
}
_FAILED_CONCLUSIONS = {
    "failure",
    "timed_out",
    "action_required",
    "cancelled",
    "startup_failure",
}
_PR_REF_RE = re.compile(r"^\s*([^/\s]+)/([^/#\s]+)#(\d+)\s*$")


def pr_key(owner: str, repo: str, number: int | str) -> str:
    return f"{owner}/{repo}#{number}"


def storage_key(
    provider: str,
    owner: str,
    repo: str,
    number: int | str,
    instance: str | None = None,
) -> str:
    """The TRACKERS-dict / on-disk key. GitHub stays UNPREFIXED (byte-identical to the
    historical key, so existing trackers, dirs, and notification ids are unchanged);
    other providers get a "<provider>:" prefix so a Forgejo owner/repo#n cannot collide
    with a GitHub one. Only the storage key carries the prefix — the human-facing
    ``PRTracker.key`` (used in event text and tool replies) stays "owner/repo#number".
    ``_safe`` maps the ':' and '/' to '_', so the dir names stay distinct on disk.

    ``instance`` is the Forgejo multi-instance alias dimension: the DEFAULT instance
    (alias "" / None) stays ``forgejo:owner/repo#n`` — byte-identical to v1, so existing
    on-disk trackers need no migration — while a NAMED instance becomes
    ``forgejo:<alias>:owner/repo#n`` so the same owner/repo#n on two forges can't collide.
    The alias is ignored for github (which has no multi-instance dimension in v1)."""
    key = pr_key(owner, repo, number)
    if provider == "github":
        return key
    if instance:
        return f"{provider}:{instance}:{key}"
    return f"{provider}:{key}"


def parse_pr_ref(ref: str) -> tuple[str, str, int] | None:
    m = _PR_REF_RE.match(ref or "")
    if not m:
        return None
    return m.group(1), m.group(2), int(m.group(3))


# --------------------------------------------------------------------------- #
# snapshot + diff (pure)
# --------------------------------------------------------------------------- #


def snapshot_from_api(data: dict) -> dict:
    """Normalize a raw GitHub fetch into the comparable state we diff on."""
    pr = data.get("pr") or {}
    snap: dict = {
        "state": pr.get("state"),
        "merged": bool(pr.get("merged")),
        "merged_by": (pr.get("merged_by") or {}).get("login"),
        "mergeable_state": pr.get("mergeable_state"),
        "head_sha": (pr.get("head") or {}).get("sha"),
        "title": pr.get("title"),
        "url": pr.get("html_url"),
        "reviews": {},
        "review_comments": {},
        "issue_comments": {},
        "check_runs": {},
        "statuses": {},
    }
    for r in data.get("reviews") or []:
        if r.get("state") == "PENDING":
            continue
        snap["reviews"][str(r.get("id"))] = {
            "state": r.get("state"),
            "user": (r.get("user") or {}).get("login"),
            "body": r.get("body") or "",
            "url": r.get("html_url"),
        }
    for c in data.get("review_comments") or []:
        snap["review_comments"][str(c.get("id"))] = {
            "user": (c.get("user") or {}).get("login"),
            "body": c.get("body") or "",
            "path": c.get("path"),
            "line": c.get("line"),
            "start_line": c.get("start_line"),
            "original_line": c.get("original_line"),
            "original_start_line": c.get("original_start_line"),
            "diff_hunk": c.get("diff_hunk"),
            "url": c.get("html_url"),
        }
    for c in data.get("issue_comments") or []:
        snap["issue_comments"][str(c.get("id"))] = {
            "user": (c.get("user") or {}).get("login"),
            "body": c.get("body") or "",
            "url": c.get("html_url"),
        }
    for ch in data.get("check_runs") or []:
        out = ch.get("output") or {}
        snap["check_runs"][str(ch.get("id"))] = {
            "name": ch.get("name"),
            "status": ch.get("status"),
            "conclusion": ch.get("conclusion"),
            "url": ch.get("html_url"),
            "title": out.get("title"),
            "summary": out.get("summary"),
        }
    for s in (data.get("status") or {}).get("statuses") or []:
        snap["statuses"][s.get("context")] = {
            "state": s.get("state"),
            "url": s.get("target_url"),
            "desc": s.get("description"),
        }
    return snap


# Gitea/Forgejo review states -> the canonical states diff()/summarize() understand.
# PENDING (and REQUEST_REVIEW) are unsubmitted — excluded entirely, like GitHub's
# PENDING. Normalizing HERE keeps the snapshot provider-agnostic so the diff never
# learns a provider dialect.
_FORGEJO_REVIEW_STATE = {
    "APPROVED": "APPROVED",
    "REQUEST_CHANGES": "CHANGES_REQUESTED",
    "COMMENT": "COMMENTED",
    "DISMISSED": "DISMISSED",
}
_FORGEJO_REVIEW_SKIP = {"PENDING", "REQUEST_REVIEW", "", None}


def snapshot_from_forgejo(data: dict) -> dict:
    """Normalize a Forgejo/Gitea REST fetch into the same comparable snapshot that
    ``snapshot_from_graphql``/``snapshot_from_api`` produce, so the transport-agnostic
    ``diff()`` is unchanged. ``data`` is what ``forgejo_client.fetch_pr`` returns:
    ``{pr, reviews, review_comments, issue_comments, statuses}`` (raw Gitea JSON).

    Gitea field -> canonical snapshot mapping (the thing you can't re-derive without
    the Gitea swagger open):

      pr.state ("open"/"closed")              -> state
      pr.merged (bool)                        -> merged
      pr.merged_by.login                      -> merged_by (a login string, or None)
      pr.mergeable (bool | absent)            -> mergeable_state: True->"clean",
                                                 False->"dirty", None/absent->"unknown"
      pr.head.sha                             -> head_sha
      pr.title / pr.html_url                  -> title / url
      pr.draft (bool)                         -> draft
      pr.labels[].name                        -> labels (sorted)
      pr.requested_reviewers[].login          -> requested_reviewers (sorted)
      reviews[] (state via _FORGEJO_REVIEW_STATE; PENDING/REQUEST_REVIEW dropped)
        {id, state, user.login, body, html_url}-> reviews[id] = {state,user,body,url}
      review_comments[] (Gitea PullReviewComment; inline)
        {id, user.login, body, path, position, original_position, diff_hunk, html_url}
                                              -> review_comments[id] = {user, body, path,
                                                 line(=position), original_line, start_line,
                                                 original_start_line, diff_hunk, url}
        + comment.resolver (User set when the thread is resolved)
                                              -> review_threads[id] = {resolved, path, line}
      issue_comments[] {id, user.login, body, html_url}
                                              -> issue_comments[id] = {user, body, url}
      statuses[] (Gitea CommitStatus)
        {context, status, target_url, description}
                                              -> statuses[context] = {state, url, desc}

    Not populated for v1 (Gitea REST doesn't expose them): check_runs (Forgejo Actions
    "check runs" live behind a separate endpoint) and the timeline facets (labels/
    reviewer/draft/force-push changes — Gitea has no timeline). Those keys are present
    but empty; ``diff()`` .get()s them, so they simply produce no events. ``timeline``
    is intentionally an (empty) key so ``load_trackers`` doesn't treat the snapshot as a
    stale pre-timeline shape and drop it. review_threads is synthesized per inline
    comment (Gitea has no thread id), so a same-conversation multi-comment resolve emits
    one pr_thread event per comment — acceptable for v1.
    """
    pr = data.get("pr") or {}
    m = pr.get("mergeable")
    snap: dict = {
        "state": pr.get("state"),
        "merged": bool(pr.get("merged")),
        "merged_by": (pr.get("merged_by") or {}).get("login"),
        "mergeable_state": "unknown" if m is None else ("clean" if m else "dirty"),
        "head_sha": (pr.get("head") or {}).get("sha"),
        "title": pr.get("title"),
        "url": pr.get("html_url"),
        "draft": bool(pr.get("draft")),
        "labels": sorted(
            str(label["name"])
            for label in (pr.get("labels") or [])
            if label.get("name")
        ),
        "requested_reviewers": sorted(
            r["login"]
            for r in (pr.get("requested_reviewers") or [])
            if (r or {}).get("login")
        ),
        "reviews": {},
        "review_comments": {},
        "issue_comments": {},
        "check_runs": {},
        "statuses": {},
        "review_threads": {},
        "timeline": {},
    }
    for r in data.get("reviews") or []:
        state = r.get("state")
        if state in _FORGEJO_REVIEW_SKIP:
            continue
        snap["reviews"][str(r.get("id"))] = {
            "state": _FORGEJO_REVIEW_STATE.get(state, state),
            "user": (r.get("user") or {}).get("login"),
            "body": r.get("body") or "",
            "url": r.get("html_url"),
        }
    for c in data.get("review_comments") or []:
        cid = str(c.get("id"))
        # Gitea uses diff-relative `position`, not a file line; it feeds
        # _inline_comment_event's line/original_line the same way GitHub's do.
        line = c.get("position")
        snap["review_comments"][cid] = {
            "user": (c.get("user") or {}).get("login"),
            "body": c.get("body") or "",
            "path": c.get("path"),
            "line": line,
            "original_line": c.get("original_position"),
            "start_line": None,
            "original_start_line": None,
            "diff_hunk": c.get("diff_hunk"),
            "url": c.get("html_url"),
        }
        # A non-null resolver means the thread is resolved. Synthesize a per-comment
        # review-thread so diff()'s resolve/unresolve rule fires on the transition; the
        # provider-neutral event kind matches a future GitHub `resolved` field.
        snap["review_threads"][cid] = {
            "resolved": bool(c.get("resolver")),
            "path": c.get("path"),
            "line": line,
        }
    for c in data.get("issue_comments") or []:
        snap["issue_comments"][str(c.get("id"))] = {
            "user": (c.get("user") or {}).get("login"),
            "body": c.get("body") or "",
            "url": c.get("html_url"),
        }
    for s in data.get("statuses") or []:
        context = s.get("context")
        # statuses are keyed (and diffed) by context; a context-less entry would
        # collapse every such status onto the None key and lose all but the last.
        # Gitea always sets context in practice, so this just drops malformed entries.
        if not context:
            continue
        snap["statuses"][context] = {
            "state": s.get("status"),
            "url": s.get("target_url"),
            "desc": s.get("description"),
        }
    return snap


def _nodes(connection: dict | None) -> list[dict]:
    return (connection or {}).get("nodes") or []


def _mergeable_map(value: str | None) -> str:
    return {"MERGEABLE": "clean", "CONFLICTING": "dirty"}.get(value or "", "unknown")


def _reviewer_name(node: dict | None) -> str | None:
    if not node:
        return None
    return node.get("login") or node.get("name")


def snapshot_from_graphql(pr: dict) -> dict:
    """Normalize a GraphQL pullRequest node into the same snapshot shape as REST.

    Enums are lowercased and CONFLICTING -> "dirty" so the transport-agnostic
    diff() is unchanged. Current-state facets (draft, labels, requested reviewers)
    feed summarize(); review-thread resolution and the timeline (an ordered map of
    node id -> recurring transition) feed later diff rules.
    """
    snap: dict = {
        "state": "open" if pr.get("state") == "OPEN" else "closed",
        "merged": bool(pr.get("merged")),
        "merged_by": (pr.get("mergedBy") or {}).get("login"),
        "mergeable_state": _mergeable_map(pr.get("mergeable")),
        "head_sha": pr.get("headRefOid"),
        "title": pr.get("title"),
        "url": pr.get("url"),
        "draft": bool(pr.get("isDraft")),
        "labels": sorted(
            str(n["name"]) for n in _nodes(pr.get("labels")) if n.get("name")
        ),
        "requested_reviewers": sorted(
            r
            for n in _nodes(pr.get("reviewRequests"))
            if (r := _reviewer_name(n.get("requestedReviewer")))
        ),
        "reviews": {},
        "review_comments": {},
        "issue_comments": {},
        "check_runs": {},
        "statuses": {},
        "review_threads": {},
        "timeline": {},
    }
    for r in _nodes(pr.get("reviews")):
        if r.get("state") == "PENDING":
            continue
        snap["reviews"][str(r.get("id"))] = {
            "state": r.get("state"),
            "user": (r.get("author") or {}).get("login"),
            "body": r.get("body") or "",
            "url": r.get("url"),
        }
    for th in _nodes(pr.get("reviewThreads")):
        snap["review_threads"][str(th.get("id"))] = {
            "resolved": bool(th.get("isResolved")),
            "path": th.get("path"),
            "line": th.get("line"),
            "start_line": th.get("startLine"),
        }
        for c in _nodes(th.get("comments")):
            snap["review_comments"][str(c.get("id"))] = {
                "user": (c.get("author") or {}).get("login"),
                "body": c.get("body") or "",
                "path": c.get("path") or th.get("path"),
                "line": c.get("line") if c.get("line") is not None else th.get("line"),
                "start_line": c.get("startLine")
                if c.get("startLine") is not None
                else th.get("startLine"),
                "original_line": c.get("originalLine"),
                "original_start_line": c.get("originalStartLine"),
                "diff_hunk": c.get("diffHunk"),
                "url": c.get("url"),
            }
    for c in _nodes(pr.get("comments")):
        snap["issue_comments"][str(c.get("id"))] = {
            "user": (c.get("author") or {}).get("login"),
            "body": c.get("body") or "",
            "url": c.get("url"),
        }
    commits = _nodes(pr.get("commits"))
    rollup = (
        (commits[0].get("commit") or {}).get("statusCheckRollup") if commits else None
    )
    for ctx in _nodes((rollup or {}).get("contexts")):
        if ctx.get("__typename") == "CheckRun":
            snap["check_runs"][str(ctx.get("id"))] = {
                "name": ctx.get("name"),
                "status": (ctx.get("status") or "").lower(),
                "conclusion": (ctx.get("conclusion") or "").lower() or None,
                "url": ctx.get("detailsUrl"),
                "title": ctx.get("title"),
                "summary": ctx.get("summary"),
            }
        elif ctx.get("__typename") == "StatusContext":
            snap["statuses"][ctx.get("context")] = {
                "state": (ctx.get("state") or "").lower(),
                "url": ctx.get("targetUrl"),
                "desc": ctx.get("description"),
            }
    # Recurring transitions, keyed by their globally-unique timeline node id (in
    # chronological order — timelineItems is fetched with `last:N`). Each becomes
    # one diff event whose identity is that node id, so a label removed then
    # re-added yields two distinct events rather than colliding on identical text.
    for ev in _nodes(pr.get("timelineItems")):
        node_id = ev.get("id")
        if not node_id:
            continue
        typename = ev.get("__typename")
        if typename == "LabeledEvent":
            item = {
                "type": "label_added",
                "detail": (ev.get("label") or {}).get("name"),
            }
        elif typename == "UnlabeledEvent":
            item = {
                "type": "label_removed",
                "detail": (ev.get("label") or {}).get("name"),
            }
        elif typename == "ReviewRequestedEvent":
            item = {
                "type": "reviewer_requested",
                "detail": _reviewer_name(ev.get("requestedReviewer")),
            }
        elif typename == "ReviewRequestRemovedEvent":
            item = {
                "type": "reviewer_removed",
                "detail": _reviewer_name(ev.get("requestedReviewer")),
            }
        elif typename == "ReadyForReviewEvent":
            item = {"type": "ready", "detail": None}
        elif typename == "ConvertToDraftEvent":
            item = {"type": "draft", "detail": None}
        elif typename == "HeadRefForcePushedEvent":
            item = {
                "type": "force_push",
                "detail": {
                    "before": (ev.get("beforeCommit") or {}).get("oid"),
                    "after": (ev.get("afterCommit") or {}).get("oid"),
                },
            }
        else:
            continue
        snap["timeline"][str(node_id)] = item
    return snap


_GREEN_CONCLUSIONS = {"success", "neutral", "skipped"}


def _checks_all_green(snap: dict) -> bool:
    """True iff there is at least one check/status and none is failing or pending."""
    runs = snap.get("check_runs") or {}
    statuses = snap.get("statuses") or {}
    if not runs and not statuses:
        return False
    for c in runs.values():
        if (
            c.get("status") != "completed"
            or c.get("conclusion") not in _GREEN_CONCLUSIONS
        ):
            return False
    return all(s.get("state") == "success" for s in statuses.values())


def _check_summary_threshold() -> int:
    """How many green CheckRun completions in a single poll collapse into one summary
    event instead of one `pr_check` each. <= 0 disables collapsing entirely (every
    green stays an individual event). Garbage/missing falls back to the default."""
    raw = os.environ.get("SUBSPACE_PR_CHECK_SUMMARY_THRESHOLD")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return CHECK_SUMMARY_THRESHOLD_DEFAULT


def _head_change_is_force_push(old: dict, new: dict) -> bool:
    """True iff the new head sha is explained by a force-push timeline event that is
    new this poll. When so, the timeline loop emits the pr_force_push event and the
    head change must NOT also fire a "new commits pushed" event."""
    old_timeline = old.get("timeline") or {}
    new_head = new.get("head_sha")
    for node_id, item in (new.get("timeline") or {}).items():
        if node_id in old_timeline or item.get("type") != "force_push":
            continue
        if (item.get("detail") or {}).get("after") == new_head:
            return True
    return False


def diff(old: dict | None, new: dict, key: str) -> list[dict]:
    """Events introduced by `new` relative to `old`. None old => baseline (no events)."""
    if old is None:
        return []
    if new["merged"] and not old["merged"]:
        return [_merged_event(new, key)]  # terminal; nothing else matters

    events: list[dict] = []
    head = new.get("head_sha")
    # Monotonic per-facet transition counters, carried forward from the previous
    # snapshot and persisted in state.json. Folding the counter into a state-transition
    # identity keeps a recurring SAME-HEAD flap (e.g. base-branch churn flipping
    # mergeability dirty->clean->dirty, or a check re-run going green->fail->green)
    # as distinct events instead of colliding on one head-keyed id. Seed from `old`
    # so diffing the same pair twice is idempotent.
    epochs = dict((old or {}).get("transition_epochs") or {})
    new["transition_epochs"] = epochs
    if old["state"] == "open" and new["state"] == "closed" and not new["merged"]:
        epochs["closed"] = epochs.get("closed", 0) + 1
        events.append(
            _event(
                "pr_closed",
                "info",
                f"{key} was closed without merging.\n{new['url']}",
                key,
                new["url"],
                f"closed:{key}:{head}:{epochs['closed']}",
            )
        )
    if new["mergeable_state"] == "dirty" and old["mergeable_state"] != "dirty":
        epochs["mergeable"] = epochs.get("mergeable", 0) + 1
        events.append(
            _event(
                "pr_conflict",
                "high",
                f"{key} now has merge conflicts and can't be merged cleanly; "
                f"rebase/merge the base branch and resolve them.\n{new['url']}",
                key,
                new["url"],
                f"conflict:{key}:{head}:{epochs['mergeable']}",
            )
        )
    # Inverse of pr_conflict: the dirty -> clean transition. It shares pr_conflict's
    # monotonic "mergeable" counter (bumped on every mergeability transition) and folds
    # it into the identity, so a re-resolve — or a re-conflict — on the *same* head is a
    # distinct event rather than collapsing onto an earlier head-keyed id.
    if new["mergeable_state"] == "clean" and old["mergeable_state"] == "dirty":
        epochs["mergeable"] = epochs.get("mergeable", 0) + 1
        events.append(
            _event(
                "pr_conflict_resolved",
                "info",
                f"{key}: merge conflicts resolved — it can be merged cleanly again.\n{new['url']}",
                key,
                new["url"],
                f"conflict_resolved:{key}:{head}:{epochs['mergeable']}",
            )
        )
    # Recurring facets (labels, reviewers, draft, force-push) come from new timeline
    # nodes: each carries a globally-unique id, so identical-looking transitions stay
    # distinct. A head-sha change not backed by a new force-push node is fresh commits.
    if (
        old["head_sha"]
        and new["head_sha"]
        and old["head_sha"] != new["head_sha"]
        and not _head_change_is_force_push(old, new)
    ):
        events.append(
            _event(
                "pr_commits",
                "info",
                f"New commits pushed to {key} (head is now {new['head_sha'][:7]}).\n{new['url']}",
                key,
                new["url"],
                f"commits:{key}:{new['head_sha']}",
            )
        )
    old_timeline = old.get("timeline") or {}
    for node_id, item in (new.get("timeline") or {}).items():
        if node_id in old_timeline:
            continue
        event = _timeline_event(item, key, new["url"], node_id)
        if event is not None:
            events.append(event)
    for rid, r in new["reviews"].items():
        if rid not in old["reviews"]:
            events.append(_review_event(r, key, rid))
    for cid, c in new["review_comments"].items():
        if cid not in old["review_comments"]:
            events.append(_inline_comment_event(c, key, cid))
    # Thread resolve/unresolve has no timeline event, so it stays a set-diff keyed on
    # the (reused) thread id. The monotonic "thread" counter folded into the identity
    # keeps a same-thread resolve->reopen->resolve flap distinct instead of collapsing
    # the second resolve onto the first (rare, but it happens).
    for tid, th in (new.get("review_threads") or {}).items():
        prev = (old.get("review_threads") or {}).get(tid)
        if prev is not None and prev.get("resolved") != th.get("resolved"):
            loc = (
                f"{th.get('path')}:{th.get('line')}"
                if th.get("line")
                else (th.get("path") or "a file")
            )
            resolved = bool(th.get("resolved"))
            state = "resolved" if resolved else "reopened (unresolved)"
            epochs["thread"] = epochs.get("thread", 0) + 1
            events.append(
                _event(
                    "pr_thread",
                    "info",
                    f"A review thread on {key} ({loc}) was {state}.\n{new['url']}",
                    key,
                    new["url"],
                    f"thread:{tid}:{resolved}:{epochs['thread']}",
                )
            )
    for cid, c in new["issue_comments"].items():
        if cid not in old["issue_comments"]:
            events.append(_comment_event(c, key, cid))
    # The all-checks-green recovery capstone (below) also stands in for a storm of
    # greens completing this poll, so when it fires the per-poll summary is suppressed
    # to avoid double-notifying. Compute the condition once, before the check loop,
    # and reuse it both here and in the capstone block.
    becoming_all_green = not _checks_all_green(old) and _checks_all_green(new)
    # Only CheckRuns are collapsed into a summary. StatusContexts (the statuses loop
    # below) are typically few, so they always stay individual.
    green_completions: list[tuple[str, dict]] = []
    for chid, ch in new["check_runs"].items():
        prev = old["check_runs"].get(chid)
        became_complete = ch["status"] == "completed" and (
            prev is None or prev.get("status") != "completed"
        )
        conclusion_changed = (
            prev is not None
            and ch["status"] == "completed"
            and prev.get("conclusion") != ch.get("conclusion")
        )
        if became_complete or conclusion_changed:
            if ch.get("conclusion") in _FAILED_CONCLUSIONS:
                events.append(_check_event(ch, key, chid))  # failures never collapse
            elif ch.get("conclusion") in _GREEN_CONCLUSIONS:
                green_completions.append((chid, ch))  # maybe collapsed below
            else:
                events.append(_check_event(ch, key, chid))  # completed, other/no concl.
    # Collapse a storm of green completions into a single summary event, unless the
    # all-green capstone below already covers this poll. Below the threshold (or with
    # collapsing disabled) keep them individual so small PRs retain per-check detail.
    threshold = _check_summary_threshold()
    if threshold > 0 and len(green_completions) >= threshold:
        if not becoming_all_green:
            events.append(_checks_summary_event(green_completions, new, key))
    else:
        for chid, ch in green_completions:
            events.append(_check_event(ch, key, chid))
    # Legacy StatusContexts are keyed on the (reused) context name, so the monotonic
    # "status" counter keeps a same-context success->failure->success flap distinct.
    for ctx, s in new["statuses"].items():
        prev = old["statuses"].get(ctx)
        if s["state"] in ("success", "failure", "error") and (
            prev is None or prev.get("state") != s["state"]
        ):
            epochs["status"] = epochs.get("status", 0) + 1
            events.append(_status_event(ctx, s, key, epochs["status"]))
    # The all-green recovery carries its own monotonic "green" counter so a check
    # re-run flapping green->fail->green on the *same* head emits each recovery as a
    # distinct event instead of colliding on one head-keyed id.
    if becoming_all_green:
        epochs["green"] = epochs.get("green", 0) + 1
        head7 = (head or "")[:7]
        events.append(
            _event(
                "pr_checks_green",
                "info",
                f"All checks are now passing on {key} (head {head7}).\n{new['url']}",
                key,
                new["url"],
                f"green:{key}:{head}:{epochs['green']}",
            )
        )
    return events


def summarize(snap: dict | None) -> str:
    """One-line current-status summary for the subscribe confirmation."""
    if not snap:
        return "unknown"
    checks = snap["check_runs"].values()
    passed = sum(1 for c in checks if c.get("conclusion") == "success")
    failed = sum(1 for c in checks if c.get("conclusion") in _FAILED_CONCLUSIONS)
    pending = sum(1 for c in checks if c.get("status") != "completed")
    parts = [snap.get("state") or "?"]
    if snap.get("merged"):
        parts.append("merged")
    if snap.get("mergeable_state"):
        parts.append(f"mergeable={snap['mergeable_state']}")
    parts.append(f"checks {passed} pass / {failed} fail / {pending} pending")
    parts.append(f"{len(snap['reviews'])} reviews")
    return ", ".join(parts)


# --------------------------------------------------------------------------- #
# formatting (pure)
# --------------------------------------------------------------------------- #


def _short(text: str | None, limit: int = SHORT_BODY_CHARS) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "… (truncated — see link)"


def _hunk_tail(hunk: str | None, n: int) -> str:
    if not hunk:
        return ""
    body = [ln for ln in hunk.splitlines() if not ln.startswith("@@")]
    tail = body[-max(1, n) :]
    return "\n".join(ln[1:] if ln[:1] in " +-" else ln for ln in tail)


def _event(
    kind: str,
    severity: str,
    content: str,
    key: str,
    url: str | None,
    identity: str,
) -> dict:
    meta = {"severity": severity, "kind": kind, "pr": key}
    if url:
        meta["url"] = url
    # Identity-addressed id: a hash of a stable source identity (a timeline node id,
    # an object id like a review/comment/check id, or a synthesized state key such as
    # f"conflict:{key}:{head}"), NOT the rendered content. This keeps dedup idempotent
    # and reproducible across restarts while letting recurring transitions that render
    # to identical text (e.g. a label removed then re-added) stay distinct events.
    event_id = hashlib.sha256(identity.encode()).hexdigest()[:16]
    return {
        "id": event_id,
        "type": kind,
        "content": content,
        "meta": meta,
        "created_at": time.time(),
    }


def synthetic_event(
    kind: str,
    severity: str,
    content: str,
    key: str,
    identity: str,
    url: str | None = None,
) -> dict:
    """Event the server emits itself (e.g. a fetch failure), not from a diff."""
    return _event(kind, severity, content, key, url, identity)


def _timeline_event(item: dict, key: str, url: str | None, node_id: str) -> dict | None:
    """Render a recurring transition (label/reviewer/draft/force-push) parsed from a
    timeline node; identity is the node id so identical-looking repeats stay distinct."""
    kind = item.get("type")
    detail = item.get("detail")
    if kind == "label_added":
        return _event(
            "pr_label",
            "info",
            f"Label '{detail}' added to {key}.\n{url}",
            key,
            url,
            node_id,
        )
    if kind == "label_removed":
        return _event(
            "pr_label",
            "info",
            f"Label '{detail}' removed from {key}.\n{url}",
            key,
            url,
            node_id,
        )
    if kind == "reviewer_requested":
        return _event(
            "pr_review_request",
            "info",
            f"Review requested from {detail} on {key}.\n{url}",
            key,
            url,
            node_id,
        )
    if kind == "reviewer_removed":
        return _event(
            "pr_review_request",
            "info",
            f"Review request for {detail} was removed on {key}.\n{url}",
            key,
            url,
            node_id,
        )
    if kind == "draft":
        return _event(
            "pr_draft",
            "info",
            f"{key} was converted to a draft.\n{url}",
            key,
            url,
            node_id,
        )
    if kind == "ready":
        return _event(
            "pr_draft",
            "info",
            f"{key} was marked ready for review.\n{url}",
            key,
            url,
            node_id,
        )
    if kind == "force_push":
        before = ((detail or {}).get("before") or "")[:7]
        after = ((detail or {}).get("after") or "")[:7]
        return _event(
            "pr_force_push",
            "info",
            f"{key} was force-pushed (history rewritten — rebase/amend/squash); "
            f"head {before} → {after}.\n{url}",
            key,
            url,
            node_id,
        )
    return None


def _merged_event(new: dict, key: str) -> dict:
    who = new.get("merged_by") or "someone"
    content = (
        f"{key} was merged by {who}. All subscribers have been unsubscribed and "
        f"polling has stopped.\n{new['url']}"
    )
    return _event("pr_merged", "high", content, key, new["url"], f"merged:{key}")


def _review_event(r: dict, key: str, review_id: str) -> dict:
    label = _REVIEW_LABEL.get(r["state"], (r["state"] or "reviewed").lower())
    severity = "high" if r["state"] == "CHANGES_REQUESTED" else "info"
    body = _short(r.get("body"))
    body_part = f"\n{body}" if body else ""
    content = f"Review on {key}: {r['user']} {label}.{body_part}\n{r['url']}"
    return _event("pr_review", severity, content, key, r["url"], f"review:{review_id}")


def _comment_event(c: dict, key: str, comment_id: str) -> dict:
    content = f"New comment on {key} by {c['user']}:\n{_short(c['body'])}\n{c['url']}"
    return _event("pr_comment", "info", content, key, c["url"], f"comment:{comment_id}")


def _inline_comment_event(c: dict, key: str, comment_id: str) -> dict:
    path = c.get("path")
    end = c.get("line") or c.get("original_line")
    start = c.get("start_line") or c.get("original_start_line") or end
    if start and end and start > end:
        start, end = end, start
    if start and end and start != end:
        loc = f"{path}:{start}-{end}"
        span = end - start + 1
    else:
        loc = f"{path}:{end}"
        span = 1
    if span <= SHORT_RANGE_LINES:
        code = _hunk_tail(c.get("diff_hunk"), span)
        snippet = f"\n```\n{code}\n```" if code else ""
    else:
        snippet = f"\n(comment spans lines {start}–{end})"
    content = (
        f"Inline comment on {key} by {c.get('user')} at {loc}:{snippet}\n"
        f"> {_short(c.get('body'))}\n{c.get('url')}"
    )
    return _event(
        "pr_inline_comment",
        "info",
        content,
        key,
        c.get("url"),
        f"inline_comment:{comment_id}",
    )


def _check_event(ch: dict, key: str, check_id: str) -> dict:
    outcome = ch.get("conclusion") or ch.get("status")
    severity = "high" if ch.get("conclusion") in _FAILED_CONCLUSIONS else "info"
    parts = [f"Check '{ch.get('name')}' on {key}: {outcome}."]
    if ch.get("title"):
        parts.append(_short(ch.get("title"), 200))
    if severity == "high" and ch.get("summary"):
        parts.append(_short(ch.get("summary"), 600))
    if ch.get("url"):
        parts.append(ch["url"])
    # status:conclusion in the identity so each completed transition (and a re-run
    # landing a different conclusion) is its own event rather than colliding.
    identity = f"check:{check_id}:{ch.get('status')}:{ch.get('conclusion')}"
    return _event("pr_check", severity, "\n".join(parts), key, ch.get("url"), identity)


def _checks_summary_event(green: list[tuple[str, dict]], new: dict, key: str) -> dict:
    """One event standing in for a storm of green check completions in a single poll.
    Identity is the set of collapsed check identities, so distinct storms are distinct
    events and re-diffing the same poll is idempotent — no monotonic counter needed."""
    passed = len(green)
    running = sum(
        1 for c in new["check_runs"].values() if c.get("status") != "completed"
    )
    url = new.get("url")
    running_part = f"; {running} still running" if running else ""
    content = f"{passed} checks just passed on {key}{running_part}.\n{url}"
    identity = (
        "checks_summary:"
        + key
        + ":"
        + ",".join(
            sorted(
                f"{chid}:{ch.get('status')}:{ch.get('conclusion')}"
                for chid, ch in green
            )
        )
    )
    return _event("pr_checks_summary", "info", content, key, url, identity)


def _status_event(ctx: str, s: dict, key: str, epoch: int) -> dict:
    severity = "high" if s["state"] in ("failure", "error") else "info"
    parts = [f"Status '{ctx}' on {key}: {s['state']}."]
    if s.get("desc"):
        parts.append(_short(s.get("desc"), 200))
    if s.get("url"):
        parts.append(s["url"])
    # epoch (a monotonic per-poll counter from diff()) makes a same-context
    # success->failure->success flap distinct rather than colliding on ctx+state.
    return _event(
        "pr_status",
        severity,
        "\n".join(parts),
        key,
        s.get("url"),
        f"status:{ctx}:{s['state']}:{epoch}",
    )


# --------------------------------------------------------------------------- #
# tracker + persistence
# --------------------------------------------------------------------------- #


class PRTracker:
    """Mutable per-PR state. The server owns the polling/delivery around it."""

    def __init__(
        self,
        owner: str,
        repo: str,
        number: int,
        client,
        provider: str = "github",
        base_url: str | None = None,
        instance: str = "",
    ) -> None:
        self.owner = owner
        self.repo = repo
        self.number = number
        self.provider = provider
        # For Forgejo, the multi-instance alias this PR's instance is configured under.
        # "" means the DEFAULT instance (unaliased FORGEJO_API_URL/TOKEN), which keeps the
        # storage key byte-identical to v1. A named instance folds its alias into the key
        # so the same owner/repo#n on two forges can't collide.
        self.instance = instance or ""
        # For Forgejo, the instance API base URL this PR lives on. Persisted so a future
        # multi-instance setup can tell provenance from the record (builder's steer), AND
        # as load_trackers' mismatch guard: if the alias is later repointed at a different
        # URL, the persisted base_url disagrees with the configured client's and the
        # tracker is skipped rather than polled against the wrong instance.
        self.base_url = base_url
        self.key = pr_key(owner, repo, number)  # human-facing ref (event text, replies)
        self.storage_key = storage_key(
            provider, owner, repo, number, self.instance
        )  # TRACKERS-dict / on-disk key; provider-prefixed for non-github, alias-tagged for named forgejo
        self.client = client
        self.subscribers: set[str] = set()
        self.acked: dict[str, set[str]] = {}  # session id -> set of acked event ids
        self.missed: dict[str, int] = {}  # session id -> count of dropped-while-unacked
        self.events: list[dict] = []
        self.event_ids: set[str] = set()
        self.snapshot: dict | None = None
        self.consecutive_no_update = 0
        self.next_poll_at: float | None = (
            None  # persisted, so a restart doesn't stampede
        )
        self.merged = False
        self.terminal = False  # merged or gone: stop polling, unsubscribe on ack
        self.terminal_id: str | None = None
        # Wall-clock epoch the tracker last went subscriber-less; None while active.
        # The server keeps a subscriber-less non-terminal tracker "warm" for a TTL so a
        # quick re-subscribe reuses the cached snapshot instead of re-baselining.
        self.idle_since: float | None = None
        self.auth_notified = False  # the one-time auth-failure event was emitted
        # Set by an agent's refresh request. The poll loop polls at once and does not
        # count a no-update result against the backoff: an on-demand poll is not
        # evidence that the PR is idle. Runtime-only, never persisted.
        self.refresh_pending = False
        self.task = None
        self.wake = asyncio.Event()

    def unacked_for(self, session_id: str) -> list[dict]:
        acked = self.acked.get(session_id, set())
        return [e for e in self.events if e["id"] not in acked]

    def record(self, events: list[dict]) -> list[dict]:
        """Add not-yet-seen events to memory (dedup by identity-addressed id); return the new ones."""
        added: list[dict] = []
        for e in events:
            event_id = e.get("id")
            if not event_id or event_id in self.event_ids:
                continue
            self.events.append(e)
            self.event_ids.add(event_id)
            added.append(e)
            if e["type"] in ("pr_merged", "pr_gone"):
                self.terminal = True
                self.terminal_id = event_id
        return added

    def _snapshot(self, raw: dict) -> dict:
        """Normalize a provider's raw fetch into the transport-agnostic snapshot."""
        if self.provider == "forgejo":
            return snapshot_from_forgejo(raw)
        return snapshot_from_graphql(raw)

    async def initial_poll(self) -> str:
        """Establish the baseline snapshot (no events) and return a status summary."""
        pr = await self.client.fetch_pr(self.owner, self.repo, self.number)
        self.snapshot = self._snapshot(pr)
        self.merged = self.snapshot["merged"]
        return summarize(self.snapshot)

    async def poll_once(self) -> list[dict]:
        """Fetch, diff against the last snapshot, and return the raw diff events."""
        pr = await self.client.fetch_pr(self.owner, self.repo, self.number)
        new = self._snapshot(pr)
        events = diff(self.snapshot, new, self.key)
        self.snapshot = new
        if new["merged"]:
            self.merged = True
        return events


# Storage layout per PR (under SUBSPACE_DATA_DIR/pr/<safe key>/):
#   state.json      polling state (snapshot, backoff, flags) — tmp+rename per poll
#   events.jsonl    append-only event log — appended to, rarely rewritten (compaction)
#   sub-<sid>.json  one per subscriber (acked id set) — tmp+rename only on that ack
# This keeps write amplification low: a poll appends a line and rewrites a small
# state file; an ack rewrites one small subscriber file and never touches the log.


def pr_store_dir() -> Path:
    return settings.data_dir() / "pr"


def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.#-]", "_", name)


def _tracker_dir(key: str) -> Path:
    return pr_store_dir() / _safe(key)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def save_state(t: PRTracker) -> None:
    _atomic_write(
        _tracker_dir(t.storage_key) / "state.json",
        json.dumps(
            {
                "owner": t.owner,
                "repo": t.repo,
                "number": t.number,
                "provider": t.provider,
                "instance": t.instance,
                "base_url": t.base_url,
                "snapshot": t.snapshot,
                "consecutive_no_update": t.consecutive_no_update,
                "next_poll_at": t.next_poll_at,
                "merged": t.merged,
                "terminal": t.terminal,
                "terminal_id": t.terminal_id,
                "auth_notified": t.auth_notified,
                "idle_since": t.idle_since,
            }
        ),
    )


def append_events(t: PRTracker, events: list[dict]) -> None:
    """Append new events to the JSONL log; compact + prune if the log grows large."""
    if not events:
        return
    directory = _tracker_dir(t.storage_key)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "events.jsonl").open("a") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    if len(t.events) > MAX_CACHED_EVENTS:
        # Before trimming, record per-subscriber how many of the events about to be
        # dropped were never acked by that subscriber — those are genuinely lost, so
        # the server can surface a "history truncated" notice on (re)connect.
        dropped_ids = {e["id"] for e in t.events[:-MAX_CACHED_EVENTS]}
        for sid in list(t.acked):
            t.missed[sid] = t.missed.get(sid, 0) + len(
                dropped_ids - t.acked.get(sid, set())
            )
        t.events = t.events[-MAX_CACHED_EVENTS:]
        t.event_ids = {e["id"] for e in t.events}
        _atomic_write(
            directory / "events.jsonl", "".join(json.dumps(e) + "\n" for e in t.events)
        )
        for sid in list(t.acked):  # drop acked ids for events that fell out of the log
            t.acked[sid] &= t.event_ids
            save_subscriber(t, sid)  # persists the bumped missed count too


def save_subscriber(t: PRTracker, session_id: str) -> None:
    _atomic_write(
        _tracker_dir(t.storage_key) / f"sub-{storage.key_filename(session_id)}.json",
        json.dumps(
            {
                "session_id": session_id,
                "acked": sorted(t.acked.get(session_id, set())),
                "missed": t.missed.get(session_id, 0),
            }
        ),
    )


def delete_subscriber(t: PRTracker, session_id: str) -> None:
    try:
        (
            _tracker_dir(t.storage_key) / f"sub-{storage.key_filename(session_id)}.json"
        ).unlink()
    except OSError:
        pass


def delete_tracker(key: str) -> None:
    shutil.rmtree(_tracker_dir(key), ignore_errors=True)


def _client_key(provider: str, instance: str) -> object:
    """The clients-map lookup key for a (provider, instance) pair. The DEFAULT instance
    (instance "") keys on the bare provider string — byte-identical to the v1
    ``{provider: client}`` shape, so existing callers/tests are unchanged — while a NAMED
    forgejo instance keys on the ``(provider, instance)`` tuple."""
    return provider if not instance else (provider, instance)


def load_trackers(clients) -> list[PRTracker]:
    # Accept either a clients map or a single client (treated as the default github —
    # back-compat for existing callers and tests). The map keys a DEFAULT instance on the
    # bare provider string ("github" / "forgejo") and a NAMED forgejo instance on a
    # (provider, alias) tuple, so the same map serves default + every configured alias.
    if not isinstance(clients, dict):
        clients = {"github": clients}
    base = pr_store_dir()
    if not base.is_dir():
        return []
    trackers: list[PRTracker] = []
    for directory in base.iterdir():
        state_path = directory / "state.json"
        if not directory.is_dir() or not state_path.exists():
            continue
        try:
            state = json.loads(state_path.read_text())
        except (OSError, ValueError):
            continue
        provider = state.get("provider", "github")  # absent -> github (legacy dirs)
        instance = state.get("instance", "") or ""  # absent -> default instance
        key = _client_key(provider, instance)
        # Skip a tracker whose (provider, instance) isn't wired on this server (e.g. a
        # forgejo tracker for an alias no longer configured, or forgejo not configured at
        # all) so it can't crash the poll loop. Membership — not truthiness — is the test:
        # a caller may deliberately map a provider to None (the storage-roundtrip tests
        # pass a single None client).
        if key not in clients:
            named = f" instance {instance!r}" if instance else ""
            print(
                f"subspace: skipping PR tracker {directory.name!r} — no client for "
                f"provider {provider!r}{named} (is it configured on this server?)",
                file=sys.stderr,
            )
            continue
        client = clients[key]
        # base_url mismatch guard: if this tracker's persisted base_url disagrees with the
        # configured client's base_url for its (provider, instance), the alias has been
        # repointed at a different URL since the tracker was written. Skip it with a loud
        # warning rather than polling it against the wrong instance (which would emit bogus
        # diffs, or leak one instance's PR state into another's addressing). Only guard
        # forgejo, and only when both sides carry a base_url to compare (a None-client test
        # stub, or a legacy tracker with no persisted base_url, can't mismatch).
        persisted_base = state.get("base_url")
        configured_base = getattr(client, "base_url", None)
        if (
            provider == "forgejo"
            and persisted_base
            and configured_base
            and persisted_base != configured_base
        ):
            print(
                f"subspace: skipping PR tracker {directory.name!r} — persisted "
                f"base_url {persisted_base!r} disagrees with the configured "
                f"{('instance ' + repr(instance)) if instance else 'default instance'} "
                f"URL {configured_base!r} (alias repointed?); not polling the wrong "
                "instance",
                file=sys.stderr,
            )
            continue
        t = PRTracker(
            state["owner"],
            state["repo"],
            int(state["number"]),
            client,
            provider=provider,
            base_url=state.get("base_url"),
            instance=instance,
        )
        snapshot = state.get("snapshot")
        # A snapshot from before the timeline-identity switch has a different
        # shape/ID space; drop it so the next poll re-baselines silently instead of
        # emitting noise (a burst of timeline facets, or differently-hashed ids).
        if snapshot is not None and "timeline" not in snapshot:
            snapshot = None
        t.snapshot = snapshot
        t.consecutive_no_update = int(state.get("consecutive_no_update", 0))
        t.next_poll_at = state.get("next_poll_at")
        t.merged = bool(state.get("merged"))
        t.terminal = bool(state.get("terminal"))
        t.terminal_id = state.get("terminal_id")
        t.auth_notified = bool(state.get("auth_notified"))
        t.idle_since = state.get("idle_since")  # absent -> None (backward compatible)

        events_path = directory / "events.jsonl"
        if events_path.exists():
            for line in events_path.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("id") and e["id"] not in t.event_ids:
                    t.events.append(e)
                    t.event_ids.add(e["id"])
            if len(t.events) > MAX_CACHED_EVENTS:
                t.events = t.events[-MAX_CACHED_EVENTS:]
                t.event_ids = {e["id"] for e in t.events}

        for sub_path in directory.glob("sub-*.json"):
            try:
                sub = json.loads(sub_path.read_text())
            except (OSError, ValueError):
                continue
            session_id = sub.get("session_id")
            if session_id:
                t.subscribers.add(session_id)
                t.acked[session_id] = set(sub.get("acked", []))
                t.missed[session_id] = int(sub.get("missed", 0))  # absent -> 0 (compat)
        # An active tracker is never warm; the reaper self-heals a zero-subscriber
        # tracker whose idle_since is still None (e.g. older on-disk state).
        if t.subscribers:
            t.idle_since = None
        trackers.append(t)
    return trackers
