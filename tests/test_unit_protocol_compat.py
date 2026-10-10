# vim: filetype=python
"""Tests for scripts/protocol-compat.py. Each case edits the real protocol
source and checks the bump that the checker gives."""

import importlib.util
import itertools
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SOURCE = (ROOT / "protocol" / "src" / "subspace_protocol" / "__init__.py").read_text()
_spec = importlib.util.spec_from_file_location(
    "protocol_compat", ROOT / "scripts" / "protocol-compat.py"
)
assert _spec is not None and _spec.loader is not None
compat = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(compat)

_counter = itertools.count()


def _edit(old: str, new: str) -> str:
    assert SOURCE.count(old) == 1, old
    return SOURCE.replace(old, new)


def _compare(new_source: str):
    n = next(_counter)
    old = compat.load(SOURCE, f"protocol_old_{n}")
    new = compat.load(new_source, f"protocol_new_{n}")
    return compat.compare(old, new)


def test_identical_is_patch() -> None:
    report = _compare(SOURCE)
    assert report.bump == "patch"
    assert not report.breaking and not report.changed


def test_docstring_change_is_patch() -> None:
    report = _compare(
        _edit(
            '"""Reply: ``scheduled``."""',
            '"""Schedule a callback. Reply: ``scheduled``."""',
        )
    )
    assert report.bump == "patch"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        pytest.param(
            "class Posted(TypedDict):\n",
            "class Posted(TypedDict):\n    note: NotRequired[str]\n",
            id="optional field on a server frame",
        ),
        pytest.param(
            "class Post(TypedDict):\n",
            "class Post(TypedDict):\n    note: NotRequired[str]\n",
            id="optional field on a client frame",
        ),
        pytest.param(
            '    channel: str\n    body: str\n    intent: NotRequired[Intent]  # default "fyi"',
            '    channel: NotRequired[str]\n    body: str\n    intent: NotRequired[Intent]  # default "fyi"',
            id="client field becomes optional",
        ),
        pytest.param(
            '    "pr_auth_error",\n]',
            '    "pr_auth_error",\n    "pr_new_kind",\n]',
            id="value added to an Open type",
        ),
        pytest.param(
            "NotifyMeta = CallbackMeta |",
            'class NewMeta(TypedDict):\n    kind: Literal["new"]\n\n\n'
            "NotifyMeta = NewMeta | CallbackMeta |",
            id="variant added to an Open union",
        ),
        pytest.param(
            "ClientFrame = (\n",
            'class Ping(TypedDict):\n    type: Literal["ping"]\n    req_id: ReqId\n\n\n'
            "ClientFrame = (\n    Ping\n    |",
            id="new client frame",
        ),
    ],
)
def test_compatible_change_is_minor(old: str, new: str) -> None:
    report = _compare(_edit(old, new))
    assert report.breaking == []
    assert report.bump == "minor"


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        pytest.param(
            "class Post(TypedDict):\n",
            "class Post(TypedDict):\n    note: str\n",
            "[old client -> new server] post.note: the receiver requires this field",
            id="required field on a client frame",
        ),
        pytest.param(
            "    id: str\n    ordinal: int\n    context: str\n    members: int\n",
            "    id: str\n    context: str\n    members: int\n",
            "[new server -> old client] posted.ordinal: the receiver requires this field",
            id="field removed from a server frame",
        ),
        pytest.param(
            "    id: str\n    ordinal: int\n    context: str\n    members: int\n",
            "    id: str\n    ordinal: NotRequired[int]\n    context: str\n    members: int\n",
            "[new server -> old client] posted.ordinal: the receiver requires this field",
            id="server field becomes optional",
        ),
        pytest.param(
            "    id: str\n    ordinal: int\n    context: str\n    members: int\n",
            "    id: str\n    ordinal: str\n    context: str\n    members: int\n",
            "[new server -> old client] posted.ordinal: the sender may send any str",
            id="server field type change",
        ),
        pytest.param(
            'Severity = Literal["low", "normal", "high"]',
            'Severity = Literal["low", "normal", "high", "critical"]',
            "the sender may send 'critical', which the receiver does not accept",
            id="value added to a closed type the server sends",
        ),
        pytest.param(
            'Threshold = Literal["all", "direct", "urgent"]',
            'Threshold = Literal["direct", "urgent"]',
            "[old client -> new server] join_channel.threshold: the sender may send 'all'",
            id="value removed from a type the server accepts",
        ),
        pytest.param(
            "    | MessageStatus\n)",
            ")",
            "message_status: the server no longer accepts this frame",
            id="client frame removed",
        ),
        pytest.param(
            "DEFAULT_PORT: Final = 8137",
            "DEFAULT_PORT: Final = 8138",
            "DEFAULT_PORT: value or behavior changed",
            id="constant changed",
        ),
        pytest.param(
            'return f"Bearer {token}"',
            'return f"Token {token}"',
            "auth_header_value: value or behavior changed",
            id="function changed",
        ),
        pytest.param(
            "class Reaction(TypedDict):",
            "class Reaction2(TypedDict):",
            "Reaction: removed from the package",
            id="public name removed",
        ),
    ],
)
def test_breaking_change_is_major(old: str, new: str, expected: str) -> None:
    new_source = _edit(old, new)
    if "Reaction2" in new_source:
        new_source = new_source.replace("list[Reaction]", "list[Reaction2]")
    report = _compare(new_source)
    assert report.bump == "major"
    assert any(expected in line for line in report.breaking), report.breaking


def test_open_does_not_loosen_what_the_server_accepts() -> None:
    # Post.intent is client-sent; marking it Open must not hide a removed value.
    new_source = _edit(
        '    intent: NotRequired[Intent]  # default "fyi"\n    severity: NotRequired[Severity]  # default "normal"\n    mentions',
        '    intent: NotRequired[Annotated[Literal["fyi"], Open]]\n    severity: NotRequired[Severity]  # default "normal"\n    mentions',
    )
    report = _compare(new_source)
    assert any(
        "post.intent: the sender may send 'question'" in line
        for line in report.breaking
    )


def test_cli_compares_git_refs() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "protocol-compat.py"), "HEAD", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "patch"
