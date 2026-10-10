#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Compare two versions of the subspace-protocol package.

    scripts/protocol-compat.py OLD_REF [NEW_REF]

Loads the protocol module at the git ref OLD_REF, and at NEW_REF or else the
working tree. Prints each finding to stderr and the version bump that the
changes need (major, minor, or patch) to stdout.

The checks assume an old client talks to a new server. A client frame must
still be accepted: every value an old client can send must conform to the new
type. A server frame must still be understood: every value the new server can
send must conform to the old type, except where the old type is marked Open.
A removed public name is a breaking change for clients that import it.

Runs on Python 3.11 with the standard library only, as on the CI runner.
"""

# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

import argparse
import ast
import copy
import importlib.util
import subprocess
import sys
import tempfile
import types
import typing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal, NotRequired, Required, Union

REPO = Path(__file__).resolve().parent.parent
MODULE_PATH = "protocol/src/subspace_protocol/__init__.py"


@dataclass
class Report:
    breaking: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)

    @property
    def bump(self) -> str:
        if self.breaking:
            return "major"
        return "minor" if self.changed else "patch"


@dataclass
class Version:
    module: types.ModuleType
    source: str


def load(source: str, name: str) -> Version:
    """Import protocol module source under a unique module name."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"{name}.py"
        path.write_text(source)
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return Version(module, source)


def git_source(ref: str) -> str:
    return subprocess.run(
        ["git", "show", f"{ref}:{MODULE_PATH}"],
        cwd=REPO,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


# --------------------------------------------------------------------------- #
# Type structure
# --------------------------------------------------------------------------- #


def _is_open(marker: object) -> bool:
    # Each loaded version has its own Open class, so match by name.
    return getattr(marker, "__name__", None) == "Open"


def unwrap(tp: object) -> tuple[object, bool]:
    """Strip Annotated and Required/NotRequired. Return (type, is_open)."""
    is_open = False
    while True:
        origin = typing.get_origin(tp)
        if origin is Annotated:
            is_open = is_open or any(_is_open(m) for m in tp.__metadata__)  # type: ignore[attr-defined]
            tp = tp.__origin__  # type: ignore[attr-defined]
        elif origin in (Required, NotRequired):
            tp = typing.get_args(tp)[0]
        else:
            return tp, is_open


@dataclass
class Members:
    """A type as a flat set of alternatives."""

    literals: set[object] = field(default_factory=set)
    others: list[object] = field(default_factory=list)
    is_open: bool = False


def members(tp: object) -> Members:
    result = Members()
    _collect(tp, result)
    return result


def _collect(tp: object, into: Members) -> None:
    tp, is_open = unwrap(tp)
    into.is_open = into.is_open or is_open
    origin = typing.get_origin(tp)
    if origin in (Union, types.UnionType):
        for arg in typing.get_args(tp):
            _collect(arg, into)
    elif origin is Literal:
        into.literals.update(typing.get_args(tp))
    else:
        into.others.append(tp)


def kind(tp: object) -> str:
    if tp is type(None):
        return "None"
    if typing.is_typeddict(tp):
        return "typeddict"
    origin = typing.get_origin(tp)
    if origin is not None:
        return getattr(origin, "__name__", repr(origin))
    return getattr(tp, "__name__", repr(tp))


def describe(tp: object) -> str:
    """A canonical text form of a type, used to detect any change."""
    tp, is_open = unwrap(tp)
    prefix = "open " if is_open else ""
    origin = typing.get_origin(tp)
    if origin in (Union, types.UnionType):
        inner = sorted(describe(arg) for arg in typing.get_args(tp))
        return f"{prefix}Union[{', '.join(inner)}]"
    if origin is Literal:
        inner = sorted(repr(arg) for arg in typing.get_args(tp))
        return f"{prefix}Literal[{', '.join(inner)}]"
    if typing.is_typeddict(tp):
        hints = typing.get_type_hints(tp, include_extras=True)
        required = tp.__required_keys__  # type: ignore[attr-defined]
        fields = ", ".join(
            f"{key}{'' if key in required else '?'}: {describe(value)}"
            for key, value in sorted(hints.items())
        )
        return f"{prefix}{tp.__name__}{{{fields}}}"  # type: ignore[attr-defined]
    if origin is not None:
        inner = ", ".join(describe(arg) for arg in typing.get_args(tp))
        return f"{prefix}{kind(tp)}[{inner}]"
    return f"{prefix}{kind(tp)}"


# --------------------------------------------------------------------------- #
# Conformance
# --------------------------------------------------------------------------- #


def conforms(
    src: object, dst: object, path: str, report: Report, *, open_ok: bool
) -> None:
    """Report each value of type ``src`` that type ``dst`` does not accept.

    ``src`` is the sender's type and ``dst`` the receiver's. The receiver
    ignores object fields it does not know. If ``open_ok``, a receiver type
    marked Open accepts new values and variants.
    """
    s, d = members(src), members(dst)
    d_open = open_ok and d.is_open
    d_kinds = {kind(o) for o in d.others}

    for value in sorted(s.literals, key=repr):
        if value in d.literals or type(value).__name__ in d_kinds or d_open:
            continue
        report.breaking.append(
            f"{path}: the sender may send {value!r}, which the receiver does not accept"
        )

    for other in s.others:
        k = kind(other)
        if k == "typeddict":
            target = _matching_typeddict(other, d.others)
            if target is not None:
                conforms_typeddict(other, target, path, report, open_ok=open_ok)
            elif not d_open:
                report.breaking.append(
                    f"{path}: the sender may send {other.__name__}, "  # type: ignore[attr-defined]
                    "which the receiver does not accept"
                )
        elif k in ("list", "dict"):
            target = next((o for o in d.others if kind(o) == k), None)
            if target is None:
                report.breaking.append(f"{path}: the receiver does not accept a {k}")
                continue
            for i, (s_arg, d_arg) in enumerate(
                zip(typing.get_args(other), typing.get_args(target), strict=True)
            ):
                part = "item" if k == "list" else ("key", "value")[i]
                conforms(s_arg, d_arg, f"{path}[{part}]", report, open_ok=open_ok)
        elif k in d_kinds or (k == "int" and "float" in d_kinds) or d_open:
            continue
        else:
            report.breaking.append(
                f"{path}: the sender may send any {k}, which the receiver does not accept"
            )


def _matching_typeddict(src: object, candidates: list[object]) -> object | None:
    typeddicts = [c for c in candidates if typing.is_typeddict(c)]
    for candidate in typeddicts:
        if candidate.__name__ == src.__name__:  # type: ignore[attr-defined]
            return candidate
    # A renamed class with the same role: compare by shape.
    return typeddicts[0] if len(typeddicts) == 1 else None


def conforms_typeddict(
    src: object, dst: object, path: str, report: Report, *, open_ok: bool
) -> None:
    s_hints = typing.get_type_hints(src, include_extras=True)
    d_hints = typing.get_type_hints(dst, include_extras=True)
    s_required = src.__required_keys__  # type: ignore[attr-defined]
    d_required = dst.__required_keys__  # type: ignore[attr-defined]
    for key, d_type in d_hints.items():
        if key not in d_required:
            if key in s_hints:
                conforms(s_hints[key], d_type, f"{path}.{key}", report, open_ok=open_ok)
            continue
        if key not in s_required:
            report.breaking.append(
                f"{path}.{key}: the receiver requires this field, "
                "but the sender may omit it"
            )
        if key in s_hints:
            conforms(s_hints[key], d_type, f"{path}.{key}", report, open_ok=open_ok)


# --------------------------------------------------------------------------- #
# Comparison
# --------------------------------------------------------------------------- #


def frames(module: types.ModuleType, union_name: str) -> dict[str, type]:
    by_type: dict[str, type] = {}
    for frame in typing.get_args(getattr(module, union_name)):
        (literal,) = typing.get_args(typing.get_type_hints(frame)["type"])
        by_type[literal] = frame
    return by_type


def public_names(source: str) -> dict[str, ast.stmt]:
    """Top-level names the module defines, mapped to their definitions."""
    names: dict[str, ast.stmt] = {}
    for node in ast.parse(source).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            names[node.name] = node
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names[target.id] = node
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names[node.target.id] = node
    return {name: node for name, node in names.items() if not name.startswith("_")}


def fingerprint(version: Version, name: str, node: ast.stmt) -> str:
    value = getattr(version.module, name)
    if isinstance(node, ast.FunctionDef):
        node = copy.deepcopy(node)
        if ast.get_docstring(node) is not None:
            node.body = node.body[1:]
        return ast.dump(node)
    if isinstance(node, ast.ClassDef) and not typing.is_typeddict(value):
        return f"class {name}"
    if isinstance(value, (str, int, float, bool)) and not isinstance(value, type):
        return repr(value)
    return describe(value)


def compare(old: Version, new: Version) -> Report:
    report = Report()

    old_names, new_names = public_names(old.source), public_names(new.source)
    for name in sorted(old_names.keys() - new_names.keys()):
        report.breaking.append(f"{name}: removed from the package")
    for name in sorted(new_names.keys() - old_names.keys()):
        report.changed.append(f"{name}: added")
    for name in sorted(old_names.keys() & new_names.keys()):
        before = fingerprint(old, name, old_names[name])
        after = fingerprint(new, name, new_names[name])
        if before == after:
            continue
        old_value = getattr(old.module, name)
        if isinstance(old_names[name], ast.FunctionDef) or (
            isinstance(old_value, (str, int, float, bool)) and name.isupper()
        ):
            report.breaking.append(f"{name}: value or behavior changed")
        else:
            report.changed.append(f"{name}: changed")

    old_client, new_client = (
        frames(old.module, "ClientFrame"),
        frames(new.module, "ClientFrame"),
    )
    for frame_type in sorted(old_client.keys() - new_client.keys()):
        report.breaking.append(f"{frame_type}: the server no longer accepts this frame")
    for frame_type in sorted(old_client.keys() & new_client.keys()):
        conforms_typeddict(
            old_client[frame_type],
            new_client[frame_type],
            f"[old client -> new server] {frame_type}",
            report,
            open_ok=False,
        )

    old_server, new_server = (
        frames(old.module, "ServerFrame"),
        frames(new.module, "ServerFrame"),
    )
    for frame_type in sorted(old_server.keys() - new_server.keys()):
        report.breaking.append(f"{frame_type}: the server no longer sends this frame")
    for frame_type in sorted(old_server.keys() & new_server.keys()):
        conforms_typeddict(
            new_server[frame_type],
            old_server[frame_type],
            f"[new server -> old client] {frame_type}",
            report,
            open_ok=True,
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find breaking changes between two protocol versions."
    )
    parser.add_argument("old_ref", help="git ref of the old version")
    parser.add_argument("new_ref", nargs="?", help="git ref of the new version")
    args = parser.parse_args()

    old = load(git_source(args.old_ref), "subspace_protocol_old")
    new_source = (
        git_source(args.new_ref) if args.new_ref else (REPO / MODULE_PATH).read_text()
    )
    new = load(new_source, "subspace_protocol_new")

    report = compare(old, new)
    for line in report.breaking:
        print(f"BREAKING {line}", file=sys.stderr)
    for line in report.changed:
        print(f"compatible {line}", file=sys.stderr)
    print(report.bump)


if __name__ == "__main__":
    main()
