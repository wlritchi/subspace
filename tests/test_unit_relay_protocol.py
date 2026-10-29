# vim: filetype=python
"""Pin the relay plugin's copy of the protocol names to subspace_protocol.

The relay has no dependency on subspace_protocol at run time. It keeps its own
frame names in plugin/subspace/lib/wsproto.py and its own vocabulary in
plugin/subspace/lib/messaging.py. These tests fail when a copy drifts.
"""

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import get_args, get_type_hints

import subspace_protocol as proto

PLUGIN_LIB = Path(__file__).resolve().parents[1] / "plugin" / "subspace" / "lib"


def _load(name: str) -> ModuleType:
    # A unique module name, because lib/ on the test path also has a messaging.py.
    spec = importlib.util.spec_from_file_location(
        f"relay_{name}", PLUGIN_LIB / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wsproto = _load("wsproto")
messaging = _load("messaging")


def _relay_constants() -> dict[str, object]:
    return {
        name: value
        for name, value in vars(wsproto).items()
        if name.isupper() and isinstance(value, str | int)
    }


def _frame_literals(union: object) -> set[str]:
    literals: set[str] = set()
    for frame in get_args(union):
        literals.update(get_args(get_type_hints(frame)["type"]))
    return literals


def test_relay_has_frame_constants() -> None:
    assert len(_relay_constants()) > 10


def test_every_relay_constant_matches_the_protocol() -> None:
    for name, value in _relay_constants().items():
        assert hasattr(proto, name), f"{name} is not in subspace_protocol"
        assert getattr(proto, name) == value, name


def test_every_relay_frame_name_is_a_protocol_frame() -> None:
    frames = _frame_literals(proto.ClientFrame) | _frame_literals(proto.ServerFrame)
    for name, value in _relay_constants().items():
        if name.startswith("DEFAULT_"):
            continue
        assert value in frames, f"{name}={value!r} names no frame"


def test_relay_vocabulary_matches_the_protocol() -> None:
    assert messaging.INTENTS == set(get_args(proto.Intent))
    assert messaging.SEVERITIES == set(get_args(proto.Severity))
    assert messaging.THRESHOLD_NAMES == set(get_args(proto.Threshold))
    assert set(messaging._LEVELS) == set(get_args(proto.Level))
    # The relay sorts levels and thresholds on one scale.
    assert list(messaging._LEVELS) == list(get_args(proto.Level))
    assert list(messaging._THRESHOLDS) == list(get_args(proto.Threshold))


def test_fields_the_relay_reads_are_in_the_protocol() -> None:
    notify = get_type_hints(proto.Notify)
    assert {"id", "content", "meta", "session_id"} <= set(notify)
    meta = get_type_hints(proto.MessageMeta)
    assert get_args(meta["kind"]) == ("message",)
    assert {"level", "threshold", "severity"} <= set(meta)
    # The relay marks a coalesced channel event "high" if any part is "high".
    assert "high" in get_args(proto.NotifySeverity)
    assert "session_id" in get_type_hints(proto.WhoamiResult)
