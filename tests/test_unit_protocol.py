# vim: filetype=python
"""Consistency checks for the subspace-protocol package."""

from typing import get_args, get_type_hints

import subspace_protocol as proto


def _frame_types(union: object) -> dict[str, type]:
    by_type: dict[str, type] = {}
    for frame in get_args(union):
        (literal,) = get_args(get_type_hints(frame)["type"])
        assert literal not in by_type, f"two frames use type {literal!r}"
        by_type[literal] = frame
    return by_type


def _constants() -> set[str]:
    return {
        value
        for name, value in vars(proto).items()
        if name.isupper()
        and isinstance(value, str)
        and name not in {"DEFAULT_HOST", "AUTH_HEADER"}
    }


def test_every_constant_has_exactly_one_frame() -> None:
    client = _frame_types(proto.ClientFrame)
    server = _frame_types(proto.ServerFrame)
    assert not client.keys() & server.keys()
    assert client.keys() | server.keys() == _constants()


def test_requests_carry_req_id_and_pushes_do_not() -> None:
    no_reply = {proto.REGISTER, proto.ACK}
    for literal, frame in _frame_types(proto.ClientFrame).items():
        has_req_id = "req_id" in get_type_hints(frame)
        assert has_req_id == (literal not in no_reply), literal
    assert "req_id" not in get_type_hints(proto.Notify)
