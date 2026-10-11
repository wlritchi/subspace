# vim: filetype=python
"""Filesystem persistence helpers shared by the server's storage layers.

Factored out of ``pr_monitor`` so newer persistence (the agent registry) does not
have to depend on PR-monitoring code. The atomic-write strategy and the
filesystem-safe slugging mirror ``pr_monitor`` exactly.
"""

import json
import os
import re
from pathlib import Path


def safe_name(name: str) -> str:
    """Slug ``name`` into a filesystem-safe filename component.

    Mirrors ``pr_monitor._safe``: replace anything outside ``[A-Za-z0-9_.#-]``
    with ``_`` so arbitrary keys can be used as filenames.
    """
    return re.sub(r"[^A-Za-z0-9_.#-]", "_", name)


_KEY_SAFE = re.compile(r"[^A-Za-z0-9_.-]")


def key_filename(key: str) -> str:
    """An injective filesystem-safe filename component for a session key.

    Unlike ``safe_name``, two different keys never give the same name: each
    character outside ``[A-Za-z0-9_.-]`` becomes ``%XX`` (its UTF-8 bytes in
    hex), and ``%`` itself is encoded.
    """
    return _KEY_SAFE.sub(
        lambda m: "".join(f"%{b:02X}" for b in m.group().encode("utf-8")), key
    )


def atomic_write(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` atomically.

    Creates the parent directory if needed, writes to a ``<name>.tmp`` sibling,
    then ``os.replace`` swaps it into place so readers never observe a partial
    file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def load_json_dir(directory: Path) -> list[dict]:
    """Load every ``*.json`` file in ``directory`` as a dict.

    Unreadable files, invalid JSON, and JSON that does not decode to an object
    are skipped. Returns ``[]`` when the directory does not exist.
    """
    if not directory.is_dir():
        return []
    records: list[dict] = []
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(data, dict):
            records.append(data)
    return records
