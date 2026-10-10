#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Move the server's data from the pre-rename location to the subspace data dir.

Before the rename, the server kept its data in ~/.claude/notifications. This
script moves that directory to settings.data_dir() (SUBSPACE_DATA_DIR, else
$XDG_DATA_HOME/subspace). Then it links ~/.claude/notifications/token to the
moved token, because the `notifications` relay still reads its secret from that
path.

Stop the server before you run this script.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = ["subspace-protocol"]
#
# [tool.uv.sources]
# subspace-protocol = { path = "../protocol", editable = true }
# ///

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import settings

LEGACY_DIR = Path.home() / ".claude" / "notifications"


def migrate(src: Path, dst: Path, *, dry_run: bool) -> None:
    if not src.is_dir() or src.is_symlink():
        sys.exit(f"nothing to migrate: {src} is not a directory")
    if dst.exists() and any(dst.iterdir()):
        sys.exit(f"refusing to migrate: {dst} already exists and is not empty")

    token_link = src / "token"
    print(f"move {src} -> {dst}")
    if token_link.is_file():
        print(f"link {token_link} -> {dst / 'token'}")
    if dry_run:
        return

    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.rmdir()
    shutil.move(src, dst)
    if (dst / "token").is_file():
        src.mkdir(mode=0o700)
        token_link.symlink_to(dst / "token")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move the server data dir from ~/.claude/notifications."
    )
    parser.add_argument(
        "--from",
        dest="src",
        type=Path,
        default=LEGACY_DIR,
        help=f"pre-rename data dir (default {LEGACY_DIR})",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print the steps but do not do them"
    )
    args = parser.parse_args()
    migrate(args.src, settings.data_dir(), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
