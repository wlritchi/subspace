#!/usr/bin/env -S uv run -qs
# vim: filetype=python
"""Make a credential for an account in the subspace accounts file.

    scripts/new-credential.py ACCOUNT_ID [--expires 2027-01-01T00:00:00Z]

Prints the token once, to stdout, and the TOML block for the accounts file, with
the hash of the secret, to stderr. The server keeps only the hash, so store the
token now. Add the block under the account's [[account]] table.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lib"))

import accounts  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Make a credential for an account in the subspace accounts file."
    )
    parser.add_argument("account", help="the account id, for example ci")
    parser.add_argument(
        "--expires",
        help="an RFC 3339 date-time after which the credential is not valid",
    )
    args = parser.parse_args()
    try:
        accounts.validate_account_id(args.account)
    except accounts.AccountsError as exc:
        parser.error(str(exc))
    expires = ""
    if args.expires:
        try:
            if not re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
                r"(?:\.[0-9]+)?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})",
                args.expires,
            ):
                raise ValueError("expected an offset date-time")
            expiry = datetime.fromisoformat(args.expires.upper())
            expires = f"\nexpires_at = {expiry.isoformat()}"
        except ValueError:
            parser.error(f"--expires: not an RFC 3339 date-time: {args.expires}")
    token, credential_id, digest = accounts.new_token()
    print(
        f"# Add under the [[account]] table with id = {args.account!r}:\n"
        f"[[account.credential]]\n"
        f'id = "{credential_id}"\n'
        f'secret_sha256 = "{digest}"{expires}\n',
        file=sys.stderr,
    )
    print(token)


if __name__ == "__main__":
    main()
