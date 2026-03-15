# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path


def resolve_config_path(raw_path: str, anchor: Path = None) -> Path:
    """
    Resolves a raw string path into a absolute Path object.

    Args:
        raw_path: The string from the config (e.g., "../data/file.dbc").
        anchor: The directory to resolve against. Defaults to CWD.
    """
    if not raw_path:
        return Path()

    p = Path(raw_path).expanduser()

    if p.is_absolute():
        return p

    # Use provided anchor or fall back to the Current Working Directory
    base = anchor or Path.cwd()
    return (base / p).resolve()
