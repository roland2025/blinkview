# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from secrets import token_hex
from typing import List, Optional


def generate_id(prefix: str = "", prev: Optional[List[str]] = None) -> str:
    """
    Generates a short, random hex ID. Example: src_8f14e45f
    Ensures the ID does not exist in the 'prev' list if provided.
    """
    while True:
        random_hex = token_hex(4)

        if prefix:
            clean_prefix = prefix if prefix.endswith('_') else f"{prefix}_"
            new_id = f"{clean_prefix}{random_hex}"
        else:
            new_id = random_hex

        # If prev is None or new_id isn't in the list, we are good to go
        if prev is None or new_id not in prev:
            return new_id


def main():
    # --- Usage ---
    source_id = generate_id("src")
    print(source_id)  # Output: src_a1b2c3d4

    pipeline_id = generate_id("pipe")
    print(pipeline_id)  # Output: pipe_f9e8d7c6


if __name__ == "__main__":
    main()
