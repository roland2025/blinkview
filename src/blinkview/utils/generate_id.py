# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from secrets import token_hex


def generate_id(prefix: str = "") -> str:
    """Generates a short, random hex ID. Example: src_8f14e45f"""
    # token_hex(4) generates 4 bytes, which becomes an 8-character hex string
    random_hex = token_hex(4)

    if prefix:
        # Ensure the prefix ends with an underscore for consistency
        clean_prefix = prefix if prefix.endswith('_') else f"{prefix}_"
        return f"{clean_prefix}{random_hex}"

    return random_hex


def main():
    # --- Usage ---
    source_id = generate_id("src")
    print(source_id)  # Output: src_a1b2c3d4

    pipeline_id = generate_id("pipe")
    print(pipeline_id)  # Output: pipe_f9e8d7c6


if __name__ == "__main__":
    main()
