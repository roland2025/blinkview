# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from copy import deepcopy


def get_by_path(
    data: dict, path: str, default=None, drop_keys: list = None, make_deep_copy: bool = False, depth: int = None
):
    """
    Retrieves a value from a nested dict/list using a path string.

    :param data: The dictionary or list to search.
    :param path: The JSON Pointer-style path (e.g., "/devices/ABC/device").
    :param default: The value to return if the path is not found.
    :param drop_keys: A list of top-level keys to exclude from the returned dictionary.
    :param make_deep_copy: If True, returns a completely independent clone of the data.
    :param depth: Limits how many levels deep to serialize. Deeper containers become empty {} or [].
    """
    # Traverse the path
    if not path or path == "/":
        current = data
    else:
        keys = [k for k in path.split("/") if k]
        current = data

        for key in keys:
            if isinstance(current, dict):
                if key in current:
                    current = current[key]
                else:
                    return default
            elif isinstance(current, list):
                try:
                    current = current[int(key)]
                except (ValueError, IndexError):
                    return default
            else:
                return default

    # Handle top-level key dropping
    if isinstance(current, dict) and drop_keys:
        # This inherently creates a shallow copy of the target node
        current = {k: v for k, v in current.items() if k not in drop_keys}

    # Handle Depth Limiting (This inherently deep-copies the pruned result)
    if depth is not None:

        def _limit_depth(obj, current_depth, max_depth):
            # If we hit the depth ceiling, hollow out any further containers
            if current_depth >= max_depth:
                if isinstance(obj, dict):
                    return {}
                if isinstance(obj, list):
                    return []
                return obj  # Primitives (strings, ints, bools) are kept

            # Otherwise, keep digging
            if isinstance(obj, dict):
                return {k: _limit_depth(v, current_depth + 1, max_depth) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_limit_depth(v, current_depth + 1, max_depth) for v in obj]

            return obj

        return _limit_depth(current, 0, depth)

    # Handle standard deep copying for total memory isolation
    if make_deep_copy:
        return deepcopy(current)

    return current
