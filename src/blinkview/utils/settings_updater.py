# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Iterable


def update_object_from_config(obj, config: dict, fields: Iterable[str]) -> bool:
    """
    Updates 'obj' attributes from 'config' if they exist in 'fields'.
    Returns True if at least one value was actually changed.
    """
    changed = False
    for field in fields:
        if field in config:
            new_val = config[field]
            # Use getattr to compare against current state
            if getattr(obj, field) != new_val:
                setattr(obj, field, new_val)
                changed = True
    return changed
