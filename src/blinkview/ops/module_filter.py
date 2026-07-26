# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.ops.id_registry import NO_PARENT


@app_njit()
def nb_inherit_states(
    new_enabled: np.ndarray,
    new_level: np.ndarray,
    new_filter: np.ndarray,
    parent_array: np.ndarray,
    essential_mask: np.ndarray,
    show_hidden: bool,
    start_idx: int,
    end_idx: int,
    off_value: int,
):
    """
    Fast-path inheritance for newly allocated UI masks.
    Mutates new_enabled, new_level, and new_filter arrays in place.
    """
    for i in range(start_idx, end_idx):
        parent_id = parent_array[i]

        # Chronological guarantee: parent_id must be < i
        if parent_id != NO_PARENT and parent_id < i:
            new_enabled[i] = new_enabled[parent_id]
            new_level[i] = new_level[parent_id]

        # Bake the filter fresh from THIS node's own enabled/level/essential
        # rather than copying the parent's already-baked filter. essential
        # is per-node and must NOT cascade through inheritance -- an
        # essential leaf sitting under an auto-created, non-essential
        # parent path segment should still filter normally.
        if new_enabled[i] and (show_hidden or essential_mask[i]):
            new_filter[i] = new_level[i]
        else:
            new_filter[i] = off_value


@app_njit()
def nb_update_subtree(
    enabled_mask: np.ndarray,
    level_mask: np.ndarray,
    filter_mask: np.ndarray,
    parent_array: np.ndarray,
    essential_mask: np.ndarray,
    show_hidden: bool,
    root_id: int,
    count: int,
    update_enabled: bool,
    new_enabled: bool,
    update_level: bool,
    new_level: int,
    off_value: int,
):
    """
    Updates the enabled state and/or log level for a root module and all its descendants.
    Exploits the chronological property (parent_id < child_id) for a single-pass update.
    """
    # Track which modules are part of the subtree
    is_in_subtree = np.zeros(count, dtype=np.bool_)
    is_in_subtree[root_id] = True

    for i in range(root_id, count):
        p_id = parent_array[i]

        # If this node is the root OR its parent is in our subtree mask
        if i == root_id or (p_id != NO_PARENT and is_in_subtree[p_id]):
            is_in_subtree[i] = True

            if update_enabled:
                enabled_mask[i] = new_enabled
            if update_level:
                level_mask[i] = new_level

            # Update the optimized baked mask
            if enabled_mask[i] and (show_hidden or essential_mask[i]):
                filter_mask[i] = level_mask[i]
            else:
                filter_mask[i] = off_value


@app_njit()
def nb_rebuild_from_explicit(
    enabled_mask: np.ndarray,
    level_mask: np.ndarray,
    filter_mask: np.ndarray,
    parent_array: np.ndarray,
    explicit_mask: np.ndarray,
    essential_mask: np.ndarray,
    show_hidden: bool,
    count: int,
    default_enabled: bool,
    default_level: int,
    off_value: int,
):
    """
    Rebuilds the entire state tree in a single pass based on explicit overrides.
    Relies on the chronological guarantee (parent_id < child_id).
    """
    for i in range(count):
        # If this node was NOT explicitly set in the saved state, it inherits
        if not explicit_mask[i]:
            p_id = parent_array[i]
            if p_id != NO_PARENT and p_id < i:
                enabled_mask[i] = enabled_mask[p_id]
                level_mask[i] = level_mask[p_id]
            else:
                enabled_mask[i] = default_enabled
                level_mask[i] = default_level

        # Always bake the optimized filter mask
        if enabled_mask[i] and (show_hidden or essential_mask[i]):
            filter_mask[i] = level_mask[i]
        else:
            filter_mask[i] = off_value
