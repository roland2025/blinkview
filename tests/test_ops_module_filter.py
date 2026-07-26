# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.ops.id_registry import NO_PARENT
from blinkview.ops.module_filter import nb_inherit_states, nb_rebuild_from_explicit, nb_update_subtree

OFF = 128
ALL = 0


def _parents(*ids):
    """Build a parent_array where entry i's parent is ids[i] (NO_PARENT if None)."""
    arr = np.empty(len(ids), dtype=np.uint32)
    for i, p in enumerate(ids):
        arr[i] = NO_PARENT if p is None else p
    return arr


# ---------------------------------------------------------------------------
# nb_inherit_states
# ---------------------------------------------------------------------------


def test_inherit_states_child_copies_enabled_parent():
    # 0 = root (enabled, level ALL), 1 = child of 0
    parent_array = _parents(None, 0)
    enabled = np.array([True, False], dtype=np.bool_)
    level = np.array([ALL, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    essential = np.zeros(2, dtype=np.bool_)

    nb_inherit_states(enabled, level, filt, parent_array, essential, True, 1, 2, OFF)

    assert enabled[1] == enabled[0]
    assert level[1] == level[0]


def test_inherit_states_disabled_child_bakes_off():
    parent_array = _parents(None, 0)
    enabled = np.array([False, False], dtype=np.bool_)
    level = np.array([ALL, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    essential = np.zeros(2, dtype=np.bool_)

    nb_inherit_states(enabled, level, filt, parent_array, essential, True, 1, 2, OFF)

    assert filt[1] == OFF


def test_inherit_states_hidden_non_essential_bakes_off():
    """show_hidden=False and non-essential module must bake to OFF even though enabled."""
    parent_array = _parents(None, 0)
    enabled = np.array([True, True], dtype=np.bool_)
    level = np.array([ALL, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    essential = np.zeros(2, dtype=np.bool_)

    nb_inherit_states(enabled, level, filt, parent_array, essential, False, 1, 2, OFF)

    assert filt[1] == OFF


def test_inherit_states_essential_does_not_cascade_to_children():
    """An essential leaf under a non-essential, hidden parent must still filter normally --
    essential must not propagate through inheritance, only the node's own flag counts."""
    # 0 = non-essential auto-created parent, 1 = essential child, 2 = non-essential grandchild
    parent_array = _parents(None, 0, 1)
    enabled = np.array([True, True, True], dtype=np.bool_)
    level = np.array([ALL, ALL, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL, ALL], dtype=np.uint8)
    essential = np.array([False, True, False], dtype=np.bool_)

    nb_inherit_states(enabled, level, filt, parent_array, essential, False, 1, 3, OFF)

    assert filt[1] == ALL  # essential leaf stays visible
    assert filt[2] == OFF  # non-essential grandchild does not inherit essential-ness


def test_inherit_states_parent_not_yet_less_than_i_is_skipped():
    """Chronological guarantee violated (parent_id >= i): inheritance copy is skipped, only
    the node's own current enabled/level bake into filter_mask."""
    parent_array = np.array([5, NO_PARENT], dtype=np.uint32)  # parent_array[0] = 5 (>= i=0)
    enabled = np.array([True, True], dtype=np.bool_)
    level = np.array([7, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    essential = np.array([True, True], dtype=np.bool_)

    nb_inherit_states(enabled, level, filt, parent_array, essential, False, 0, 1, OFF)

    assert enabled[0] is np.True_
    assert level[0] == 7
    assert filt[0] == 7


# ---------------------------------------------------------------------------
# nb_update_subtree
# ---------------------------------------------------------------------------


def test_update_subtree_enables_only_descendants():
    # 0 = root, 1 = child of 0, 2 = unrelated sibling root
    parent_array = _parents(None, 0, None)
    enabled = np.array([False, False, False], dtype=np.bool_)
    level = np.array([ALL, ALL, ALL], dtype=np.uint8)
    filt = np.full(3, OFF, dtype=np.uint8)
    essential = np.zeros(3, dtype=np.bool_)

    nb_update_subtree(
        enabled,
        level,
        filt,
        parent_array,
        essential,
        True,
        root_id=0,
        count=3,
        update_enabled=True,
        new_enabled=True,
        update_level=False,
        new_level=0,
        off_value=OFF,
    )

    assert enabled[0] and enabled[1]
    assert not enabled[2]
    assert filt[0] == ALL and filt[1] == ALL
    assert filt[2] == OFF


def test_update_subtree_level_only_does_not_touch_enabled():
    parent_array = _parents(None, 0)
    enabled = np.array([True, True], dtype=np.bool_)
    level = np.array([ALL, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    essential = np.zeros(2, dtype=np.bool_)

    nb_update_subtree(
        enabled,
        level,
        filt,
        parent_array,
        essential,
        True,
        root_id=0,
        count=2,
        update_enabled=False,
        new_enabled=False,
        update_level=True,
        new_level=9,
        off_value=OFF,
    )

    assert enabled[0] and enabled[1]  # untouched
    assert level[0] == 9 and level[1] == 9
    assert filt[0] == 9 and filt[1] == 9


def test_update_subtree_stops_at_branch_outside_root():
    """A node whose parent is not part of the subtree (chronologically appears in-range but
    branches off elsewhere) must not be swept up into the update."""
    # 0 = root A, 1 = child of A, 2 = root B (independent), 3 = child of B
    parent_array = _parents(None, 0, None, 2)
    enabled = np.zeros(4, dtype=np.bool_)
    level = np.array([ALL, ALL, ALL, ALL], dtype=np.uint8)
    filt = np.full(4, OFF, dtype=np.uint8)
    essential = np.zeros(4, dtype=np.bool_)

    nb_update_subtree(
        enabled,
        level,
        filt,
        parent_array,
        essential,
        True,
        root_id=0,
        count=4,
        update_enabled=True,
        new_enabled=True,
        update_level=False,
        new_level=0,
        off_value=OFF,
    )

    assert enabled[0] and enabled[1]
    assert not enabled[2] and not enabled[3]


# ---------------------------------------------------------------------------
# nb_rebuild_from_explicit
# ---------------------------------------------------------------------------


def test_rebuild_from_explicit_override_wins_over_inheritance():
    # 0 = root, 1 = child, explicitly disabled despite parent being enabled
    parent_array = _parents(None, 0)
    enabled = np.array([True, False], dtype=np.bool_)  # [1] pre-set by caller from saved state
    level = np.array([ALL, ALL], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    explicit = np.array([False, True], dtype=np.bool_)
    essential = np.zeros(2, dtype=np.bool_)

    nb_rebuild_from_explicit(
        enabled,
        level,
        filt,
        parent_array,
        explicit,
        essential,
        True,
        count=2,
        default_enabled=True,
        default_level=ALL,
        off_value=OFF,
    )

    assert enabled[0] is np.True_  # root falls back to default (no parent)
    assert enabled[1] is np.False_  # explicit override preserved, not overwritten by inheritance
    assert filt[1] == OFF


def test_rebuild_from_explicit_non_explicit_inherits_parent():
    parent_array = _parents(None, 0)
    enabled = np.array([True, False], dtype=np.bool_)
    level = np.array([ALL, 5], dtype=np.uint8)
    filt = np.array([ALL, ALL], dtype=np.uint8)
    explicit = np.array([False, False], dtype=np.bool_)  # neither explicit
    essential = np.zeros(2, dtype=np.bool_)

    nb_rebuild_from_explicit(
        enabled,
        level,
        filt,
        parent_array,
        explicit,
        essential,
        True,
        count=2,
        default_enabled=True,
        default_level=ALL,
        off_value=OFF,
    )

    assert enabled[1] == enabled[0]
    assert level[1] == level[0]


def test_rebuild_from_explicit_root_without_parent_uses_defaults():
    parent_array = _parents(None)
    enabled = np.array([False], dtype=np.bool_)
    level = np.array([9], dtype=np.uint8)
    filt = np.array([ALL], dtype=np.uint8)
    explicit = np.array([False], dtype=np.bool_)
    essential = np.zeros(1, dtype=np.bool_)

    nb_rebuild_from_explicit(
        enabled,
        level,
        filt,
        parent_array,
        explicit,
        essential,
        True,
        count=1,
        default_enabled=True,
        default_level=ALL,
        off_value=OFF,
    )

    assert enabled[0] is np.True_
    assert level[0] == ALL
