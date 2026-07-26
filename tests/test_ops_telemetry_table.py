# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.ops.telemetry_table import nb_initialize_new_modules, nb_update_visible_state

MAX_MSG_BYTES = 16


def _buffer(n_mods, contents=None):
    buf = np.zeros(n_mods * MAX_MSG_BYTES, dtype=np.uint8)
    if contents:
        for mod_id, msg in contents.items():
            b = msg.encode("utf-8")
            off = mod_id * MAX_MSG_BYTES
            buf[off : off + len(b)] = np.frombuffer(b, dtype=np.uint8)
    return buf


def _msg(buf, mod_id, length):
    off = mod_id * MAX_MSG_BYTES
    return buf[off : off + length].tobytes().decode("utf-8")


# ---------------------------------------------------------------------------
# nb_initialize_new_modules
# ---------------------------------------------------------------------------


def test_initialize_new_modules_activates_module_with_first_message():
    n = 2
    sequences = np.array([5, 0], dtype=np.int64)
    painted_seqs = np.zeros(n, dtype=np.int64)
    arrival_times = np.zeros(n)
    change_times = np.zeros(n)
    current_levels = np.array([2, 0], dtype=np.uint8)
    painted_levels = np.zeros(n, dtype=np.uint8)
    current_lengths = np.array([5, 0], dtype=np.int64)
    painted_lengths = np.zeros(n, dtype=np.int64)
    current_buffers = _buffer(n, {0: "hello"})
    painted_buffers = _buffer(n)
    newly_active_ids = np.zeros(n, dtype=np.int64)
    is_visible = np.zeros(n, dtype=np.bool_)

    count = nb_initialize_new_modules(
        n,
        sequences,
        painted_seqs,
        arrival_times,
        change_times,
        current_levels,
        painted_levels,
        current_lengths,
        painted_lengths,
        current_buffers,
        painted_buffers,
        now=100.0,
        max_msg_bytes=MAX_MSG_BYTES,
        newly_active_ids=newly_active_ids,
        is_visible=is_visible,
    )

    assert count == 1
    assert newly_active_ids[0] == 0
    assert painted_seqs[0] == 5
    assert painted_levels[0] == 2
    assert _msg(painted_buffers, 0, 5) == "hello"
    assert arrival_times[0] == 100.0
    assert change_times[0] == 100.0


def test_initialize_new_modules_skips_module_with_no_messages_yet():
    n = 1
    sequences = np.array([0], dtype=np.int64)
    painted_seqs = np.zeros(n, dtype=np.int64)
    arrival_times = np.zeros(n)
    change_times = np.zeros(n)
    current_levels = np.zeros(n, dtype=np.uint8)
    painted_levels = np.zeros(n, dtype=np.uint8)
    current_lengths = np.zeros(n, dtype=np.int64)
    painted_lengths = np.zeros(n, dtype=np.int64)
    current_buffers = _buffer(n)
    painted_buffers = _buffer(n)
    newly_active_ids = np.zeros(n, dtype=np.int64)
    is_visible = np.zeros(n, dtype=np.bool_)

    count = nb_initialize_new_modules(
        n,
        sequences,
        painted_seqs,
        arrival_times,
        change_times,
        current_levels,
        painted_levels,
        current_lengths,
        painted_lengths,
        current_buffers,
        painted_buffers,
        now=1.0,
        max_msg_bytes=MAX_MSG_BYTES,
        newly_active_ids=newly_active_ids,
        is_visible=is_visible,
    )

    assert count == 0


def test_initialize_new_modules_skips_already_visible_modules():
    """Already-visible modules are handled by nb_update_visible_state instead - re-painting
    them here would silently skip the Qt dataChanged emit that function is responsible for."""
    n = 1
    sequences = np.array([3], dtype=np.int64)
    painted_seqs = np.zeros(n, dtype=np.int64)
    arrival_times = np.zeros(n)
    change_times = np.zeros(n)
    current_levels = np.zeros(n, dtype=np.uint8)
    painted_levels = np.zeros(n, dtype=np.uint8)
    current_lengths = np.array([1], dtype=np.int64)
    painted_lengths = np.zeros(n, dtype=np.int64)
    current_buffers = _buffer(n, {0: "x"})
    painted_buffers = _buffer(n)
    newly_active_ids = np.zeros(n, dtype=np.int64)
    is_visible = np.array([True], dtype=np.bool_)

    count = nb_initialize_new_modules(
        n,
        sequences,
        painted_seqs,
        arrival_times,
        change_times,
        current_levels,
        painted_levels,
        current_lengths,
        painted_lengths,
        current_buffers,
        painted_buffers,
        now=1.0,
        max_msg_bytes=MAX_MSG_BYTES,
        newly_active_ids=newly_active_ids,
        is_visible=is_visible,
    )

    assert count == 0
    assert painted_seqs[0] == 0  # untouched


# ---------------------------------------------------------------------------
# nb_update_visible_state
# ---------------------------------------------------------------------------


def _visible_state(n=1):
    return dict(
        current_seqs=np.zeros(n, dtype=np.int64),
        painted_seqs=np.zeros(n, dtype=np.int64),
        current_levels=np.zeros(n, dtype=np.uint8),
        painted_levels=np.zeros(n, dtype=np.uint8),
        arrival_times=np.zeros(n),
        change_times=np.zeros(n),
        current_lengths=np.zeros(n, dtype=np.int64),
        painted_lengths=np.zeros(n, dtype=np.int64),
        current_buffers=_buffer(n),
        painted_buffers=_buffer(n),
    )


def test_update_visible_state_repaints_when_sequence_changes():
    s = _visible_state()
    s["current_seqs"][0] = 5
    s["current_lengths"][0] = 3
    s["current_buffers"] = _buffer(1, {0: "new"})

    needs_update = nb_update_visible_state(
        visible_mod_ids=np.array([0], dtype=np.int64),
        now=10.0,
        fade_dur=1.0,
        stale_limit=5.0,
        buffer_time=0.5,
        max_msg_bytes=MAX_MSG_BYTES,
        **s,
    )

    assert needs_update[0]
    assert s["painted_seqs"][0] == 5
    assert s["painted_lengths"][0] == 3
    assert _msg(s["painted_buffers"], 0, 3) == "new"
    assert s["arrival_times"][0] == 10.0


def test_update_visible_state_handles_scrub_backward_to_no_message():
    """A backward playback scrub can drop current_seqs to 0 for a module that had a painted
    message; the != comparison (not >) must still detect this and clear the painted buffer."""
    s = _visible_state()
    s["painted_seqs"][0] = 7
    s["painted_lengths"][0] = 3
    s["current_seqs"][0] = 0  # scrubbed back before this module's first message

    needs_update = nb_update_visible_state(
        visible_mod_ids=np.array([0], dtype=np.int64),
        now=20.0,
        fade_dur=1.0,
        stale_limit=5.0,
        buffer_time=0.5,
        max_msg_bytes=MAX_MSG_BYTES,
        **s,
    )

    assert needs_update[0]
    assert s["painted_seqs"][0] == 0
    assert s["painted_lengths"][0] == 0
    assert s["change_times"][0] == 20.0


def test_update_visible_state_no_change_when_both_seqs_zero():
    s = _visible_state()
    needs_update = nb_update_visible_state(
        visible_mod_ids=np.array([0], dtype=np.int64),
        now=1.0,
        fade_dur=1.0,
        stale_limit=5.0,
        buffer_time=0.5,
        max_msg_bytes=MAX_MSG_BYTES,
        **s,
    )
    assert not needs_update[0]


def test_update_visible_state_same_sequence_flashes_within_fade_window():
    s = _visible_state()
    s["current_seqs"][0] = 3
    s["painted_seqs"][0] = 3
    s["change_times"][0] = 9.5  # recent change

    needs_update = nb_update_visible_state(
        visible_mod_ids=np.array([0], dtype=np.int64),
        now=10.0,
        fade_dur=1.0,
        stale_limit=100.0,
        buffer_time=0.5,
        max_msg_bytes=MAX_MSG_BYTES,
        **s,
    )

    assert needs_update[0]  # still within fade_dur + buffer_time of the last change


def test_update_visible_state_same_sequence_settles_after_fade_and_before_stale():
    s = _visible_state()
    s["current_seqs"][0] = 3
    s["painted_seqs"][0] = 3
    s["change_times"][0] = 0.0
    s["arrival_times"][0] = 0.0

    needs_update = nb_update_visible_state(
        visible_mod_ids=np.array([0], dtype=np.int64),
        now=50.0,
        fade_dur=1.0,
        stale_limit=100.0,
        buffer_time=0.5,
        max_msg_bytes=MAX_MSG_BYTES,
        **s,
    )

    assert not needs_update[0]  # long past fade, not yet near stale_limit
