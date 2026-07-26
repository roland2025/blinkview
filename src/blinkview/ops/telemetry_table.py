# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit


@app_njit()
def nb_initialize_new_modules(
    n_mods: int,
    sequences: np.ndarray,
    painted_seqs: np.ndarray,
    arrival_times: np.ndarray,
    change_times: np.ndarray,
    current_levels: np.ndarray,
    painted_levels: np.ndarray,
    current_lengths: np.ndarray,
    painted_lengths: np.ndarray,
    current_buffers: np.ndarray,
    painted_buffers: np.ndarray,
    now: float,
    max_msg_bytes: int,
    newly_active_ids: np.ndarray,  # NEW: Output buffer for IDs
    is_visible: np.ndarray,
) -> int:
    """
    Initializes modules and returns the count of newly initialized modules.
    Populates `newly_active_ids` with the activated mod_ids.

    Skips modules that are already visible: those are handled by nb_update_visible_state
    instead, which (unlike this function) also flags them for a Qt dataChanged emit. Without
    this guard, a module whose painted_seqs got reset to 0 by a playback-clock scrub
    backward past its first message, then scrubbed forward again, would get silently
    repainted here with no accompanying signal - leaving the view showing a stale "---".
    """
    count = 0
    for mod_id in range(n_mods):
        if is_visible[mod_id]:
            continue

        if painted_seqs[mod_id] == 0 and sequences[mod_id] > 0:
            painted_seqs[mod_id] = sequences[mod_id]
            arrival_times[mod_id] = now
            change_times[mod_id] = now
            painted_levels[mod_id] = current_levels[mod_id]

            c_len = min(current_lengths[mod_id], max_msg_bytes)
            painted_lengths[mod_id] = c_len

            off = mod_id * max_msg_bytes
            for b in range(c_len):
                painted_buffers[off + b] = current_buffers[off + b]

            newly_active_ids[count] = mod_id
            count += 1

    return count


@app_njit()
def nb_update_visible_state(
    visible_mod_ids: np.ndarray,
    current_seqs: np.ndarray,
    painted_seqs: np.ndarray,
    current_levels: np.ndarray,  # Added
    painted_levels: np.ndarray,  # Added
    arrival_times: np.ndarray,
    change_times: np.ndarray,
    now: float,
    fade_dur: float,
    stale_limit: float,
    buffer_time: float,
    current_buffers: np.ndarray,
    current_lengths: np.ndarray,
    painted_buffers: np.ndarray,
    painted_lengths: np.ndarray,
    max_msg_bytes: int,
):
    """
    Evaluates visible rows and updates painted state in-place at C-speed.
    Returns a single boolean mask of rows that need a Qt signal emitted.

    Uses `c_seq != p_seq` (not `c_seq > p_seq`) so that scrubbing the playback clock
    *backward* - which can make a module's latest-as-of-ts sequence decrease, or drop back
    to 0 for a module with no message yet at the scrubbed-to time - still repaints instead of
    silently keeping a stale forward-in-time value. In LIVE mode current_seqs only ever grows,
    so `!=` and `>` are equivalent there; this is a strict generalization, not a behavior change.
    """
    n_visible = len(visible_mod_ids)
    needs_update = np.zeros(n_visible, dtype=np.bool_)

    for i in range(n_visible):
        mod_id = visible_mod_ids[i]
        c_seq = current_seqs[mod_id]
        p_seq = painted_seqs[mod_id]

        if c_seq == 0 and p_seq == 0:
            continue

        if c_seq != p_seq:
            needs_update[i] = True

            # --- MOVED FROM PYTHON: Direct state updates ---
            arrival_times[mod_id] = now
            painted_seqs[mod_id] = c_seq
            painted_levels[mod_id] = current_levels[mod_id]

            if c_seq == 0:
                # Scrubbed back to before this module's first message - clear the painted
                # buffer so data() falls back to "---" instead of showing a future value.
                change_times[mod_id] = now
                painted_lengths[mod_id] = 0
            else:
                c_len = current_lengths[mod_id]
                p_len = painted_lengths[mod_id]

                msg_changed = False
                if c_len != p_len:
                    msg_changed = True
                else:
                    # Fast byte-by-byte comparison
                    offset = mod_id * max_msg_bytes
                    for b in range(c_len):
                        idx = offset + b
                        if current_buffers[idx] != painted_buffers[idx]:
                            msg_changed = True
                            break

                # --- MOVED FROM PYTHON: Buffer state updates ---
                if msg_changed:
                    change_times[mod_id] = now
                    painted_lengths[mod_id] = c_len

                    # In Numba, a simple for-loop over contiguous memory compiles
                    # down to the equivalent of a C memcpy, avoiding Python slice overhead.
                    offset = mod_id * max_msg_bytes
                    for b in range(c_len):
                        painted_buffers[offset + b] = current_buffers[offset + b]

        else:
            # Animation/Stale timeout checks
            arr_time = arrival_times[mod_id]
            chg_time = change_times[mod_id]

            elapsed_flash = now - chg_time
            elapsed_stale = now - arr_time

            if elapsed_flash <= (fade_dur + buffer_time) or (stale_limit <= elapsed_stale <= stale_limit + buffer_time):
                needs_update[i] = True

    return needs_update
