# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Numba backend for the multi-rule key-value extractor - see
core/types/kv_extraction.py's module docstring and plans/kv-extractor-numba-backend.md.

The whole-batch entry point, nb_process_kv_batch, mirrors ops/dispatch.py's
nb_process_batch_kernel / parsers/binary_parser.py's run() loop: one call processes as much of an
input PooledLogBatch as fits into the current output batch, and reports whether it stopped early
because the output filled up (so the caller can flush, acquire a fresh output batch, and resume
the *same* input batch exactly where it left off) - never partially re-emitting a row across that
boundary, since a row is only ever started once there's already room-checked space for it.

KEY_VALUE is the only rule type that needs a per-row dynamic module lookup (the key text varies
per row) - it reuses the exact discovery-tracker/temp-id machinery ops/modules.py's
nb_parse_module_tags_statemachine already established (ops/discovery.py's nb_resolve_module_id,
core/types/modules.py's MODULE_TEMP_ID_BASE), so "add a new module efficiently" here means the
same thing it means there: write the candidate name into the shared scratch tracker, resolve it
against the permanent registry hash table first, fall back to an in-batch temp id, and let the
caller's post_process() step do the one real (Python-side) registry insert per distinct new name
per output batch - not per row.
"""

from blinkview.core.numba_config import app_njit
from blinkview.core.types.kv_extraction import KV_MATCH_CONTAINS, KV_MATCH_ENDS_WITH, KvRuleID
from blinkview.core.types.modules import MODULE_ID_FULL, MODULE_ID_UNKNOWN
from blinkview.ops.discovery import nb_resolve_module_id
from blinkview.ops.segments import nb_bundle_push_len
from blinkview.ops.strings import nb_is_whitespace, nb_to_lower

CHAR_DQUOTE = 34
CHAR_SQUOTE = 39
CHAR_COMMA = 44
CHAR_RBRACE = 125
CHAR_RBRACKET = 93

# Numba can't type a plain Python class's attribute access (KvRuleID.KEY_VALUE) as a global inside
# an njit function - extracted to flat module-level int constants instead, same as
# ops/pipeline.py does for ParserID (e.g. `LEVEL_NAME_MAP = ParserID.LEVEL_NAME_MAP`).
KV_RULE_KEY_VALUE = KvRuleID.KEY_VALUE
KV_RULE_ANCHOR_WORD = KvRuleID.ANCHOR_WORD
KV_RULE_JSON_LITE = KvRuleID.JSON_LITE
KV_RULE_DSV = KvRuleID.DSV
KV_RULE_POSITIONAL = KvRuleID.POSITIONAL


@app_njit(inline="always")
def nb_write_prefixed_key_lower(tracker, name_prefix_bytes, key_buffer, key_start, key_len):
    """Writes name_prefix_bytes + lowercased(key_buffer[key_start:key_start+key_len]) into
    tracker.name_bytes at the tracker's current write frontier (tracker.bytes_cursor[0]) -
    WITHOUT advancing the cursor itself. nb_resolve_module_id decides whether to claim that span
    (only if the name turns out to be genuinely new) - same handoff nb_parse_fixed_width_name uses
    (ops/modules.py). Returns (write_start, total_len)."""
    write_start = tracker.bytes_cursor[0]
    prefix_len = len(name_prefix_bytes)

    for i in range(prefix_len):
        tracker.name_bytes[write_start + i] = name_prefix_bytes[i]
    for i in range(key_len):
        tracker.name_bytes[write_start + prefix_len + i] = nb_to_lower(key_buffer[key_start + i])

    return write_start, prefix_len + key_len


@app_njit(inline="always")
def nb_find_word_span(buffer, start, end, z_start, y_count):
    """Locates the byte span of words [z_start, z_start+y_count) (y_count=0 means "to the end")
    in the whitespace-delimited word sequence within buffer[start:end]. Returns (-1, -1) if that
    span doesn't exist. Shared by ANCHOR_WORD (after its pattern precondition matches) and
    POSITIONAL (unconditional)."""
    start_byte = -1
    end_byte = -1
    word_count = 0
    in_word = False

    for j in range(start, end):
        c = buffer[j]
        is_ws = c == 32 or c == 9 or c == 10 or c == 13

        if not in_word:
            if not is_ws:
                in_word = True
                if word_count == z_start:
                    start_byte = j
        else:
            if is_ws:
                in_word = False
                if y_count > 0 and word_count == z_start + y_count - 1:
                    end_byte = j
                    break
                if word_count >= z_start:
                    end_byte = j
                word_count += 1

    if in_word and word_count >= z_start:
        if y_count == 0 or word_count < z_start + y_count:
            end_byte = end

    if start_byte == -1 or end_byte == -1 or start_byte >= end_byte:
        return -1, -1

    return start_byte, end_byte


@app_njit(inline="always")
def nb_extract_key_value_row(in_b, i, cfg, tracker, string_table, out_b, device_id_int):
    start = in_b.offsets[i]
    end = start + in_b.lengths[i]
    buffer = in_b.buffer

    prefix_bytes = cfg.prefix_bytes
    p_len = len(prefix_bytes)
    if p_len > 0 and (end - start) >= p_len:
        match = True
        for idx in range(p_len):
            if buffer[start + idx] != prefix_bytes[idx]:
                match = False
                break
        if match:
            start += p_len

    ts_ns = in_b.timestamps[i]
    rx_ns = in_b.rx_timestamps[i]
    level = in_b.levels[i]

    field_delim_int = cfg.field_delim
    kv_delim_int = cfg.kv_delim
    name_prefix_bytes = cfg.name_prefix_bytes

    chunk_start = start
    kv_pos = -1
    in_quote = 0

    for j in range(start, end + 1):
        c = buffer[j] if j < end else field_delim_int

        if (c == CHAR_DQUOTE or c == CHAR_SQUOTE) and kv_pos != -1 and j < end:
            if in_quote == 0:
                in_quote = c
            elif in_quote == c:
                in_quote = 0

        if c == kv_delim_int and kv_pos == -1 and j < end:
            kv_pos = j
        elif (c == field_delim_int and in_quote == 0) or j == end:
            if kv_pos != -1:
                k_start = chunk_start
                k_end = kv_pos
                while k_start < k_end and nb_is_whitespace(buffer[k_start]):
                    k_start += 1
                while k_end > k_start and nb_is_whitespace(buffer[k_end - 1]):
                    k_end -= 1

                v_start = kv_pos + 1
                v_end = j
                while v_start < v_end and nb_is_whitespace(buffer[v_start]):
                    v_start += 1
                while v_end > v_start and nb_is_whitespace(buffer[v_end - 1]):
                    v_end -= 1

                if v_start < v_end and (buffer[v_start] == CHAR_DQUOTE or buffer[v_start] == CHAR_SQUOTE):
                    quote_char = buffer[v_start]
                    if buffer[v_end - 1] == quote_char:
                        v_start += 1
                        v_end -= 1

                if k_start < k_end and v_start < v_end:
                    key_len = k_end - k_start
                    write_start, total_len = nb_write_prefixed_key_lower(
                        tracker, name_prefix_bytes, buffer, k_start, key_len
                    )
                    target_mod_id = nb_resolve_module_id(
                        tracker.name_bytes, write_start, total_len, string_table, tracker
                    )

                    if target_mod_id != MODULE_ID_FULL and target_mod_id != MODULE_ID_UNKNOWN:
                        ok = nb_bundle_push_len(
                            out_b,
                            ts_ns,
                            rx_ns,
                            buffer[v_start:v_end],
                            v_end - v_start,
                            level,
                            target_mod_id,
                            device_id_int,
                            0,
                            0,
                            0,
                            0,
                        )
                        if not ok:
                            return False

            chunk_start = j + 1
            kv_pos = -1
            in_quote = 0

    return True


@app_njit(inline="always")
def nb_extract_anchor_word_row(in_b, i, cfg, out_b, device_id_int):
    start = in_b.offsets[i]
    end = start + in_b.lengths[i]
    buffer = in_b.buffer

    pattern_bytes = cfg.pattern_bytes
    pat_len = len(pattern_bytes)
    if (end - start) < pat_len:
        return True

    match_mode = cfg.match_mode
    matched = False

    if match_mode == KV_MATCH_ENDS_WITH:
        offset = end - pat_len
        matched = True
        for idx in range(pat_len):
            if buffer[offset + idx] != pattern_bytes[idx]:
                matched = False
                break
    elif match_mode == KV_MATCH_CONTAINS:
        first_byte = pattern_bytes[0] if pat_len > 0 else 0
        for idx in range(start, end - pat_len + 1):
            if buffer[idx] != first_byte:
                continue
            match_found = True
            for j in range(1, pat_len):
                if buffer[idx + j] != pattern_bytes[j]:
                    match_found = False
                    break
            if match_found:
                matched = True
                break
    else:  # KV_MATCH_STARTS_WITH
        matched = True
        for idx in range(pat_len):
            if buffer[start + idx] != pattern_bytes[idx]:
                matched = False
                break

    if not matched:
        return True

    start_byte, end_byte = nb_find_word_span(buffer, start, end, cfg.word_index, cfg.word_count)
    if start_byte == -1:
        return True

    target_mod_id = cfg.static_target_id
    if target_mod_id == MODULE_ID_UNKNOWN:
        return True

    return nb_bundle_push_len(
        out_b,
        in_b.timestamps[i],
        in_b.rx_timestamps[i],
        buffer[start_byte:end_byte],
        end_byte - start_byte,
        in_b.levels[i],
        target_mod_id,
        device_id_int,
        0,
        0,
        0,
        0,
    )


@app_njit(inline="always")
def nb_extract_positional_row(in_b, i, cfg, out_b, device_id_int):
    start = in_b.offsets[i]
    end = start + in_b.lengths[i]
    buffer = in_b.buffer

    start_byte, end_byte = nb_find_word_span(buffer, start, end, cfg.word_index, cfg.word_count)
    if start_byte == -1:
        return True

    target_mod_id = cfg.static_target_id
    if target_mod_id == MODULE_ID_UNKNOWN:
        return True

    return nb_bundle_push_len(
        out_b,
        in_b.timestamps[i],
        in_b.rx_timestamps[i],
        buffer[start_byte:end_byte],
        end_byte - start_byte,
        in_b.levels[i],
        target_mod_id,
        device_id_int,
        0,
        0,
        0,
        0,
    )


@app_njit(inline="always")
def nb_extract_json_lite_row(in_b, i, cfg, out_b, device_id_int):
    start = in_b.offsets[i]
    end = start + in_b.lengths[i]
    buffer = in_b.buffer
    msg_len = end - start

    key_bytes = cfg.pattern_bytes
    key_len = len(key_bytes)
    if key_len == 0 or msg_len < key_len:
        return True

    idx = -1
    for match_idx in range(start, end - key_len + 1):
        found = True
        for k in range(key_len):
            if buffer[match_idx + k] != key_bytes[k]:
                found = False
                break
        if found:
            idx = match_idx
            break

    if idx == -1:
        return True

    pos = idx + key_len
    while pos < end and buffer[pos] == 32:
        pos += 1
    if pos >= end:
        return True

    if buffer[pos] == CHAR_DQUOTE:
        pos += 1
        end_pos = -1
        for scan_pos in range(pos, end):
            if buffer[scan_pos] == CHAR_DQUOTE:
                end_pos = scan_pos
                break
        if end_pos == -1:
            return True
    else:
        end_pos = pos
        while end_pos < end:
            c = buffer[end_pos]
            if c == CHAR_COMMA or c == CHAR_RBRACE or c == CHAR_RBRACKET or c == 32 or c == 10 or c == 13:
                break
            end_pos += 1

    if end_pos <= pos:
        return True

    target_mod_id = cfg.static_target_id
    if target_mod_id == MODULE_ID_UNKNOWN:
        return True

    # Matches the original pure-Python rule's quirk of using ts_ns for rx_ts_ns too (behavioral
    # parity with the implementation this replaces, not a considered design choice worth changing
    # as part of a backend swap).
    ts_ns = in_b.timestamps[i]
    return nb_bundle_push_len(
        out_b,
        ts_ns,
        ts_ns,
        buffer[pos:end_pos],
        end_pos - pos,
        in_b.levels[i],
        target_mod_id,
        device_id_int,
        0,
        0,
        0,
        0,
    )


@app_njit(inline="always")
def nb_extract_dsv_row(in_b, i, cfg, out_b, device_id_int):
    start = in_b.offsets[i]
    end = start + in_b.lengths[i]
    buffer = in_b.buffer

    start_bytes = cfg.pattern_bytes
    start_len = len(start_bytes)
    scan_start = start
    if start_len > 0:
        if (end - start) < start_len:
            return True
        for j in range(start_len):
            if buffer[start + j] != start_bytes[j]:
                return True
        scan_start = start + start_len

    prefix_bytes = cfg.prefix_bytes
    prefix_len = len(prefix_bytes)
    if prefix_len > 0 and (end - scan_start) >= prefix_len:
        has_prefix = True
        for j in range(prefix_len):
            if buffer[scan_start + j] != prefix_bytes[j]:
                has_prefix = False
                break
        if has_prefix:
            scan_start += prefix_len

    field_delim_int = cfg.field_delim
    field_target_ids = cfg.field_target_ids
    n_fields = len(field_target_ids)

    ts_ns = in_b.timestamps[i]
    rx_ns = in_b.rx_timestamps[i]
    level = in_b.levels[i]

    field_idx = 0
    field_start = scan_start

    for j in range(scan_start, end + 1):
        is_delim = (j < end and buffer[j] == field_delim_int) or j == end
        if is_delim:
            if field_idx < n_fields:
                target_mod_id = field_target_ids[field_idx]

                if target_mod_id != MODULE_ID_UNKNOWN:
                    chunk_start = field_start
                    chunk_end = j
                    while chunk_start < chunk_end and nb_is_whitespace(buffer[chunk_start]):
                        chunk_start += 1
                    while chunk_end > chunk_start and nb_is_whitespace(buffer[chunk_end - 1]):
                        chunk_end -= 1

                    if chunk_start < chunk_end:
                        ok = nb_bundle_push_len(
                            out_b,
                            ts_ns,
                            rx_ns,
                            buffer[chunk_start:chunk_end],
                            chunk_end - chunk_start,
                            level,
                            target_mod_id,
                            device_id_int,
                            0,
                            0,
                            0,
                            0,
                        )
                        if not ok:
                            return False

            field_idx += 1
            if field_idx >= n_fields:
                break
            field_start = j + 1

    return True


@app_njit(inline="always")
def nb_apply_kv_rule(in_b, i, rule_id, cfg, tracker, string_table, out_b, device_id_int):
    if rule_id == KV_RULE_KEY_VALUE:
        return nb_extract_key_value_row(in_b, i, cfg, tracker, string_table, out_b, device_id_int)
    elif rule_id == KV_RULE_ANCHOR_WORD:
        return nb_extract_anchor_word_row(in_b, i, cfg, out_b, device_id_int)
    elif rule_id == KV_RULE_JSON_LITE:
        return nb_extract_json_lite_row(in_b, i, cfg, out_b, device_id_int)
    elif rule_id == KV_RULE_DSV:
        return nb_extract_dsv_row(in_b, i, cfg, out_b, device_id_int)
    elif rule_id == KV_RULE_POSITIONAL:
        return nb_extract_positional_row(in_b, i, cfg, out_b, device_id_int)
    return True


@app_njit()
def nb_process_kv_batch(
    in_b,
    state,
    rules,
    tracker,
    string_table,
    out_b,
    device_identity_id,
    system_identity_id,
    device_id_int,
):
    """Processes in_b (resuming from state.in_idx[0]) against every rule in `rules` (a flat
    NumbaList of (module_id, rule_id, config) triples - the same shape
    core/types/parsing.py's ParserPipelineBundle uses for the frame-parser pipeline), in ONE call,
    for as many rows as fit in out_b.

    Returns True if out_b filled up before the whole input batch was consumed (state.in_idx left
    at the first not-yet-processed row, so the caller can flush out_b, acquire a fresh one, and
    call this again to resume exactly there - the same resumable-chunk contract
    ops/dispatch.py's nb_process_batch_kernel uses). Returns False once every row has been
    handled (state.in_idx reset to 0, ready for the next input batch)."""
    n = in_b.size[0]
    i = state.in_idx[0]

    capacity = out_b.capacity
    buf_limit = int(len(out_b.buffer) * 0.9)
    n_rules = len(rules)

    while i < n:
        # Bail *before* starting a new row if out_b is already at/near capacity - guarantees a
        # row resumed after a flush is never partially re-emitted (nothing from row i has been
        # written yet at this point).
        if out_b.size[0] >= capacity or out_b.msg_cursor[0] >= buf_limit:
            state.in_idx[0] = i
            return True

        device_id = in_b.devices[i]
        if device_id != device_identity_id and device_id != system_identity_id:
            module_id = in_b.modules[i]

            for r in range(n_rules):
                rule = rules[r]
                if rule[0] == module_id:
                    ok = nb_apply_kv_rule(in_b, i, rule[1], rule[2], tracker, string_table, out_b, device_id_int)
                    if not ok:
                        state.in_idx[0] = i
                        return True

        i += 1

    state.in_idx[0] = 0
    return False
