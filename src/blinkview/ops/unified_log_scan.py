# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.constants import CHAR_COLON, CHAR_CR, CHAR_LF, CHAR_SPACE
from blinkview.ops.segments import nb_bundle_push_len
from blinkview.ops.timestamps import nb_parse_unified_log_ts_ns

# Fixed width of 'YYYY-MM-DDTHH:MM:SS.uuuuuuZ' (see parsers/unified_log_replay.py's grammar doc).
_TS_TOKEN_LEN = 27


@app_njit(inline="always")
def nb_level_char_to_value(level_char: int) -> int:
    """Maps a single ASCII level char to its LogLevel int value (utils/log_level.py).
    Returns -1 for anything not one of the 7 levels the writer ever emits per-row
    (ALL/OFF are filter-only sentinels, never written as a row's own level)."""
    if level_char == 84:  # 'T'
        return 1
    elif level_char == 68:  # 'D'
        return 2
    elif level_char == 73:  # 'I'
        return 4
    elif level_char == 87:  # 'W'
        return 8
    elif level_char == 69:  # 'E'
        return 16
    elif level_char == 70:  # 'F'
        return 32
    elif level_char == 67:  # 'C'
        return 64
    return -1


@app_njit()
def nb_scan_unified_log_lines(
    buf,
    start_cursor,
    max_rows,
    out_ts_ns,
    out_level,
    out_dev_off,
    out_dev_len,
    out_mod_off,
    out_mod_len,
    out_msg_off,
    out_msg_len,
    out_malformed_off,
    out_malformed_len,
    max_malformed,
):
    """
    Scans `buf` (a mmap'd byte view of a unified log file, see parsers/unified_log_replay.py)
    starting at `start_cursor`, for up to `max_rows` newline-terminated lines matching the fixed
    grammar 'YYYY-MM-DDTHH:MM:SS.uuuuuuZ <LEVEL> <DEVICE> <MODULE>: <MESSAGE>'.

    Well-formed rows write ts_ns/level plus device/module/message (offset, length) spans into
    the row's slot in the out_* arrays. Malformed lines (wrong token shape, wrong timestamp
    width) are not raised as errors here - Numba can't call back into a Python logger - instead
    their (offset, length) span is recorded into out_malformed_off/len (capped at max_malformed)
    so the Python caller can log the same per-line warning UnifiedLogReplay used to emit from its
    regex-based parser. Blank lines are silently skipped, matching the old behavior.

    Returns (rows_found, next_cursor, malformed_found, malformed_overflow) - next_cursor is
    where the next scan call should resume; malformed_overflow counts malformed lines beyond
    max_malformed encountered in this call (for a single rollup log line).
    """
    buf_len = buf.shape[0]
    cursor = start_cursor
    rows_found = 0
    malformed_found = 0
    malformed_overflow = 0

    while cursor < buf_len and rows_found < max_rows:
        line_start = cursor
        line_end = line_start
        while line_end < buf_len and buf[line_end] != CHAR_LF:
            line_end += 1
        next_cursor = line_end + 1 if line_end < buf_len else buf_len

        content_end = line_end
        if content_end > line_start and buf[content_end - 1] == CHAR_CR:
            content_end -= 1

        if content_end <= line_start:
            cursor = next_cursor
            continue

        ok = True
        p = line_start

        # --- token 1: timestamp+Z ---
        ts_start = p
        while p < content_end and buf[p] != CHAR_SPACE:
            p += 1
        if p >= content_end or (p - ts_start) != _TS_TOKEN_LEN:
            ok = False

        # --- token 2: level ---
        lvl_start = 0
        lvl_end = 0
        if ok:
            p += 1
            lvl_start = p
            while p < content_end and buf[p] != CHAR_SPACE:
                p += 1
            if p >= content_end:
                ok = False
            lvl_end = p

        # --- token 3: device ---
        dev_start = 0
        dev_end = 0
        if ok:
            p += 1
            dev_start = p
            while p < content_end and buf[p] != CHAR_SPACE:
                p += 1
            if p >= content_end:
                ok = False
            dev_end = p

        # --- token 4: "module: message" (split on first ': ') ---
        mod_start = 0
        mod_end = 0
        msg_start = 0
        msg_end = 0
        if ok:
            p += 1
            mod_start = p
            colon_idx = -1
            scan = p
            while scan < content_end - 1:
                if buf[scan] == CHAR_COLON and buf[scan + 1] == CHAR_SPACE:
                    colon_idx = scan
                    break
                scan += 1
            if colon_idx == -1 or colon_idx <= mod_start:
                ok = False
            else:
                mod_end = colon_idx
                msg_start = colon_idx + 2
                msg_end = content_end

        if ok:
            out_ts_ns[rows_found] = nb_parse_unified_log_ts_ns(buf, ts_start)

            level_val = 4  # LogLevel.INFO fallback, matches old _LEVEL_BY_CHAR.get(..., INFO)
            if (lvl_end - lvl_start) == 1:
                resolved = nb_level_char_to_value(buf[lvl_start])
                if resolved != -1:
                    level_val = resolved
            out_level[rows_found] = level_val

            out_dev_off[rows_found] = dev_start
            out_dev_len[rows_found] = dev_end - dev_start
            out_mod_off[rows_found] = mod_start
            out_mod_len[rows_found] = mod_end - mod_start
            out_msg_off[rows_found] = msg_start
            out_msg_len[rows_found] = msg_end - msg_start
            rows_found += 1
        else:
            if malformed_found < max_malformed:
                out_malformed_off[malformed_found] = line_start
                out_malformed_len[malformed_found] = content_end - line_start
                malformed_found += 1
            else:
                malformed_overflow += 1

        cursor = next_cursor

    return rows_found, cursor, malformed_found, malformed_overflow


@app_njit()
def nb_push_unified_log_rows(bundle, buf, ts_ns, level, device_id, module_id, msg_off, msg_len, row_count):
    """
    Pushes up to `row_count` already-parsed rows (device_id/module_id resolved by the Python
    caller via id_registry in between the scan and this call - see nb_scan_unified_log_lines)
    directly into `bundle` via nb_bundle_push_len. Message bytes are a zero-copy slice of `buf`
    (the mmap'd source file) - never re-encoded/copied before this point.

    Stops early and returns the count actually pushed if the bundle fills up mid-batch, so the
    caller can distribute() the batch, acquire a fresh one, and resume from the returned index.
    """
    pushed = 0
    while pushed < row_count:
        off = msg_off[pushed]
        length = msg_len[pushed]
        ok = nb_bundle_push_len(
            bundle,
            ts_ns[pushed],
            ts_ns[pushed],
            buf[off : off + length],
            length,
            level[pushed],
            module_id[pushed],
            device_id[pushed],
            0,
            0,
            0,
            0,
            0,
            0,
        )
        if not ok:
            break
        pushed += 1

    return pushed
