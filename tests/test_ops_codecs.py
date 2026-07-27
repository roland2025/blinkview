# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.frames import FrameConfig
from blinkview.core.types.parsing import STATE_COMPLETE
from blinkview.ops.codecs import (
    nb_decode_cobs_frame,
    nb_decode_newline_frame,
    nb_decode_newline_frame_no_filters,
    nb_decode_slip_frame,
    nb_parser_noop,
    nb_process_byte_filters,
    nb_shift_frame_buffer,
)


def _buf(*byte_values):
    return np.array(byte_values, dtype=dtypes.BYTE)


def _text_buf(msg):
    return np.frombuffer(msg.encode("ascii"), dtype=dtypes.BYTE).copy()


def _cfg(filter_ansi=False, filter_printable=False, filter_trim_r=False):
    return FrameConfig(
        decode_id=0,
        delimiter=ord("\n"),
        length_fixed=False,
        length_min=1,
        length_max=4096,
        length=0,
        filter_printable=filter_printable,
        filter_ansi=filter_ansi,
        filter_trim_r=filter_trim_r,
        report_error=False,
    )


class TestProcessByteFilters:
    def test_no_filters_always_writes(self):
        should_write, state = nb_process_byte_filters(ord("a"), 0, False, False)
        assert should_write is True
        assert state == 0

    def test_printable_filter_drops_control_bytes(self):
        should_write, _ = nb_process_byte_filters(1, 0, False, True)
        assert should_write is False

    def test_printable_filter_keeps_printable_bytes(self):
        should_write, _ = nb_process_byte_filters(ord("A"), 0, False, True)
        assert should_write is True

    def test_ansi_filter_swallows_escape_sequence(self):
        # ESC -> '[' -> a terminating byte in [64,126] ends the sequence
        write1, state1 = nb_process_byte_filters(27, 0, True, False)
        write2, state2 = nb_process_byte_filters(ord("["), state1, True, False)
        write3, state3 = nb_process_byte_filters(ord("m"), state2, True, False)

        assert (write1, write2, write3) == (False, False, False)
        assert state3 == 0

    def test_ansi_filter_orphan_esc_falls_back_to_normal_evaluation(self):
        write1, state1 = nb_process_byte_filters(27, 0, True, False)
        # Not '[' - orphan ESC dropped, current byte evaluated normally
        write2, state2 = nb_process_byte_filters(ord("x"), state1, True, False)

        assert write1 is False
        assert write2 is True
        assert state2 == 0


class TestDecodeNewlineFrame:
    def test_copies_bytes_up_to_but_excluding_the_newline(self):
        msg = "hello\n"
        out_buf = np.zeros(32, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_newline_frame(_text_buf(msg), 0, len(msg), out_buf, 0, _cfg(), 0)

        assert status == STATE_COMPLETE
        assert consumed == len(msg)
        assert bytes(out_buf[:cursor]) == b"hello"

    def test_strips_trailing_carriage_return_when_configured(self):
        msg = "hello\r\n"
        out_buf = np.zeros(32, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_newline_frame(
            _text_buf(msg), 0, len(msg), out_buf, 0, _cfg(filter_trim_r=True), 0
        )

        assert bytes(out_buf[:cursor]) == b"hello"

    def test_keeps_carriage_return_when_not_configured(self):
        msg = "hello\r\n"
        out_buf = np.zeros(32, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_newline_frame(
            _text_buf(msg), 0, len(msg), out_buf, 0, _cfg(filter_trim_r=False), 0
        )

        assert bytes(out_buf[:cursor]) == b"hello\r"

    def test_filter_printable_drops_non_printable_bytes(self):
        msg = "he\x01llo\n"
        out_buf = np.zeros(32, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_newline_frame(
            _text_buf(msg), 0, len(msg), out_buf, 0, _cfg(filter_printable=True), 0
        )

        assert bytes(out_buf[:cursor]) == b"hello"

    def test_respects_output_buffer_capacity(self):
        msg = "hello\n"
        out_buf = np.zeros(3, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_newline_frame(_text_buf(msg), 0, len(msg), out_buf, 0, _cfg(), 0)

        assert cursor == 3
        assert bytes(out_buf[:cursor]) == b"hel"


class TestDecodeNewlineFrameNoFilters:
    def test_vectorized_copy_appends_at_out_cursor(self):
        msg = "abc"
        out_buf = np.zeros(10, dtype=dtypes.BYTE)
        out_buf[0] = ord("X")

        new_cursor = nb_decode_newline_frame_no_filters(_text_buf(msg), 0, len(msg), out_buf, 1, _cfg())

        assert new_cursor == 4
        assert bytes(out_buf[:4]) == b"Xabc"

    def test_respects_output_buffer_capacity(self):
        msg = "abcdef"
        out_buf = np.zeros(3, dtype=dtypes.BYTE)

        new_cursor = nb_decode_newline_frame_no_filters(_text_buf(msg), 0, len(msg), out_buf, 0, _cfg())

        assert new_cursor == 3
        assert bytes(out_buf[:3]) == b"abc"


class TestDecodeCobsFrame:
    def test_too_short_input_returns_cursor_unchanged(self):
        out_buf = np.zeros(16, dtype=dtypes.BYTE)
        cursor = nb_decode_cobs_frame(_buf(1), 0, 1, out_buf, 0, _cfg())
        assert cursor == 0

    def test_decodes_a_simple_cobs_frame(self):
        # COBS-encode b"ab" (no zero bytes): [code=3, 'a', 'b'] with no trailing implicit zero,
        # since read_idx reaches 'end' exactly after the data bytes (code < 0xFF's zero-insertion
        # branch requires read_idx < end).
        f_buf = _buf(3, ord("a"), ord("b"))
        out_buf = np.zeros(16, dtype=dtypes.BYTE)

        cursor = nb_decode_cobs_frame(f_buf, 0, len(f_buf), out_buf, 0, _cfg())

        assert bytes(out_buf[:cursor]) == b"ab"

    def test_stops_at_a_zero_code_byte(self):
        f_buf = _buf(3, ord("a"), ord("b"), 0, ord("c"))
        out_buf = np.zeros(16, dtype=dtypes.BYTE)

        cursor = nb_decode_cobs_frame(f_buf, 0, len(f_buf), out_buf, 0, _cfg())

        assert bytes(out_buf[:cursor]) == b"ab\x00"


class TestDecodeSlipFrame:
    def test_passes_through_unescaped_bytes(self):
        f_buf = _buf(ord("a"), ord("b"))
        out_buf = np.zeros(16, dtype=dtypes.BYTE)

        cursor = nb_decode_slip_frame(f_buf, 0, len(f_buf), out_buf, 0, _cfg())

        assert bytes(out_buf[:cursor]) == b"ab"

    def test_unescapes_end_sequence(self):
        f_buf = _buf(0xDB, 0xDC)
        out_buf = np.zeros(16, dtype=dtypes.BYTE)

        cursor = nb_decode_slip_frame(f_buf, 0, len(f_buf), out_buf, 0, _cfg())

        assert list(out_buf[:cursor]) == [0xC0]

    def test_unescapes_esc_sequence(self):
        f_buf = _buf(0xDB, 0xDD)
        out_buf = np.zeros(16, dtype=dtypes.BYTE)

        cursor = nb_decode_slip_frame(f_buf, 0, len(f_buf), out_buf, 0, _cfg())

        assert list(out_buf[:cursor]) == [0xDB]

    def test_unknown_escape_passes_through_the_literal_next_byte(self):
        f_buf = _buf(0xDB, 0x42)
        out_buf = np.zeros(16, dtype=dtypes.BYTE)

        cursor = nb_decode_slip_frame(f_buf, 0, len(f_buf), out_buf, 0, _cfg())

        assert list(out_buf[:cursor]) == [0x42]


class TestParserNoop:
    def test_returns_start_cursor_unchanged(self):
        assert nb_parser_noop(_buf(1, 2, 3), 5, 10, None, 0, None) == 5


class TestShiftFrameBuffer:
    def test_shifts_residual_bytes_to_the_front(self):
        f_buf = _buf(ord("a"), ord("b"), ord("c"), ord("d"), ord("e"))
        residue_len = nb_shift_frame_buffer(f_buf, 2, 5)

        assert residue_len == 3
        assert bytes(f_buf[:3]) == b"cde"

    def test_no_residue_when_read_ptr_equals_write_ptr(self):
        f_buf = _buf(ord("a"), ord("b"))
        residue_len = nb_shift_frame_buffer(f_buf, 2, 2)
        assert residue_len == 0

    def test_read_ptr_zero_returns_write_ptr_unchanged(self):
        f_buf = _buf(ord("a"), ord("b"), ord("c"))
        residue_len = nb_shift_frame_buffer(f_buf, 0, 3)
        assert residue_len == 3
