# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.frames import FrameConfig
from blinkview.core.types.parsing import STATE_COMPLETE, CodecID
from blinkview.ops.frame_dispatch import nb_dispatch_frame_decoder


def _buf(msg):
    return np.frombuffer(msg.encode("ascii"), dtype=dtypes.BYTE).copy()


def _cfg(decode_id, filter_ansi=False, filter_printable=False, filter_trim_r=False):
    return FrameConfig(
        decode_id=decode_id,
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


class TestDispatchNewline:
    def test_dispatches_to_the_newline_decoder(self):
        msg = "hello\n"
        f_buf = _buf(msg)
        out_buf = np.zeros(32, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_dispatch_frame_decoder(
            f_buf, 0, len(f_buf), out_buf, 0, _cfg(CodecID.NEWLINE), 0
        )

        assert status == STATE_COMPLETE
        assert consumed == len(msg)
        assert bytes(out_buf[:cursor]) == b"hello"


class TestDispatchAdbLong:
    def test_dispatches_to_the_adb_long_decoder(self):
        msg = "[ 1.0] android.wifi I/Tag: hello"
        f_buf = _buf(msg)
        out_buf = np.zeros(128, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_dispatch_frame_decoder(
            f_buf, 0, len(f_buf), out_buf, 0, _cfg(CodecID.ADB_LONG), 0
        )

        # Incomplete without a second header - same as calling nb_decode_adb_long_frame directly.
        assert consumed == 0
        assert status == 1  # STATE_INCOMPLETE


class TestDispatchUnknownCodec:
    def test_unknown_codec_id_falls_through_and_consumes_the_whole_chunk(self):
        msg = "raw unrecognized bytes"
        f_buf = _buf(msg)
        out_buf = np.zeros(32, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_dispatch_frame_decoder(f_buf, 0, len(f_buf), out_buf, 5, _cfg(CodecID.NONE), 0)

        assert status == STATE_COMPLETE
        assert cursor == 5  # out_cursor passed through unchanged
        assert consumed == len(msg)
