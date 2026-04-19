# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import CodecID
from blinkview.ops.codecs import decode_cobs_frame, decode_newline_frame, decode_slip_frame

_ID_NONE = CodecID.NONE
_ID_NEWLINE = CodecID.NEWLINE
_ID_COBS = CodecID.COBS
_ID_SLIP = CodecID.SLIP
_ID_PLUGIN = CodecID.PLUGIN


@app_njit(inline="always")
def dispatch_frame_decoder(target_buf, target_start, target_end, out_buf, out_cursor, f_cfg):
    d_id = f_cfg.decode_id

    # Use the extracted local constants instead of CodecID.NEWLINE
    if d_id == _ID_NEWLINE:
        return decode_newline_frame(target_buf, target_start, target_end, out_buf, out_cursor, f_cfg)

    elif d_id == _ID_COBS:
        return decode_cobs_frame(target_buf, target_start, target_end, out_buf, out_cursor, f_cfg)

    elif d_id == _ID_SLIP:
        return decode_slip_frame(target_buf, target_start, target_end, out_buf, out_cursor, f_cfg)
    #
    # elif d_id == _ID_PLUGIN:
    #     plugin_func = f_cfg.decode_func
    #     return plugin_func(target_buf, target_start, target_end, out_buf, out_cursor, f_cfg)

    return target_end + 1
