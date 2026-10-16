# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Debug access to a J-Link RTT reader's target through the probe session the reader already
holds, so an agent need not start a second one (J-Link Commander) next to it.

Every operation takes the reader's probe lock (JLinkRTTReader.probe()), the one its RTT read loop
holds for each DLL call, and fails with TargetDebugError, whose message is meant for the agent."""

import struct
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from blinkview.io.rtt import JLinkRTTReader

# Said next to the down buffers when the DLL left every one of them empty while it did describe
# an up buffer: seen on the test board, where the control block in target memory named and sized them.
DOWN_BUFFERS_UNREAD = (
    "the J-Link DLL described no down buffer; an empty one may be unused or not read by the DLL. "
    "Their names and sizes are in the RTT control block in target memory (read_memory)."
)

# The most one read_memory call returns.
MAX_READ_BYTES = 4096
# A read takes the probe lock once per chunk, so RTT reads run in between and the target's
# up-buffer does not overflow during a long one.
READ_CHUNK_BYTES = 1024
# Access width in bits -> struct format of one unit, little-endian like Cortex-M memory.
_UNIT_FORMATS = {8: "B", 16: "H", 32: "I"}
_ADDRESS_END = 1 << 32


class TargetDebugError(Exception):
    """A debug operation that could not be carried out; the message says why."""


class JLinkDebug:
    """The debug operations of one JLinkRTTReader, as `reader.debug`."""

    # The agent API tools this provides; listed per source by the `info` tool.
    TOOLS = ("read_memory", "probe_status")

    def __init__(self, reader: "JLinkRTTReader"):
        self._reader = reader

    def read_memory(self, address: int, length: int, width: int = 8) -> bytes:
        """`length` bytes of target memory from `address`, accessed `width` bits at a time, as
        they lie in memory. On Cortex-M this does not halt the core."""
        unit_format = _UNIT_FORMATS.get(width)
        if unit_format is None:
            raise TargetDebugError(f"width must be one of {sorted(_UNIT_FORMATS)}, got {width}")
        unit = width // 8
        if not 0 < length <= MAX_READ_BYTES:
            raise TargetDebugError(f"length must be 1..{MAX_READ_BYTES} bytes, got {length}")
        if not 0 <= address < _ADDRESS_END or address + length > _ADDRESS_END:
            raise TargetDebugError(f"0x{address:x} + {length} is outside the 32-bit address space")
        if address % unit or length % unit:
            raise TargetDebugError(f"a {width}-bit read needs an address and length that are multiples of {unit}")

        data = bytearray()
        for offset in range(0, length, READ_CHUNK_BYTES):
            chunk_address = address + offset
            count = min(READ_CHUNK_BYTES, length - offset) // unit
            with self._reader.probe() as jl:
                units = getattr(jl, f"memory_read{width}")(chunk_address, count)
            if len(units) < count:
                raise TargetDebugError(f"read stopped at 0x{chunk_address + len(units) * unit:08x}")
            data += struct.pack(f"<{count}{unit_format}", *units[:count])
        return bytes(data)

    def probe_status(self) -> dict:
        """The probe, the target and RTT as the open session sees them. A field the DLL could not
        read holds {"error": ...} instead of failing the whole status."""
        reader = self._reader
        with reader.probe() as jl:
            status = {
                "target_device": reader.target_device,
                "interface": reader.interface,
                "serial_number": _field(lambda: jl.serial_number),
                "speed_khz": _field(lambda: jl.speed),
                "target_voltage_mv": _field(lambda: jl.hardware_status.VTarget),  # a property in pylink
                "target_connected": _field(jl.target_connected),
                "core": _field(jl.core_name),
                "halted": _field(jl.halted),
                "rtt": _field(lambda: _rtt_status(jl, reader.channel)),
            }
        return status


def _field(read: Callable):
    try:
        return read()
    except Exception as e:
        return {"error": str(e) or type(e).__name__}


def _rtt_status(jl, channel: int) -> dict:
    status = jl.rtt_get_status()
    up = [_buffer(jl, i, up=True) for i in range(status.NumUpBuffers)]
    down = [_buffer(jl, i, up=False) for i in range(status.NumDownBuffers)]
    rtt = {
        "running": bool(status.IsRunning),
        "channel": channel,
        "bytes_transferred": status.NumBytesTransferred,
        "host_overflows": status.HostOverflowCount,
        "up_buffers": up,
        "down_buffers": down,
    }
    if down and all(b.get("empty") for b in down) and not all(b.get("empty") for b in up):
        rtt["down_buffers_note"] = DOWN_BUFFERS_UNREAD
    return rtt


def _buffer(jl, index: int, up: bool) -> dict:
    desc = jl.rtt_get_buf_descriptor(index, up)
    name = desc.acName.decode(errors="replace")
    if not name and not desc.SizeOfBuffer:
        # An unused slot, or (for down buffers, on the test board) one the DLL did not read: no name or
        # size is reported for either, as they could be wrong.
        return {"index": index, "empty": True}
    return {"index": index, "name": name, "size": desc.SizeOfBuffer}
