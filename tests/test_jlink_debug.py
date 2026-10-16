# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""JLinkDebug (io/jlink_debug.py) on a real JLinkRTTReader whose JLink is a fake with target memory."""

import struct
from contextlib import contextmanager
from types import SimpleNamespace

import pylink
import pytest

from blinkview.io.jlink_debug import DOWN_BUFFERS_UNREAD, MAX_READ_BYTES, READ_CHUNK_BYTES, TargetDebugError
from tests.test_rtt_reader import FakeJLink, make_reader, probe_held_by_another_thread  # noqa: F401

RAM = 0x20000000


class MemoryJLink(FakeJLink):
    """A FakeJLink with 8 KB of RAM at 0x20000000, filled with 0, 1, 2, ... (mod 256)."""

    def __init__(self):
        super().__init__()
        self.ram = bytearray(i & 0xFF for i in range(8192))
        self.reads = []
        self.status_raises = {}

    def _read(self, addr, count, unit, fmt):
        self.reads.append((addr, count, unit))
        offset = addr - RAM
        if offset < 0 or offset + count * unit > len(self.ram):
            raise pylink.errors.JLinkReadException(-1)
        return list(struct.unpack(f"<{count}{fmt}", self.ram[offset : offset + count * unit]))

    def memory_read8(self, addr, num_bytes):
        return self._read(addr, num_bytes, 1, "B")

    def memory_read16(self, addr, num_halfwords):
        return self._read(addr, num_halfwords, 2, "H")

    def memory_read32(self, addr, num_words):
        return self._read(addr, num_words, 4, "I")

    def _maybe_raise(self, name):
        if name in self.status_raises:
            raise self.status_raises[name]

    @property
    def serial_number(self):
        return 123456

    @property
    def speed(self):
        return 4000

    @property
    def hardware_status(self):
        self._maybe_raise("hardware_status")
        return SimpleNamespace(VTarget=3300)

    def target_connected(self):
        return True

    def core_name(self):
        return "Cortex-M4"

    def halted(self):
        self._maybe_raise("halted")
        return False

    def rtt_get_status(self):
        return SimpleNamespace(
            IsRunning=1, NumBytesTransferred=1000, HostOverflowCount=2, NumUpBuffers=2, NumDownBuffers=2
        )

    def rtt_get_buf_descriptor(self, index, up):
        if not up and index == 1:
            return SimpleNamespace(acName=b"", SizeOfBuffer=0)  # what the DLL answered on the test board
        name = (b"Terminal", b"Data")[index] if up else b"Terminal"
        return SimpleNamespace(acName=name, SizeOfBuffer=8192 if up else 16)


# The pylink.JLink members JLinkDebug uses, and whether pylink makes each a property.
_JLINK_MEMBERS = {
    "serial_number": True,
    "speed": True,
    "hardware_status": True,
    "target_connected": False,
    "core_name": False,
    "halted": False,
    "rtt_get_status": False,
    "rtt_get_buf_descriptor": False,
    "memory_read8": False,
    "memory_read16": False,
    "memory_read32": False,
}


@pytest.mark.parametrize("name, is_property", _JLINK_MEMBERS.items())
def test_the_fake_matches_pylink_on_property_or_method(name, is_property):
    """A fake that makes a pylink property a method hides `jl.x()` calling it (as happened with
    hardware_status, found only on hardware)."""
    assert isinstance(getattr(pylink.JLink, name), property) is is_property
    assert isinstance(getattr(MemoryJLink, name), property) is is_property


@pytest.fixture
def reader():
    reader = make_reader()
    reader.jlink = MemoryJLink()
    return reader


class TestReadMemory:
    @pytest.mark.parametrize("width", [8, 16, 32])
    def test_returns_the_bytes_as_they_lie_in_memory_at_every_width(self, reader, width):
        assert reader.debug.read_memory(RAM + 4, 8, width) == bytes(range(4, 12))
        assert reader.jlink.reads == [(RAM + 4, 8 // (width // 8), width // 8)]

    def test_a_long_read_takes_the_probe_once_per_chunk(self, reader, monkeypatch):
        events = []
        real_probe = reader.probe

        @contextmanager
        def probe():
            events.append("take")
            with real_probe() as jl:
                yield jl
            events.append("release")

        monkeypatch.setattr(reader, "probe", probe)

        data = reader.debug.read_memory(RAM, MAX_READ_BYTES, 32)

        assert data == bytes(reader.jlink.ram[:MAX_READ_BYTES])
        chunks = MAX_READ_BYTES // READ_CHUNK_BYTES
        assert events == ["take", "release"] * chunks
        assert [addr for addr, _count, _unit in reader.jlink.reads] == [
            RAM + i * READ_CHUNK_BYTES for i in range(chunks)
        ]

    @pytest.mark.parametrize(
        "address, length, width, message",
        [
            (RAM, 4, 64, "width must be one of"),
            (RAM, 0, 8, "length must be 1..4096"),
            (RAM, MAX_READ_BYTES + 1, 8, "length must be 1..4096"),
            (-4, 4, 8, "outside the 32-bit address space"),
            (0xFFFFFFFE, 4, 8, "outside the 32-bit address space"),
            (RAM + 2, 4, 32, "multiples of 4"),
            (RAM, 6, 32, "multiples of 4"),
            (RAM + 1, 2, 16, "multiples of 2"),
        ],
    )
    def test_refuses_bad_arguments_before_touching_the_probe(self, reader, address, length, width, message):
        with pytest.raises(TargetDebugError, match=message):
            reader.debug.read_memory(address, length, width)
        assert reader.jlink.reads == []

    def test_a_short_read_says_where_it_stopped(self, reader):
        reader.jlink.memory_read8 = lambda addr, count: [0] * (count - 3)
        with pytest.raises(TargetDebugError, match=f"read stopped at 0x{RAM + 13:08x}"):
            reader.debug.read_memory(RAM, 16)

    def test_a_dll_error_becomes_a_target_debug_error(self, reader):
        with pytest.raises(TargetDebugError):
            reader.debug.read_memory(0x10000000, 4)

    def test_no_open_session(self, reader):
        reader.jlink = None
        with pytest.raises(TargetDebugError, match="no open J-Link session"):
            reader.debug.read_memory(RAM, 4)

    def test_gives_up_when_the_reader_thread_never_lets_go_of_the_probe(self, probe_held_by_another_thread):  # noqa: F811
        with pytest.raises(TargetDebugError, match="the J-Link is busy"):
            probe_held_by_another_thread.debug.read_memory(RAM, 4)


class TestProbeStatus:
    def test_reports_probe_target_and_rtt(self, reader):
        status = reader.debug.probe_status()

        assert status == {
            "target_device": reader.target_device,
            "interface": "swd",
            "serial_number": 123456,
            "speed_khz": 4000,
            "target_voltage_mv": 3300,
            "target_connected": True,
            "core": "Cortex-M4",
            "halted": False,
            "rtt": {
                "running": True,
                "channel": 0,
                "bytes_transferred": 1000,
                "host_overflows": 2,
                "up_buffers": [
                    {"index": 0, "name": "Terminal", "size": 8192},
                    {"index": 1, "name": "Data", "size": 8192},
                ],
                "down_buffers": [
                    {"index": 0, "name": "Terminal", "size": 16},
                    {"index": 1, "empty": True},
                ],
            },
        }

    def test_down_buffers_the_dll_left_empty_get_a_note(self, reader):
        empty = SimpleNamespace(acName=b"", SizeOfBuffer=0)
        real = reader.jlink.rtt_get_buf_descriptor
        reader.jlink.rtt_get_buf_descriptor = lambda index, up: real(index, up) if up else empty

        rtt = reader.debug.probe_status()["rtt"]

        assert rtt["down_buffers"] == [{"index": 0, "empty": True}, {"index": 1, "empty": True}]
        assert rtt["down_buffers_note"] == DOWN_BUFFERS_UNREAD

    def test_no_note_when_a_down_buffer_was_described_or_no_up_buffer_was(self, reader):
        assert "down_buffers_note" not in reader.debug.probe_status()["rtt"]

        reader.jlink.rtt_get_buf_descriptor = lambda index, up: SimpleNamespace(acName=b"", SizeOfBuffer=0)
        assert "down_buffers_note" not in reader.debug.probe_status()["rtt"]

    def test_a_field_that_fails_holds_its_error_and_the_rest_is_still_read(self, reader):
        reader.jlink.status_raises = {
            "hardware_status": pylink.errors.JLinkException("Error in reading hardware status."),
            "halted": RuntimeError(),
        }

        status = reader.debug.probe_status()

        assert status["target_voltage_mv"] == {"error": "Error in reading hardware status."}
        assert status["halted"] == {"error": "RuntimeError"}
        assert status["core"] == "Cortex-M4"

    def test_no_open_session(self, reader):
        reader.jlink = None
        with pytest.raises(TargetDebugError, match="no open J-Link session"):
            reader.debug.probe_status()
