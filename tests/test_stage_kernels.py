# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end runs of the section-driven path (decoder.kernel -> section.kernel per pipeline step -> nb_finish_frames)
for every parser family that has a dedicated stage kernel."""

import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.id_registry.registry import IDRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.output import OutputConfig
from blinkview.ops.dispatch import nb_finish_frames
from blinkview.parsers import adb_decoder  # noqa: F401  (registers the adb_long_frame steps)
from blinkview.parsers.binary_parser import BinaryParser
from blinkview.parsers.frame_decoders import FrameDecoderFactory
from blinkview.parsers.frame_parsers import FrameParserFactory, FrameSectionParserFactory
from blinkview.parsers.state import FrameState
from blinkview.utils import level_map  # noqa: F401  (registers the log_level_* steps)

RX_NS = 1_700_000_000_000_000_000


def _shared(id_registry):
    registry = FactoryRegistry()
    registry.register("frame_decoder", FrameDecoderFactory)
    registry.register("frame_parser", FrameParserFactory)
    registry.register("frame_section_parser", FrameSectionParserFactory)
    return SimpleNamespace(
        array_pool=NumpyArrayPool(), time_ns=time.time_ns, factories=registry, id_registry=id_registry
    )


def _rows(decoder, steps, data, pid_tid=False):
    """Runs `data` (one input chunk) through a freshly configured parser; returns the output rows."""
    id_registry = IDRegistry(NumpyArrayPool())
    shared = _shared(id_registry)
    pool = shared.array_pool

    parser = BinaryParser()
    parser.logger = PrintLogger("test.stage_kernels")
    parser.shared = shared
    parser.local = SimpleNamespace(device_id=id_registry.get_device("stage_kernels"))
    parser.apply_config(
        {
            "frame_decoder": decoder,
            "frame_parser": {"type": "default", "steps": steps},
            "delay": 20,
        }
    )
    codec = parser._frame_codec
    frame_parser = parser._frame_parser
    p_config = frame_parser.bundle()

    frame_state = FrameState(pool, codec.frame_length_maximum)
    f_state = frame_state.bundle
    o_config = OutputConfig(compact_buffer=True)

    in_batch = pool.create(PooledLogBatch, 1, len(data))
    assert in_batch.insert(RX_NS, RX_NS, data)
    out = pool.create(
        PooledLogBatch, 64, 4096, has_levels=True, has_modules=True, has_devices=True, has_pids=True, has_tids=True
    )

    f_state.in_frame[0] = True
    f_state.offset[0] = 0
    frame_state.reset_batch_trackers()
    out_full, n = codec.kernel(f_state, in_batch.bundle, p_config, o_config, out.bundle)
    if n:
        for section in frame_parser.pipeline:
            section.kernel(out.bundle, f_state, n)
        nb_finish_frames(
            f_state.fstart,
            f_state.fcur,
            f_state.fend,
            f_state.ftotal,
            f_state.fstatus,
            p_config,
            o_config.compact_buffer,
            out.bundle,
            n,
        )
    assert not out_full
    frame_parser.post_process(out)

    b = out.bundle
    rows = []
    for i in range(out.size):
        off, ln = int(b.offsets[i]), int(b.lengths[i])
        rows.append(
            (
                bytes(b.buffer[off : off + ln]),
                int(b.levels[i]),
                int(b.modules[i]),
                int(b.timestamps[i]),
                # the pid/tid columns are uninitialised pool memory unless a step writes them
                (int(b.pids[i]), int(b.tids[i])) if pid_tid else None,
            )
        )
    return rows


LINE = {"type": "line_decoder", "frame_delimiter": 10}
ADB = {"type": "decode_adb_long_frame", "frame_delimiter": 10}

CASES = {
    "python_logging": (
        LINE,
        [
            {"type": "log_level_python"},
            {"type": "timestamp_integer", "unix_timestamp": True, "precision": 0},
            {"type": "module_name_normalizer", "max_depth": 4, "max_length": 64},
        ],
        b"INFO 1700000123 svc.mod: hello\nERROR 1700000124 other.mod: bad thing\n",
        [b"hello", b"bad thing"],
    ),
    "idf_timestamp": (
        LINE,
        [{"type": "timestamp_idf_v1", "precision": 1}, {"type": "log_level_idf"}],
        b"(1234) I hello\n(1300) E broken\n",
        [b"hello", b"broken"],
    ),
    "fixed_width_module": (
        LINE,
        [{"type": "log_level_default"}, {"type": "module_name_fixed_width", "max_length": 8}],
        b"I main    started\nW net      link down\n",
        [b"started", b"link down"],
    ),
    "rsyslog": (
        LINE,
        [
            {"type": "timestamp_rfc3164", "year": 2026},
            {"type": "skip_words", "count": 1},
            {"type": "module_name_rsyslog", "max_length": 32},
        ],
        b"Jan  2 15:04:05 myhost sshd[123]: accepted\nJan  2 15:04:06 myhost kernel: oops\n",
        [b"accepted", b"oops"],
    ),
    "iso8601_desktop": (
        LINE,
        [
            {"type": "timestamp_iso8601_desktop"},
            {"type": "log_level_python"},
            {"type": "module_name_normalizer", "max_depth": 4, "max_length": 64},
        ],
        b"2026-01-15 10:23:01,456 INFO app.core: started\n2026-01-15 10:23:02,000 DEBUG app.io: read\n",
        [b"started", b"read"],
    ),
    "rfc3339": (
        LINE,
        [
            {"type": "timestamp_rfc3339"},
            {"type": "log_level_python"},
            {"type": "module_name_normalizer", "max_depth": 4, "max_length": 64},
        ],
        b"2026-01-15T10:23:01.456789Z INFO app.core: started\n2026-01-15T10:23:02.000000+02:00 WARNING app.io: slow\n",
        [b"started", b"slow"],
    ),
    "zephyr_uptime": (
        LINE,
        [{"type": "timestamp_zephyr_uptime_formatted"}, {"type": "log_level_zephyr"}],
        b"[00:00:01.234,000] <inf> main: booted\n[00:00:02.000,000] <err> net: failed\n",
        [b"main: booted", b"net: failed"],
    ),
    "zephyr_realtime": (
        LINE,
        [{"type": "timestamp_zephyr_realtime"}, {"type": "log_level_zephyr"}],
        b"[2026-01-15 10:23:01.456,000] <inf> main: booted\n[2026-01-15 10:23:02.000,000] <err> net: failed\n",
        [b"main: booted", b"net: failed"],
    ),
    "adb_long": (
        ADB,
        [
            {"type": "timestamp_adb_long_frame"},
            {"type": "process_pid_tid_adb_long_frame"},
            {"type": "log_level_adb_long_frame"},
            {"type": "module_name_adb_long_frame"},
        ],
        b"[ 12.345  1234: 5678 I/Tag ]\nfirst message\n\n[ 13.000  1234: 5678 E/Other ]\nsecond message\n\n[ 14.000  1 :2 I/X ]\n",
        [b"first message", b"second message"],
    ),
}


@pytest.mark.parametrize("name", list(CASES))
def test_section_pipeline_emits_expected_payloads(name):
    decoder, steps, data, expected_payloads = CASES[name]

    pid_tid = any(step["type"] == "process_pid_tid_adb_long_frame" for step in steps)
    rows = _rows(decoder, steps, data, pid_tid=pid_tid)

    assert [row[0] for row in rows] == expected_payloads


def test_section_pipeline_actually_parses_the_fields():
    """Guards against payloads matching while every other column is left at its default: spot-check decoded
    columns for the ADB long frame."""
    decoder, steps, data, _ = CASES["adb_long"]
    rows = _rows(decoder, steps, data, pid_tid=True)

    assert rows[0][4] == (1234, 5678)  # pid / tid
    assert rows[0][3] != 0  # timestamp
    assert rows[0][1] != rows[1][1]  # I vs E level differ
