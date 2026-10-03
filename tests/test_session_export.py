# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import os
import re
import subprocess
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import zstandard

from blinkview.storage.log_file_archive import compress_log_part_file
from blinkview.utils import session_export
from blinkview.utils.session_export import (
    STATS_LOGGER_NAMES,
    ExportFilter,
    FilterError,
    SessionRead,
    iter_session_lines,
    keep_line,
    make_filter,
    run_export,
    setup_export_parser,
)

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "blinkview"


def _line(i: int, device: str = "nrf", module: str = "app.main") -> bytes:
    return b"2026-10-01T12:00:%02d.%06dZ I %s %s: message %d" % (i % 60, i, device.encode(), module.encode(), i)


def _lines(start: int, count: int) -> list[bytes]:
    return [_line(i) for i in range(start, start + count)]


def _write_plain(folder: Path, index: int, lines: list[bytes], terminated: bool = True) -> Path:
    path = folder / f"session.{index:04d}.log"
    data = b"\n".join(lines) + (b"\n" if terminated and lines else b"")
    path.write_bytes(data)
    return path


def _write_zst(folder: Path, index: int, lines: list[bytes]) -> Path:
    plain = _write_plain(folder, index, lines)
    archive = compress_log_part_file(plain)
    plain.unlink()
    return archive


def _read(parts, check_gaps=True):
    report = SessionRead()
    lines = list(iter_session_lines(parts, report, check_gaps=check_gaps))
    return lines, report


class TestReadsParts:
    def test_plain_part(self, tmp_path):
        expected = _lines(0, 50)
        lines, report = _read([_write_plain(tmp_path, 0, expected)])

        assert lines == expected
        assert report.warning_lines() == []
        assert not report.parts[0].compressed

    def test_compressed_part(self, tmp_path):
        expected = _lines(0, 50)
        lines, report = _read([_write_zst(tmp_path, 0, expected)])

        assert lines == expected
        assert report.warning_lines() == []
        assert report.parts[0].compressed

    def test_mixed_parts_in_listed_order(self, tmp_path):
        """A live session: earlier parts rotated away and compressed, the current one plain."""
        parts = [
            _write_zst(tmp_path, 0, _lines(0, 10)),
            _write_zst(tmp_path, 1, _lines(10, 10)),
            _write_plain(tmp_path, 2, _lines(20, 10)),
        ]

        lines, report = _read(parts)

        assert lines == _lines(0, 30)
        assert [p.compressed for p in report.parts] == [True, True, False]
        assert report.warning_lines() == []

    def test_empty_part_yields_nothing_without_warning(self, tmp_path):
        lines, report = _read([_write_plain(tmp_path, 0, [])])

        assert lines == []
        assert report.warning_lines() == []

    def test_empty_lines_are_kept(self, tmp_path):
        expected = [_line(0), b"", _line(1)]
        lines, _ = _read([_write_plain(tmp_path, 0, expected)])

        assert lines == expected

    def test_invalid_utf8_passes_through_unchanged(self, tmp_path):
        expected = [_line(0) + b" \xff\xfe\xc3", _line(1)]
        lines, _ = _read([_write_zst(tmp_path, 0, expected)])

        assert lines == expected


class TestChunkBoundaries:
    @pytest.mark.parametrize("chunk", [1, 7, 64, 4096])
    def test_lines_crossing_plain_chunk_boundaries(self, tmp_path, chunk):
        expected = _lines(0, 40)
        with patch.object(session_export, "PLAIN_CHUNK_BYTES", chunk):
            lines, _ = _read([_write_plain(tmp_path, 0, expected)])

        assert lines == expected

    @pytest.mark.parametrize("chunk", [1, 13, 256])
    def test_lines_crossing_compressed_input_chunk_boundaries(self, tmp_path, chunk):
        expected = _lines(0, 400)
        part = _write_zst(tmp_path, 0, expected)
        with patch.object(session_export, "ZST_INPUT_CHUNK_BYTES", chunk):
            lines, report = _read([part])

        assert lines == expected
        assert report.warning_lines() == []


class TestPartialLastLine:
    def test_unterminated_last_line_is_yielded_and_flagged(self, tmp_path):
        expected = _lines(0, 5)
        lines, report = _read([_write_plain(tmp_path, 0, expected, terminated=False)])

        assert lines == expected
        assert report.parts[0].ends_partial

    def test_terminated_part_is_not_flagged(self, tmp_path):
        _, report = _read([_write_plain(tmp_path, 0, _lines(0, 5))])

        assert not report.parts[0].ends_partial

    def test_plain_part_is_read_only_up_to_its_size_at_open(self, tmp_path):
        """A live part keeps growing while it's read - bytes appended after open are not chased,
        and a line cut at that point is a partial last line."""
        part = _write_plain(tmp_path, 0, _lines(0, 3))
        real_open = open

        class GrowsOnFirstRead:
            """FileLogger appending while the export is reading."""

            def __init__(self, f):
                self._f = f
                self._grown = False

            def read(self, n):
                chunk = self._f.read(n)
                if not self._grown:
                    self._grown = True
                    with real_open(part, "ab") as writer:
                        writer.write(_line(3) + b"\n")
                return chunk

            def __getattr__(self, name):
                return getattr(self._f, name)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._f.close()

        def open_growing(path, mode="r", *args, **kwargs):
            return GrowsOnFirstRead(real_open(path, mode, *args, **kwargs))

        with patch.object(session_export, "PLAIN_CHUNK_BYTES", 16):
            with patch("blinkview.utils.session_export.open", open_growing, create=True):
                lines, report = _read([part])

        assert part.read_bytes().endswith(_line(3) + b"\n")  # it did grow
        assert lines == _lines(0, 3)
        assert not report.parts[0].ends_partial


class TestBrokenArchives:
    def test_truncated_archive_keeps_lines_up_to_the_break_and_reads_on(self, tmp_path):
        first = _lines(0, 20_000)
        archive = _write_zst(tmp_path, 0, first)
        data = archive.read_bytes()
        archive.write_bytes(data[: len(data) // 2])
        following = _write_plain(tmp_path, 1, _lines(20_000, 5))

        lines, report = _read([archive, following])

        kept_from_first = lines[: lines.index(_line(20_000))]
        assert 0 < len(kept_from_first) < len(first)
        # Every complete line kept is exact; only the very last one can be cut mid-line.
        assert kept_from_first[:-1] == first[: len(kept_from_first) - 1]
        assert report.parts[0].ends_partial
        assert "truncated" in report.parts[0].warning
        assert lines[-5:] == _lines(20_000, 5)
        assert report.parts[1].warning is None

    def test_corrupt_archive_warns_and_reads_on(self, tmp_path):
        """Corrupts the first block header, which zstd always detects. compress_file writes no
        content checksum, so corrupt bytes inside a block's literals decode silently into wrong
        text - no reader can catch that."""
        archive = _write_zst(tmp_path, 0, _lines(0, 20_000))
        data = bytearray(archive.read_bytes())
        data[20:84] = b"\xff" * 64
        archive.write_bytes(bytes(data))
        following = _write_plain(tmp_path, 1, _lines(20_000, 5))

        lines, report = _read([archive, following])

        assert "corrupt" in report.parts[0].warning
        assert lines[-5:] == _lines(20_000, 5)

    def test_zero_byte_archive_is_reported_as_truncated(self, tmp_path):
        archive = tmp_path / "session.0000.log.zst"
        archive.write_bytes(b"")

        lines, report = _read([archive])

        assert lines == []
        assert "truncated" in report.parts[0].warning

    def test_data_after_the_frame_is_ignored_with_a_warning(self, tmp_path):
        expected = _lines(0, 10)
        archive = _write_zst(tmp_path, 0, expected)
        archive.write_bytes(archive.read_bytes() + zstandard.ZstdCompressor().compress(b"second frame\n"))

        lines, report = _read([archive])

        assert lines == expected
        assert "after the end of the zstd frame" in report.parts[0].warning

    def test_data_after_the_frame_starting_on_a_chunk_boundary_is_detected(self, tmp_path):
        archive = _write_zst(tmp_path, 0, _lines(0, 10))
        frame_size = archive.stat().st_size
        archive.write_bytes(archive.read_bytes() + b"junk")

        with patch.object(session_export, "ZST_INPUT_CHUNK_BYTES", frame_size):
            _, report = _read([archive])

        assert "after the end of the zstd frame" in report.parts[0].warning


class TestVanishedParts:
    def test_plain_part_compressed_away_after_listing_is_read_from_its_archive(self, tmp_path):
        listed = tmp_path / "session.0000.log"
        archive = _write_zst(tmp_path, 0, _lines(0, 10))
        assert not listed.exists()

        lines, report = _read([listed])

        assert lines == _lines(0, 10)
        assert report.parts[0].path == archive
        assert report.parts[0].compressed
        assert report.warning_lines() == []

    def test_plain_part_unlinked_between_resolve_and_open_falls_back_to_its_archive(self, tmp_path):
        listed = _write_plain(tmp_path, 0, _lines(0, 10))
        archive = compress_log_part_file(listed)
        real_open = open

        def unlink_then_open(path, mode="r", *args, **kwargs):
            if Path(path) == listed and listed.exists():
                listed.unlink()  # FileLogger's unlink lands right between existing_part() and open()
            return real_open(path, mode, *args, **kwargs)

        with patch("blinkview.utils.session_export.open", unlink_then_open, create=True):
            lines, report = _read([listed])

        assert lines == _lines(0, 10)
        assert report.parts[0].path == archive

    def test_part_gone_entirely_warns_and_reads_on(self, tmp_path):
        gone = tmp_path / "session.0000.log"
        following = _write_plain(tmp_path, 1, _lines(10, 3))

        lines, report = _read([gone, following])

        assert lines == _lines(10, 3)
        assert report.parts[0].path is None
        assert report.warning_lines() == ["session.0000.log: vanished before it could be read, skipped"]


class TestGaps:
    def test_hole_in_numbering_warns(self, tmp_path):
        parts = [_write_plain(tmp_path, i, _lines(i, 1)) for i in (0, 1, 3, 4, 7)]

        _, report = _read(parts)

        assert report.warnings == ["missing parts 0002, 0005-0006"]

    def test_first_index_not_zero_warns(self, tmp_path):
        parts = [_write_plain(tmp_path, i, _lines(i, 1)) for i in (3, 4)]

        _, report = _read(parts)

        assert report.warnings == ["missing parts 0000-0002"]

    def test_gap_check_uses_listed_names_including_compressed_ones(self, tmp_path):
        parts = [_write_zst(tmp_path, 0, _lines(0, 1)), _write_plain(tmp_path, 1, _lines(1, 1))]

        _, report = _read(parts)

        assert report.warnings == []

    def test_single_part_file_skips_the_gap_check(self, tmp_path):
        _, report = _read([_write_plain(tmp_path, 5, _lines(0, 1))], check_gaps=False)

        assert report.warnings == []


def _log(device: str, module: str, message: str = "x") -> bytes:
    return f"2026-10-01T12:00:00.000000Z I {device} {module}: {message}".encode()


class TestStatsFilter:
    @pytest.mark.parametrize(
        "module",
        [
            "central.stats",
            "source.nrf_rtt.stats",
            "source.nrf_rtt.tuner",
            "parser.nrf.stats_in",
            "parser.nrf.stats_out",
            "parser.nrf.tuner_out",
            "reorder.stats_out",
            "stats",
        ],
    )
    def test_system_stats_lines_are_dropped(self, module):
        assert not keep_line(_log("system", module, "1234 msg/s"), ExportFilter())

    @pytest.mark.parametrize("module", ["statsd", "foo.stats_extra", "stats.collector", "app.tuner2"])
    def test_system_modules_that_only_resemble_stats_are_kept(self, module):
        assert keep_line(_log("system", module), ExportFilter())

    @pytest.mark.parametrize(
        "message",
        ["send_command: reset", "J-Link connection lost", "Target system has no power"],
    )
    def test_other_system_lines_are_kept(self, message):
        """Host actions that explain firmware behaviour."""
        assert keep_line(_log("system", "source.nrf_rtt", message), ExportFilter())

    def test_a_firmware_module_named_stats_is_kept(self):
        assert keep_line(_log("nrf", "stats"), ExportFilter())
        assert keep_line(_log("nrf", "app.stats"), ExportFilter())

    def test_include_stats_keeps_them(self):
        options = ExportFilter(include_stats=True)
        assert keep_line(_log("system", "central.stats"), options)
        assert keep_line(_log("system", "parser.nrf.tuner_out"), options)

    def test_a_module_token_without_its_colon_still_matches(self):
        assert not keep_line(b"2026-10-01T12:00:00.000000Z I system central.stats x", ExportFilter())

    @pytest.mark.parametrize("line", [b"", b"garbage", b"two fields", b"only three fields", b"\xff\xfe \x00"])
    def test_lines_with_fewer_than_four_fields_are_kept(self, line):
        assert keep_line(line, ExportFilter())

    def test_invalid_utf8_message_is_filtered_without_decoding(self):
        assert keep_line(_log("nrf", "app.main") + b" \xff\xfe", ExportFilter())
        assert not keep_line(_log("system", "central.stats") + b" \xff\xfe", ExportFilter())

    def test_names_match_every_stats_child_call_in_src(self):
        """A new logger.stats_child(name) would otherwise leak its lines into every export."""
        used = set()
        for path in SRC_ROOT.rglob("*.py"):
            used |= set(re.findall(r"stats_child\(\s*[\"']([^\"']+)[\"']", path.read_text(encoding="utf-8")))

        assert used, "found no stats_child calls - has the API been renamed?"
        assert used == set(STATS_LOGGER_NAMES)


class TestDrops:
    def test_module_prefix_drops_on_any_device(self):
        options = make_filter(drop=["app.bms"])
        assert not keep_line(_log("nrf", "app.bms"), options)
        assert not keep_line(_log("iot", "app.bms.cell"), options)
        assert not keep_line(_log("nrf", "app.bmsx"), options)  # a plain prefix, as logprep.py
        assert keep_line(_log("nrf", "app.battery"), options)

    def test_device_and_module_prefix_drops_on_that_device_only(self):
        options = make_filter(drop=["iot battery."])
        assert not keep_line(_log("iot", "battery.level"), options)
        assert keep_line(_log("nrf", "battery.level"), options)
        assert keep_line(_log("iot", "battery"), options)  # "battery" doesn't start with "battery."
        assert keep_line(_log("iotx", "battery.level"), options)

    def test_drop_device_drops_the_whole_device_and_only_it(self):
        options = make_filter(drop_device=["iot"])
        assert not keep_line(_log("iot", "app"), options)
        assert not keep_line(_log("iot", "x.y.z"), options)
        assert keep_line(_log("iot2", "app"), options)
        assert keep_line(_log("nrf", "iot"), options)

    def test_several_rules_combine(self):
        options = make_filter(drop=["app.bms", "iot modem."], drop_device=["can0"])
        assert not keep_line(_log("nrf", "app.bms"), options)
        assert not keep_line(_log("iot", "modem.at"), options)
        assert not keep_line(_log("can0", "frame"), options)
        assert keep_line(_log("nrf", "modem.at"), options)

    def test_drops_never_touch_lines_that_are_not_log_lines(self):
        assert keep_line(b"app.bms", make_filter(drop=["app.bms"]))

    @pytest.mark.parametrize("bad", ["", " app", "app ", "a  b", "a b c"])
    def test_malformed_drop_is_rejected(self, bad):
        """An empty prefix would match - and silently drop - every line."""
        with pytest.raises(FilterError):
            make_filter(drop=[bad])

    @pytest.mark.parametrize("bad", ["", "a b"])
    def test_malformed_drop_device_is_rejected(self, bad):
        with pytest.raises(FilterError):
            make_filter(drop_device=[bad])


def _at(ts: str, device: str = "nrf") -> bytes:
    return f"{ts} I {device} app: x".encode()


class TestTimeBounds:
    def test_since_is_inclusive_and_until_exclusive(self):
        options = make_filter(since="2026-10-01T12:00:00Z", until="2026-10-01T13:00:00Z")
        assert not keep_line(_at("2026-10-01T11:59:59.999999Z"), options)
        assert keep_line(_at("2026-10-01T12:00:00.000000Z"), options)
        assert keep_line(_at("2026-10-01T12:59:59.999999Z"), options)
        assert not keep_line(_at("2026-10-01T13:00:00.000000Z"), options)

    def test_offset_is_converted_to_utc(self):
        options = make_filter(since="2026-10-01T15:00+03:00")
        assert not keep_line(_at("2026-10-01T11:59:59.999999Z"), options)
        assert keep_line(_at("2026-10-01T12:00:00.000000Z"), options)

    def test_naive_time_is_local(self):
        local_noon_utc = datetime(2026, 10, 1, 12, 0).astimezone().astimezone(timezone.utc)
        just_before = (local_noon_utc - timedelta(microseconds=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        at = local_noon_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        options = make_filter(since="2026-10-01T12:00")

        assert not keep_line(_at(just_before), options)
        assert keep_line(_at(at), options)

    def test_date_only_means_midnight(self):
        options = make_filter(until="2026-10-02T00:00Z")
        assert keep_line(_at("2026-10-01T23:59:59.999999Z"), options)
        assert not keep_line(_at("2026-10-02T00:00:00.000000Z"), options)
        local_midnight = datetime(2026, 10, 2).astimezone().astimezone(timezone.utc)
        assert make_filter(since="2026-10-02").since == local_midnight.strftime("%Y-%m-%dT%H:%M:%S.%fZ").encode()

    def test_lines_without_a_timestamp_are_kept(self):
        options = make_filter(since="2026-10-01T12:00Z", until="2026-10-01T13:00Z")
        for line in (
            b"",
            b"continuation of a message",
            b"2026-10-01T09:00",
            b"2026-10-01 09:00:00.000000Z I nrf app: x",
        ):
            assert keep_line(line, options), line

    def test_bounds_and_drops_both_apply(self):
        options = make_filter(drop=["app"], since="2026-10-01T12:00Z")
        assert not keep_line(_at("2026-10-01T12:30:00.000000Z"), options)  # in range, but dropped

    @pytest.mark.parametrize("bad", ["yesterday", "2026-13-01", "12:00"])
    def test_unparseable_time_is_rejected(self, bad):
        with pytest.raises(FilterError):
            make_filter(since=bad)

    def test_empty_window_is_rejected(self):
        with pytest.raises(FilterError, match="not before"):
            make_filter(since="2026-10-01T13:00Z", until="2026-10-01T12:00Z")


def _session(root: Path, name: str, lines: list[bytes], created_at: str = "2026-10-01T12:00:00Z") -> Path:
    folder = root / name
    folder.mkdir(parents=True)
    (folder / "metadata.json").write_text(json.dumps({"session_id": name, "created_at": created_at}))
    _write_plain(folder, 0, lines)
    return folder


def _run(argv: list[str]):
    parser = ArgumentParser(prog="blink export")
    setup_export_parser(parser)
    run_export(parser.parse_args(argv))


SESSION_LINES = [_line(0), _log("system", "central.stats", "5 msg/s"), _line(1), _log("system", "gui.lag", "40 ms")]
KEPT_LINES = [SESSION_LINES[0], SESSION_LINES[2], SESSION_LINES[3]]


class TestCommand:
    def test_session_folder_to_stdout(self, tmp_path, capsysbinary):
        folder = _session(tmp_path, "20261001_120000_a", SESSION_LINES)

        _run([str(folder)])

        assert capsysbinary.readouterr().out == b"\n".join(KEPT_LINES) + b"\n"

    def test_include_stats_keeps_everything(self, tmp_path, capsysbinary):
        folder = _session(tmp_path, "20261001_120000_a", SESSION_LINES)

        _run([str(folder), "--include-stats"])

        assert capsysbinary.readouterr().out == b"\n".join(SESSION_LINES) + b"\n"

    def test_single_part_file_is_named_after_its_folder(self, tmp_path):
        folder = _session(tmp_path, "20261001_120000_a", SESSION_LINES)
        _write_plain(folder, 3, [_line(9)])  # alone, not a gap

        _run([str(folder / "session.0003.log"), "-o", str(tmp_path / "out")])

        assert (tmp_path / "out" / "20261001_120000_a.log").read_bytes() == _line(9) + b"\n"

    def test_several_sessions_to_outdir(self, tmp_path, capsysbinary):
        a = _session(tmp_path, "a", SESSION_LINES)
        b = _session(tmp_path, "b", [_line(5)])
        outdir = tmp_path / "new" / "out"

        _run([str(a), str(b), "-o", str(outdir)])

        assert (outdir / "a.log").read_bytes() == b"\n".join(KEPT_LINES) + b"\n"
        assert (outdir / "b.log").read_bytes() == _line(5) + b"\n"
        captured = capsysbinary.readouterr()
        assert captured.out == b""
        assert b"3 of 4 lines" in captured.err

    def test_partial_last_line_is_terminated_and_bytes_pass_through(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        lines = [_line(0) + b" \xff\xfe", _line(1)]
        _write_plain(folder, 0, lines, terminated=False)

        _run([str(folder)])

        assert capsysbinary.readouterr().out == b"\n".join(lines) + b"\n"

    def test_warnings_go_to_stderr_with_the_session_name(self, tmp_path, capsysbinary):
        folder = _session(tmp_path, "s", [_line(0)])
        _write_plain(folder, 2, [_line(2)])

        _run([str(folder)])

        captured = capsysbinary.readouterr()
        assert captured.out == _line(0) + b"\n" + _line(2) + b"\n"
        assert b"warning: s: missing parts 0001" in captured.err

    def test_mixed_plain_and_compressed_parts(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        _write_zst(folder, 0, [_line(0)])
        _write_plain(folder, 1, [_line(1)])

        _run([str(folder)])

        assert capsysbinary.readouterr().out == _line(0) + b"\n" + _line(1) + b"\n"


class TestSessionLookup:
    @pytest.fixture
    def log_root(self, tmp_path):
        root = tmp_path / "logs"
        _session(root / "proj", "20261001_120000_old_bench", [_line(0)], created_at="2026-10-01T12:00:00Z")
        _session(root / "proj", "20261002_120000_new_astra", [_line(1)], created_at="2026-10-02T12:00:00Z")
        with patch.object(session_export, "resolve_log_root", return_value=(root, "proj")):
            yield root

    def test_by_id(self, log_root, capsysbinary):
        _run(["20261001_120000_old_bench"])

        captured = capsysbinary.readouterr()
        assert captured.out == _line(0) + b"\n"
        assert b"20261001_120000_old_bench: 20261001_120000_old_bench" in captured.err

    def test_partial_name_prints_what_it_resolved_to(self, log_root, capsysbinary):
        _run(["astra"])

        captured = capsysbinary.readouterr()
        assert captured.out == _line(1) + b"\n"
        assert b"astra: 20261002_120000_new_astra" in captured.err

    def test_last(self, log_root, capsysbinary):
        _run(["--last"])

        assert capsysbinary.readouterr().out == _line(1) + b"\n"

    def test_unknown_name_is_an_error(self, log_root):
        with pytest.raises(SystemExit, match="no session folder, part file or recorded session matching 'nope'"):
            _run(["nope"])

    def test_an_existing_path_wins_over_a_name(self, log_root, tmp_path, capsysbinary, monkeypatch):
        """A folder in cwd named like a recorded session is exported, not the recorded one."""
        _session(tmp_path / "cwd", "astra", [_line(7)])
        monkeypatch.chdir(tmp_path / "cwd")

        _run(["astra"])

        assert capsysbinary.readouterr().out == _line(7) + b"\n"


class TestCommandErrors:
    def test_nothing_to_export(self):
        with pytest.raises(SystemExit, match="give a SESSION"):
            _run([])

    def test_last_with_sessions(self, tmp_path):
        with pytest.raises(SystemExit, match="not both"):
            _run(["--last", str(tmp_path)])

    def test_several_sessions_without_outdir(self, tmp_path):
        a = _session(tmp_path, "a", [_line(0)])
        b = _session(tmp_path, "b", [_line(1)])

        with pytest.raises(SystemExit, match="several sessions need -o"):
            _run([str(a), str(b)])

    def test_output_name_clash_writes_nothing(self, tmp_path):
        folder = _session(tmp_path, "s", [_line(0)])
        _write_plain(folder, 1, [_line(1)])
        outdir = tmp_path / "out"

        with pytest.raises(SystemExit, match="same output file: s"):
            _run([str(folder / "session.0000.log"), str(folder / "session.0001.log"), "-o", str(outdir)])

        assert not outdir.exists()

    def test_folder_without_parts(self, tmp_path):
        with pytest.raises(SystemExit, match="no session.NNNN.log"):
            _run([str(tmp_path)])

    def test_bad_filter_value_is_an_error_before_reading(self, tmp_path):
        with pytest.raises(SystemExit, match="--since 'soon'"):
            _run([str(tmp_path / "does-not-matter"), "--since", "soon"])


class TestFilterFlags:
    def test_repeated_drop_flags_and_drop_device(self, tmp_path, capsysbinary):
        lines = [_log("nrf", "app.bms"), _log("nrf", "app.main"), _log("iot", "battery.x"), _log("can0", "f")]
        folder = _session(tmp_path, "s", lines)

        _run([str(folder), "--drop", "app.bms", "--drop", "iot battery.", "--drop-device", "can0"])

        assert capsysbinary.readouterr().out == _log("nrf", "app.main") + b"\n"

    def test_since_and_until(self, tmp_path, capsysbinary):
        lines = [_at(f"2026-10-01T12:00:0{i}.000000Z") for i in range(5)]
        folder = _session(tmp_path, "s", lines)

        _run([str(folder), "--since", "2026-10-01T12:00:01Z", "--until", "2026-10-01T12:00:03Z"])

        assert capsysbinary.readouterr().out == lines[1] + b"\n" + lines[2] + b"\n"


PRESET_LINES = [
    _log("nrf", "app.bms"),
    _log("nrf", "app.main"),
    _log("iot", "battery.level"),
    _log("can0", "frame"),
    _log("nrf", "gui.lag"),
]


def _profile(root: Path, presets) -> Path:
    """A profile folder with a <profile>.export_presets.json; returns the profile JSON's path, as
    metadata.json config.source_file records it (the JSON itself needn't exist)."""
    root.mkdir(parents=True)
    presets_file = root / f"{root.name}.export_presets.json"
    presets_file.write_text(presets if isinstance(presets, str) else json.dumps(presets))
    return root / f"{root.name}.json"


def _session_recorded_with(root: Path, name: str, source_file: Path, lines=PRESET_LINES) -> Path:
    folder = root / name
    folder.mkdir(parents=True)
    (folder / "metadata.json").write_text(json.dumps({"config": {"source_file": str(source_file)}}))
    _write_plain(folder, 0, lines)
    return folder


class TestPresets:
    @pytest.fixture(autouse=True)
    def no_active_profile(self, tmp_path):
        """The fallback profile folder - empty unless a test puts a presets file there."""
        active = tmp_path / "active_profile"
        with patch.object(session_export, "resolve_active_profile_dir", return_value=active):
            yield active

    def test_preset_from_the_recorded_profile(self, tmp_path, capsysbinary):
        profile = _profile(tmp_path / "default", {"analysis": {"drop": ["app.bms", "iot battery."]}})
        folder = _session_recorded_with(tmp_path, "s", profile)

        _run([str(folder), "--preset", "analysis"])

        captured = capsysbinary.readouterr()
        assert captured.out.splitlines() == [_log("nrf", "app.main"), _log("can0", "frame"), _log("nrf", "gui.lag")]
        assert b"preset 'analysis' from" in captured.err

    def test_flags_add_to_the_preset(self, tmp_path, capsysbinary):
        profile = _profile(tmp_path / "default", {"analysis": {"drop": ["app.bms"], "drop_device": ["iot"]}})
        folder = _session_recorded_with(tmp_path, "s", profile)

        _run([str(folder), "--preset", "analysis", "--drop", "gui.lag", "--drop-device", "can0"])

        assert capsysbinary.readouterr().out.splitlines() == [_log("nrf", "app.main")]

    def test_falls_back_to_the_active_profile(self, tmp_path, capsysbinary, no_active_profile):
        """Recorded profile gone (or recorded elsewhere): the active profile's presets apply."""
        _profile(no_active_profile, {"analysis": {"drop_device": ["nrf"]}})
        folder = _session_recorded_with(tmp_path, "s", tmp_path / "gone" / "gone.json")

        _run([str(folder), "--preset", "analysis"])

        assert capsysbinary.readouterr().out.splitlines() == [_log("iot", "battery.level"), _log("can0", "frame")]

    def test_session_without_metadata_uses_the_active_profile(self, tmp_path, capsysbinary, no_active_profile):
        _profile(no_active_profile, {"analysis": {"drop_device": ["nrf", "iot", "can0"]}})
        folder = tmp_path / "s"
        folder.mkdir()
        _write_plain(folder, 0, PRESET_LINES)

        _run([str(folder), "--preset", "analysis"])

        assert capsysbinary.readouterr().out == b""

    def test_recorded_profile_wins_over_the_active_one(self, tmp_path, capsysbinary, no_active_profile):
        _profile(no_active_profile, {"analysis": {"drop_device": ["nrf"]}})
        profile = _profile(tmp_path / "default", {"analysis": {"drop_device": ["iot"]}})
        folder = _session_recorded_with(tmp_path, "s", profile)

        _run([str(folder), "--preset", "analysis"])

        assert _log("nrf", "app.main") in capsysbinary.readouterr().out.splitlines()

    def test_a_preset_missing_from_the_recorded_profile_is_not_looked_up_elsewhere(self, tmp_path, no_active_profile):
        """The first presets file found decides - mixing two profiles' presets would surprise."""
        _profile(no_active_profile, {"analysis": {}})
        profile = _profile(tmp_path / "default", {"other": {}})
        folder = _session_recorded_with(tmp_path, "s", profile)

        with pytest.raises(SystemExit, match=r"not in .*available: other"):
            _run([str(folder), "--preset", "analysis"])

    def test_two_sessions_from_different_profiles_resolve_separately(self, tmp_path):
        a = _session_recorded_with(tmp_path, "a", _profile(tmp_path / "pa", {"analysis": {"drop_device": ["nrf"]}}))
        b = _session_recorded_with(tmp_path, "b", _profile(tmp_path / "pb", {"analysis": {"drop_device": ["iot"]}}))

        _run([str(a), str(b), "--preset", "analysis", "-o", str(tmp_path / "out")])

        assert _log("nrf", "app.main") not in (tmp_path / "out" / "a.log").read_bytes().splitlines()
        assert _log("iot", "battery.level") in (tmp_path / "out" / "a.log").read_bytes().splitlines()
        assert _log("nrf", "app.main") in (tmp_path / "out" / "b.log").read_bytes().splitlines()
        assert _log("iot", "battery.level") not in (tmp_path / "out" / "b.log").read_bytes().splitlines()

    def test_no_presets_file_anywhere_lists_where_it_looked(self, tmp_path):
        folder = _session_recorded_with(tmp_path, "s", tmp_path / "default" / "default.json")

        with pytest.raises(
            SystemExit,
            match=r"no presets file found \(looked for: .*default\.export_presets\.json.*"
            r"active_profile\.export_presets\.json",
        ):
            _run([str(folder), "--preset", "analysis"])

    @pytest.mark.parametrize(
        "presets, message",
        [
            ("{not json", "can't read"),
            ('["a"]', "expected an object"),
            ({"analysis": ["app.bms"]}, "must be an object"),
            ({"analysis": {"drops": ["app.bms"]}}, r"unknown keys \['drops'\]"),
            ({"analysis": {"drop": "app.bms"}}, "drop must be a list of strings"),
            ({"analysis": {"drop_device": [1]}}, "drop_device must be a list of strings"),
            ({"analysis": {"drop": [""]}}, r"preset 'analysis': --drop ''"),
        ],
    )
    def test_broken_presets_file_is_an_error_naming_it(self, tmp_path, presets, message):
        folder = _session_recorded_with(tmp_path, "s", _profile(tmp_path / "default", presets))

        with pytest.raises(SystemExit, match=message) as excinfo:
            _run([str(folder), "--preset", "analysis"])

        if message != "can't read":
            assert "export_presets.json" in str(excinfo.value)

    def test_a_broken_preset_for_a_later_session_writes_nothing(self, tmp_path):
        good = _session_recorded_with(tmp_path, "a", _profile(tmp_path / "pa", {"analysis": {}}))
        bad = _session_recorded_with(tmp_path, "b", _profile(tmp_path / "pb", {"other": {}}))
        outdir = tmp_path / "out"

        with pytest.raises(SystemExit):
            _run([str(good), str(bad), "--preset", "analysis", "-o", str(outdir)])

        assert not outdir.exists()

    def test_description_is_allowed_and_ignored(self, tmp_path, capsysbinary):
        profile = _profile(tmp_path / "default", {"analysis": {"description": "for reading", "drop": ["app"]}})
        folder = _session_recorded_with(tmp_path, "s", profile)

        _run([str(folder), "--preset", "analysis"])

        assert capsysbinary.readouterr().out.splitlines() == [
            _log("iot", "battery.level"),
            _log("can0", "frame"),
            _log("nrf", "gui.lag"),
        ]


def _summary(err: bytes) -> str:
    return err.decode()[err.decode().index("summary:") :]


class TestSummary:
    def test_counts_times_parts_and_tags(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        (folder / "metadata.json").write_text(json.dumps({"environment": {"dev_mode": True}}))
        _write_zst(folder, 0, [_at("2026-10-01T12:00:00.000000Z"), _log("system", "central.stats")])
        _write_plain(folder, 1, [_at("2026-10-01T12:00:01.000000Z"), _at("2026-10-01T12:00:02.000000Z", "iot")])

        _run([str(folder), "--summary"])

        summary = _summary(capsysbinary.readouterr().err)
        assert "summary: s" in summary
        assert "3 kept of 4 read" in summary
        assert "2026-10-01T12:00:00.000000Z .. 2026-10-01T12:00:02.000000Z (UTC)" in summary
        assert "session.0000.log.zst (zst), session.0001.log" in summary
        assert "dev mode  on" in summary
        tag_lines = summary.split("by kept lines)\n")[1].splitlines()
        assert [line.split() for line in tag_lines] == [["2", "nrf", "app"], ["1", "iot", "app"]]

    def test_partial_last_line_with_a_cut_timestamp_is_not_the_last_time(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        path = folder / "session.0000.log"
        path.write_bytes(_at("2026-10-01T12:00:00.000000Z") + b"\n" + b"2026-10-01T12:00:0")

        _run([str(folder), "--summary"])

        summary = _summary(capsysbinary.readouterr().err)
        assert ".. 2026-10-01T12:00:00.000000Z (UTC)" in summary
        assert "(not a log line)" in summary

    @pytest.mark.parametrize(
        "metadata, expected",
        [
            ({"environment": {"dev_mode": False}}, "off"),
            ({"environment": {}}, "not recorded"),
            (None, "not recorded"),
        ],
    )
    def test_dev_mode_states(self, tmp_path, capsysbinary, metadata, expected):
        folder = tmp_path / "s"
        folder.mkdir()
        if metadata is not None:
            (folder / "metadata.json").write_text(json.dumps(metadata))
        _write_plain(folder, 0, [_line(0)])

        _run([str(folder), "--summary"])

        assert f"dev mode  {expected}" in _summary(capsysbinary.readouterr().err)

    def test_part_file_reads_dev_mode_from_its_folder(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        (folder / "metadata.json").write_text(json.dumps({"environment": {"dev_mode": False}}))
        part = _write_plain(folder, 0, [_line(0)])

        _run([str(part), "--summary"])

        assert "dev mode  off" in _summary(capsysbinary.readouterr().err)

    def test_vanished_and_broken_parts_are_marked(self, tmp_path, capsysbinary):
        folder = tmp_path / "s"
        folder.mkdir()
        archive = _write_zst(folder, 0, _lines(0, 2000))
        archive.write_bytes(archive.read_bytes()[:-20])
        _write_plain(folder, 1, [_line(1)])

        with patch.object(
            session_export, "existing_part", side_effect=lambda p: None if p.name.endswith("1.log") else p
        ):
            _run([str(folder), "--summary"])

        summary = _summary(capsysbinary.readouterr().err)
        assert "session.0000.log.zst (zst, warning), session.0001.log (vanished)" in summary

    def test_no_summary_without_the_flag(self, tmp_path, capsysbinary):
        _run([str(_session(tmp_path, "s", [_line(0)]))])

        assert b"summary:" not in capsysbinary.readouterr().err


def _subprocess_env(tmp_path: Path) -> dict:
    env = dict(os.environ)
    env["HOME"] = env["USERPROFILE"] = str(tmp_path / "home")
    env["BLINK_PROJECT_ROOT"] = str(tmp_path / "no_project")
    return env


class TestFreshProcess:
    def test_export_does_not_import_numba_qt_or_the_pipeline(self, tmp_path):
        """Must be a subprocess: in-process, sys.modules holds whatever other tests imported."""
        folder = tmp_path / "s"
        folder.mkdir()
        _write_zst(folder, 0, [_line(0)])
        _write_plain(folder, 1, [_line(1)])
        code = (
            "import sys, atexit\n"
            "atexit.register(lambda: sys.stderr.write('MODULES:' + ','.join(sorted(sys.modules)) + '\\n'))\n"
            f"sys.argv = ['blink', 'export', {str(folder)!r}, '-o', {str(tmp_path / 'out')!r}]\n"
            "from blinkview.__main__ import main\n"
            "main()\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code], env=_subprocess_env(tmp_path), capture_output=True, text=True, timeout=60
        )

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "out" / "s.log").read_bytes() == _line(0) + b"\n" + _line(1) + b"\n"
        modules = result.stderr.split("MODULES:")[1].strip().split(",")
        heavy = [
            m
            for m in modules
            if m.split(".")[0] in ("numba", "llvmlite", "PySide6", "qtpy")
            or m.startswith(("blinkview.parsers", "blinkview.ops", "blinkview.storage", "blinkview.core.id_registry"))
        ]
        assert heavy == []

    def test_piping_into_a_reader_that_stops_early_exits_quietly(self, tmp_path):
        """`blink export X | head`. Enough output to overflow the pipe buffer, so the writer is
        still writing when the reader goes away."""
        folder = tmp_path / "s"
        folder.mkdir()
        _write_plain(folder, 0, [_line(i) for i in range(200_000)])

        proc = subprocess.Popen(
            [sys.executable, "-m", "blinkview", "export", str(folder)],
            env=_subprocess_env(tmp_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout.read(100)
        proc.stdout.close()
        stderr = proc.stderr.read()
        returncode = proc.wait(timeout=60)

        assert b"Traceback" not in stderr, stderr.decode(errors="replace")
        assert returncode == 0
