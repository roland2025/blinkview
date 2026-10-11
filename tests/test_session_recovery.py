# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import os
import platform
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from blinkview.core.zstd_file_compression import decompress_file_to_buffer
from blinkview.utils import session_lister, session_recovery
from blinkview.utils.session_lister import HEARTBEAT_STALE_S, list_sessions, read_session_metadata
from blinkview.utils.session_recovery import LOCK_FILE_NAME, recover_session

FOLDER = "20260314_124429_Examples_Can_Untitled"
LOG_TEXT = b"1 first row\n2 second row\n3 torn la"
CREATED = datetime(2026, 3, 14, 10, 44, 29, tzinfo=timezone.utc)
HEARTBEAT = CREATED + timedelta(hours=2)


@contextmanager
def _recorder(running: bool):
    """Whether the pid in the session's metadata is the recorder, still running."""
    with (
        patch.object(session_lister, "_pid_alive", return_value=running),
        patch.object(session_recovery, "_process_running_since", return_value=running),
    ):
        yield


def _meta(**overrides):
    meta = {
        "session_id": FOLDER,
        "status": "active",
        "created_at": CREATED.isoformat() + "Z",
        "project": {"display_name": "Untitled"},
        "config": {"profile": "Examples_Can"},
        "process": {"pid": 4242, "host": platform.node(), "heartbeat_at": HEARTBEAT.isoformat()},
        "previous_session_id": "20260314_100000_Examples_Can_Untitled",
        "loggers": {
            "session": {"extension": "log", "last_part": 1, "total_bytes": 100},
            "src_502d8046": {"extension": "bin", "last_part": 0, "total_bytes": 4},
        },
    }
    meta.update(overrides)
    return meta


def _set_mtime(path, when: datetime):
    os.utime(path, (when.timestamp(), when.timestamp()))


def _make_session(tmp_path, meta=None, metadata_text=None):
    """A session folder as a killed recorder leaves it: one compressed part, the parts it was
    still writing, a partial cold tier and two unfinished temp files."""
    session_dir = tmp_path / "proj" / FOLDER
    session_dir.mkdir(parents=True)
    if metadata_text is None:
        metadata_text = json.dumps(_meta() if meta is None else meta)
    (session_dir / "metadata.json").write_text(metadata_text, encoding="utf-8")

    (session_dir / "session.0000.log.zst").write_bytes(b"compressed earlier")
    (session_dir / "session.0001.log").write_bytes(LOG_TEXT)
    (session_dir / "src_502d8046.0000.bin").write_bytes(b"\x01\x02\x03\x04")
    (session_dir / "Examples_Can.start.json").write_text("{}")
    (session_dir / ".metadata.json.4242.deadbeef.tmp").write_text("{")
    (session_dir / "session.0000.log.zst.tmp").write_bytes(b"half")
    (session_dir / "cold").mkdir()
    (session_dir / "cold" / "segment_0000000000.blkseg").write_bytes(b"old rows")
    (session_dir / "cold-archive").mkdir()

    for path in session_dir.iterdir():
        _set_mtime(path, HEARTBEAT + timedelta(seconds=2))
    return session_dir


class TestRecoverCrashedSession:
    def test_marks_the_session_crashed_and_finished(self, tmp_path):
        session_dir = _make_session(tmp_path)

        with _recorder(running=False):
            result = recover_session(session_dir)

        assert result.recovered and result.skipped is None and result.errors == ()
        meta = read_session_metadata(session_dir)
        assert meta["status"] == "crashed"
        assert "recovered_at" in meta
        # The last part was written 2 s after the last heartbeat.
        assert meta["duration_seconds"] == 7202.0
        assert session_lister._parse_timestamp(meta["finished_at"]) == HEARTBEAT + timedelta(seconds=2)
        # Everything the recorder wrote is kept, including the rotation chain.
        assert meta["previous_session_id"] == "20260314_100000_Examples_Can_Untitled"
        assert meta["process"]["pid"] == 4242

        (listed,) = list_sessions(tmp_path, "proj")
        assert listed.status == "crashed"
        assert listed.duration_seconds == 7202.0

    def test_removes_partial_cold_storage_and_temp_files(self, tmp_path):
        session_dir = _make_session(tmp_path)

        with _recorder(running=False):
            result = recover_session(session_dir)

        assert result.cold_removed
        assert result.tmp_removed == 2
        assert not (session_dir / "cold").exists()
        assert not (session_dir / "cold-archive").exists()
        assert list(session_dir.glob("*.tmp")) == []
        assert not (session_dir / LOCK_FILE_NAME).exists()

    def test_compresses_the_parts_left_uncompressed(self, tmp_path):
        session_dir = _make_session(tmp_path)

        with _recorder(running=False):
            result = recover_session(session_dir)

        assert result.parts_compressed == 2
        assert not (session_dir / "session.0001.log").exists()
        assert not (session_dir / "src_502d8046.0000.bin").exists()
        assert decompress_file_to_buffer(session_dir / "session.0001.log.zst").tobytes() == LOG_TEXT
        assert decompress_file_to_buffer(session_dir / "src_502d8046.0000.bin.zst").tobytes() == b"\x01\x02\x03\x04"
        # Not parts: left alone.
        assert (session_dir / "session.0000.log.zst").read_bytes() == b"compressed earlier"
        assert (session_dir / "Examples_Can.start.json").read_text() == "{}"

    def test_plain_part_replaces_a_stale_compressed_copy(self, tmp_path):
        session_dir = _make_session(tmp_path)
        (session_dir / "session.0001.log.zst").write_bytes(b"older, shorter copy")

        with _recorder(running=False):
            recover_session(session_dir)

        assert decompress_file_to_buffer(session_dir / "session.0001.log.zst").tobytes() == LOG_TEXT

    def test_finish_time_ignores_part_mtimes_far_from_the_heartbeat(self, tmp_path):
        # A folder copied without its timestamps.
        session_dir = _make_session(tmp_path)
        _set_mtime(session_dir / "session.0001.log", HEARTBEAT + timedelta(days=30))

        with _recorder(running=False):
            recover_session(session_dir)

        meta = read_session_metadata(session_dir)
        assert meta["duration_seconds"] == 7200.0 + HEARTBEAT_STALE_S

    def test_session_from_before_the_heartbeat_uses_the_last_write(self, tmp_path):
        meta = _meta()
        del meta["process"]
        session_dir = _make_session(tmp_path, meta)

        result = recover_session(session_dir)

        assert result.recovered
        assert read_session_metadata(session_dir)["duration_seconds"] == 7202.0


class TestIdempotence:
    def test_second_call_changes_nothing(self, tmp_path):
        session_dir = _make_session(tmp_path)
        with _recorder(running=False):
            recover_session(session_dir)
            first = (session_dir / "metadata.json").read_text(encoding="utf-8")
            # A replay of the recovered session rebuilt the cold tier, complete this time.
            (session_dir / "cold").mkdir()
            (session_dir / "cold" / "segment_0000000000.blkseg").write_bytes(b"all rows")

            result = recover_session(session_dir)

        assert not result.recovered and result.skipped is None
        assert not result.cold_removed and result.parts_compressed == 0
        assert (session_dir / "metadata.json").read_text(encoding="utf-8") == first
        assert (session_dir / "cold" / "segment_0000000000.blkseg").exists()

    def test_compression_can_be_left_for_a_later_call(self, tmp_path):
        session_dir = _make_session(tmp_path)

        with _recorder(running=False):
            quick = recover_session(session_dir, compress=False)
            assert quick.recovered and quick.parts_compressed == 0
            assert (session_dir / "session.0001.log").exists()

            later = recover_session(session_dir)

        assert not later.recovered and later.parts_compressed == 2
        assert not (session_dir / "session.0001.log").exists()


class TestSessionsLeftAlone:
    def _assert_untouched(self, session_dir, result):
        assert not result.recovered and result.skipped
        assert (session_dir / "session.0001.log").read_bytes() == LOG_TEXT
        assert (session_dir / "cold" / "segment_0000000000.blkseg").exists()
        assert len(list(session_dir.glob("*.tmp"))) == 2
        assert not (session_dir / LOCK_FILE_NAME).exists()

    def test_recording_session(self, tmp_path):
        meta = _meta()
        meta["process"]["heartbeat_at"] = datetime.now(timezone.utc).isoformat()
        session_dir = _make_session(tmp_path, meta)

        with _recorder(running=True):
            result = recover_session(session_dir, force=True)

        self._assert_untouched(session_dir, result)
        assert read_session_metadata(session_dir)["status"] == "active"

    def test_stale_heartbeat_but_the_recorder_is_still_running(self, tmp_path):
        # e.g. stopped in a debugger - session_status() alone calls this crashed.
        session_dir = _make_session(tmp_path)

        with _recorder(running=True):
            result = recover_session(session_dir, force=True)

        self._assert_untouched(session_dir, result)
        assert "still running" in result.skipped

    def test_finished_session(self, tmp_path):
        session_dir = _make_session(tmp_path, _meta(status="finished"))

        result = recover_session(session_dir)

        self._assert_untouched(session_dir, result)

    def test_other_host_needs_force(self, tmp_path):
        meta = _meta()
        meta["process"]["host"] = "some-other-machine"
        session_dir = _make_session(tmp_path, meta)

        result = recover_session(session_dir)
        self._assert_untouched(session_dir, result)
        assert "some-other-machine" in result.skipped

        assert recover_session(session_dir, force=True).recovered

    def test_missing_folder(self, tmp_path):
        assert recover_session(tmp_path / "nope").skipped == "not a directory"


class TestUnreadableMetadata:
    def test_torn_metadata_is_rebuilt_from_the_folder(self, tmp_path):
        session_dir = _make_session(tmp_path, metadata_text='{"session_id": "2026')
        (tmp_path / "ws" / "profiles" / "Examples").mkdir(parents=True)
        (tmp_path / "ws" / "profiles" / "Examples_Can").mkdir()
        assert list_sessions(tmp_path, "proj") == []

        with patch.object(session_recovery, "get_workspace_dir", return_value=tmp_path / "ws"):
            result = recover_session(session_dir)

        assert result.recovered and result.metadata_rebuilt
        meta = read_session_metadata(session_dir)
        assert meta["status"] == "crashed"
        assert meta["metadata_rebuilt"] is True
        assert meta["config"]["profile"] == "Examples_Can"
        assert meta["project"]["display_name"] == "Untitled"
        assert meta["loggers"] == {
            "session": {"extension": "log", "last_part": 1},
            "src_502d8046": {"extension": "bin", "last_part": 0},
        }
        # Folder names carry local time.
        created = session_lister._parse_timestamp(meta["created_at"])
        assert created.astimezone().strftime("%Y%m%d_%H%M%S") == "20260314_124429"

        (listed,) = list_sessions(tmp_path, "proj")
        assert listed.status == "crashed" and listed.profile == "Examples_Can"
        assert listed.duration_seconds is not None

    def test_unknown_profile_keeps_the_whole_name(self, tmp_path):
        session_dir = _make_session(tmp_path, metadata_text="")

        with patch.object(session_recovery, "get_workspace_dir", return_value=tmp_path / "ws"):
            recover_session(session_dir)

        meta = read_session_metadata(session_dir)
        assert meta["config"]["profile"] == ""
        assert meta["project"]["display_name"] == "Examples_Can_Untitled"

    def test_folder_written_to_moments_ago_is_left_alone(self, tmp_path):
        session_dir = _make_session(tmp_path, metadata_text="{")
        _set_mtime(session_dir / "session.0001.log", datetime.now(timezone.utc))

        result = recover_session(session_dir)

        assert not result.recovered and "moments ago" in result.skipped
        assert (session_dir / "metadata.json").read_text(encoding="utf-8") == "{"

    def test_folder_without_parts_is_not_a_session(self, tmp_path):
        folder = tmp_path / "proj" / FOLDER
        folder.mkdir(parents=True)
        (folder / "notes.txt").write_text("x")

        assert recover_session(folder).skipped == "not a session folder"
        assert [p.name for p in folder.iterdir()] == ["notes.txt"]


class TestLock:
    def test_session_being_recovered_elsewhere_is_skipped(self, tmp_path):
        session_dir = _make_session(tmp_path)
        lock = session_dir / LOCK_FILE_NAME
        lock.write_text(json.dumps({"pid": 777, "host": platform.node()}), encoding="utf-8")

        def running_since(pid, timestamp):
            return pid == 777  # the other recoverer lives, the recorder (4242) does not

        with (
            patch.object(session_lister, "_pid_alive", return_value=False),
            patch.object(session_recovery, "_process_running_since", side_effect=running_since),
        ):
            result = recover_session(session_dir)

        assert not result.recovered and "another process" in result.skipped
        assert lock.exists()
        assert (session_dir / "session.0001.log").exists()

    def test_lock_of_a_dead_recovery_is_taken_over(self, tmp_path):
        session_dir = _make_session(tmp_path)
        (session_dir / LOCK_FILE_NAME).write_text(json.dumps({"pid": 777, "host": platform.node()}), encoding="utf-8")

        with _recorder(running=False):
            result = recover_session(session_dir)

        assert result.recovered
        assert not (session_dir / LOCK_FILE_NAME).exists()


def test_process_running_since_tells_a_reused_pid_apart():
    import psutil

    started = psutil.Process(os.getpid()).create_time()
    assert session_recovery._process_running_since(os.getpid(), time.time())
    # A session created before this process started can't have been recorded by it.
    assert not session_recovery._process_running_since(os.getpid(), started - 60)


class TestBlinkRecoverCommand:
    def _run(self, tmp_path, monkeypatch, capsys, *argv):
        from argparse import ArgumentParser

        from blinkview.utils.session_recovery import run_recover, setup_recover_parser

        monkeypatch.setattr(
            "blinkview.utils.session_lister.resolve_log_root", lambda log_dir=None, settings=None: (tmp_path, "proj")
        )
        parser = ArgumentParser()
        setup_recover_parser(parser)
        with _recorder(running=False):
            run_recover(parser.parse_args(list(argv)))
        return capsys.readouterr()

    def _add_finished_session(self, tmp_path):
        finished = tmp_path / "proj" / "20260313_090000_Examples_Can_Untitled"
        finished.mkdir()
        (finished / "metadata.json").write_text(json.dumps(_meta(status="finished")), encoding="utf-8")
        return finished

    def test_without_arguments_recovers_every_crashed_session(self, tmp_path, monkeypatch, capsys):
        session_dir = _make_session(tmp_path)
        self._add_finished_session(tmp_path)

        captured = self._run(tmp_path, monkeypatch, capsys)

        assert read_session_metadata(session_dir)["status"] == "crashed"
        # Only the session something was done to is reported.
        assert captured.out.splitlines() == [
            f"{FOLDER}: marked as crashed, partial cold storage removed, 2 temp file(s) removed, "
            "2 log part(s) compressed"
        ]
        assert "1 recovered of 1 broken session(s)." in captured.err

    def test_summary_counts_broken_sessions_that_were_left_alone(self, tmp_path, monkeypatch, capsys):
        meta = _meta()
        meta["process"]["host"] = "some-other-machine"
        _make_session(tmp_path, meta)
        self._add_finished_session(tmp_path)

        captured = self._run(tmp_path, monkeypatch, capsys)

        assert "0 recovered of 1 broken session(s)." in captured.err

    def test_summary_when_nothing_is_broken(self, tmp_path, monkeypatch, capsys):
        self._add_finished_session_in_new_project(tmp_path)

        captured = self._run(tmp_path, monkeypatch, capsys)

        assert "No broken sessions among 1." in captured.err

    def test_finds_a_session_whose_metadata_is_unreadable(self, tmp_path, monkeypatch, capsys):
        session_dir = _make_session(tmp_path, metadata_text="{")

        with patch.object(session_recovery, "get_workspace_dir", return_value=tmp_path / "ws"):
            captured = self._run(tmp_path, monkeypatch, capsys)

        assert "metadata rebuilt" in captured.out
        assert read_session_metadata(session_dir)["status"] == "crashed"

    def test_named_session_reports_why_it_was_skipped(self, tmp_path, monkeypatch, capsys):
        finished = self._add_finished_session_in_new_project(tmp_path)

        captured = self._run(tmp_path, monkeypatch, capsys, finished.name)

        assert captured.out.strip() == f"{finished.name}: skipped - session is finished"

    def _add_finished_session_in_new_project(self, tmp_path):
        (tmp_path / "proj").mkdir()
        return self._add_finished_session(tmp_path)

    def test_no_compress_keeps_the_plain_parts(self, tmp_path, monkeypatch, capsys):
        session_dir = _make_session(tmp_path)

        self._run(tmp_path, monkeypatch, capsys, FOLDER, "--no-compress")

        assert (session_dir / "session.0001.log").exists()
        assert read_session_metadata(session_dir)["status"] == "crashed"

    def test_unknown_session_exits_with_an_error(self, tmp_path, monkeypatch, capsys):
        import pytest

        _make_session(tmp_path)

        with pytest.raises(SystemExit) as exit_info:
            self._run(tmp_path, monkeypatch, capsys, "no_such_session")

        assert exit_info.value.code == 1
        assert "no session folder 'no_such_session'" in capsys.readouterr().err
