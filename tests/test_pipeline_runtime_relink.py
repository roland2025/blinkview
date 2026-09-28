# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Real end-to-end tests for editing pipeline/source topology at runtime through
ConfigManager.apply_patch - the same path the sidebar/config dialogs use - and confirming the
data actually reaches central storage, without restarting the Registry.

Regression: re-pointing an existing pipeline's sources_ to a newly added binary_file source
never streamed until an app restart. The pipeline was also subscribed to its own
"/pipelines/<id>" config path, registered before the manager's "/pipelines" subscription, so it
applied the new sources_ first and the manager's old/new link diff came out empty."""

import json
import time

import pytest

from tests.fakes.real_registry import make_real_registry


def _poll_for_message(log_pool, device_id, needle: bytes, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with log_pool.get_snapshot() as segments:
            for seg in segments:
                for _ts, msg, _rx_ts, _level, _module, device, *_rest in seg:
                    if device == device_id and needle in bytes(msg):
                        return
        time.sleep(0.02)
    pytest.fail(f"never saw a row for device {device_id} containing {needle!r} within {timeout}s")


def _binary_source(path, name, enabled=True):
    return {
        "enabled": enabled,
        "type": "binary_file",
        "name": name,
        "file_path": str(path),
        "loop": True,
    }


def _write_profile(tmp_path, sources, pipelines):
    (tmp_path / "test_config.json").write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": sources,
                "pipelines": pipelines,
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )


@pytest.fixture
def data_files(tmp_path):
    old = tmp_path / "old.bin"
    new = tmp_path / "new.bin"
    old.write_bytes(b"".join(b"from old source %d\n" % i for i in range(50)))
    new.write_bytes(b"".join(b"from new source %d\n" % i for i in range(50)))
    return old, new


class TestRelinkExistingPipeline:
    def test_repointing_sources_to_a_new_source_streams_without_restart(self, tmp_path, data_files):
        old, new = data_files
        _write_profile(
            tmp_path,
            sources={"src_old": _binary_source(old, "old", enabled=False)},
            pipelines={"pipe_1": {"enabled": True, "type": "default", "name": "p", "sources_": ["src_old"]}},
        )
        reg = make_real_registry(tmp_path, "relink", start=True)
        try:
            reg.config.apply_patch("/sources", [{"op": "add", "path": "/src_new", "value": _binary_source(new, "new")}])
            reg.config.apply_patch("/pipelines/pipe_1", [{"op": "replace", "path": "/sources_/0", "value": "src_new"}])

            pipe = reg.pipelines.get("pipe_1")
            assert pipe in reg.sources.get("src_new").subscribers
            assert pipe not in reg.sources.get("src_old").subscribers

            _poll_for_message(reg.central.log_pool, reg.id_registry.get_device("p").id, b"from new source")
        finally:
            reg.stop()


class TestEnableDisabledPipeline:
    def test_enabling_a_pipeline_disabled_at_startup_streams_without_restart(self, tmp_path, data_files):
        _old, new = data_files
        _write_profile(
            tmp_path,
            sources={"src_1": _binary_source(new, "bin")},
            pipelines={"pipe_1": {"enabled": False, "type": "default", "name": "p", "sources_": ["src_1"]}},
        )
        reg = make_real_registry(tmp_path, "enable", start=True)
        try:
            reg.config.apply_patch("/pipelines/pipe_1", [{"op": "replace", "path": "/enabled", "value": True}])

            assert reg.pipelines.get("pipe_1") in reg.sources.get("src_1").subscribers
            _poll_for_message(reg.central.log_pool, reg.id_registry.get_device("p").id, b"from new source")
        finally:
            reg.stop()


class TestProfileMissingManagerKeys:
    def test_profile_without_sources_and_pipelines_keys_still_builds_both_managers(self, tmp_path, data_files):
        """A profile JSON lacking the keys used to crash both managers' apply_config(None),
        leaving registry.sources/pipelines None - so every later edit was silently ignored."""
        _old, new = data_files
        (tmp_path / "test_config.json").write_text(
            json.dumps(
                {
                    "version": "0.2",
                    "plugins": {},
                    "reorder": {"enabled": True, "type": "default"},
                    "central": {"enabled": True, "type": "default"},
                }
            )
        )
        reg = make_real_registry(tmp_path, "nokeys", start=True)
        try:
            assert reg.sources is not None
            assert reg.pipelines is not None

            reg.config.apply_patch(
                "/", [{"op": "add", "path": "/sources", "value": {"src_1": _binary_source(new, "bin")}}]
            )
            reg.config.apply_patch(
                "/",
                [
                    {
                        "op": "add",
                        "path": "/pipelines",
                        "value": {"pipe_1": {"enabled": True, "type": "default", "name": "p", "sources_": ["src_1"]}},
                    }
                ],
            )

            _poll_for_message(reg.central.log_pool, reg.id_registry.get_device("p").id, b"from new source")
        finally:
            reg.stop()
