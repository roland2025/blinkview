# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Editing one source or pipeline through ConfigManager.apply_patch - the path the config
dialogs and the mcp_server use - restarts that item only, not its siblings.

Regression: SourcesManager/PipelineManager apply every item's config on any edit under
"/sources" or "/pipelines". BaseDaemon.apply_config wrote "name" into the `logging` dict it had
just stored as self.logging, so the next apply saw self.logging differ from the profile's block,
reported a change and restarted every item that has a `logging` block."""

import json

import pytest

from tests.fakes.real_registry import make_real_registry

LOGGING = {"enabled": False, "type": "default"}


def _source(path, name):
    return {
        "enabled": True,
        "type": "binary_file",
        "name": name,
        "file_path": str(path),
        "loop": True,
        "logging": dict(LOGGING),
    }


def _pipeline(name, source_id):
    return {"enabled": True, "type": "default", "name": name, "sources_": [source_id], "logging": dict(LOGGING)}


@pytest.fixture
def registry(tmp_path):
    data = tmp_path / "data.bin"
    data.write_bytes(b"".join(b"line %d\n" % i for i in range(50)))
    (tmp_path / "test_config.json").write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {"src_a": _source(data, "a"), "src_b": _source(data, "b")},
                "pipelines": {"pipe_a": _pipeline("pa", "src_a"), "pipe_b": _pipeline("pb", "src_b")},
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )
    reg = make_real_registry(tmp_path, "sibling_restart", start=True)
    try:
        yield reg
    finally:
        reg.stop()


@pytest.fixture
def restarts(registry, monkeypatch):
    """The reference ids of the sources/pipelines restarted, in order."""
    restarted = []
    items = {**registry.sources.sources, **registry.pipelines.pipelines}
    for item_id, item in items.items():

        def restart(original=item.restart, item_id=item_id):
            restarted.append(item_id)
            return original()

        monkeypatch.setattr(item, "restart", restart)
    return restarted


# Each edit twice: the first apply after startup is the one that used to store the mutated dict,
# so the second is where an unchanged sibling first looked changed.
EDITS = [
    ("/sources/src_a", "loop", [False, True], "src_a"),
    ("/pipelines/pipe_a", "name", ["pa2", "pa3"], "pipe_a"),
]


@pytest.mark.parametrize("path, field, values, edited", EDITS, ids=["source", "pipeline"])
def test_edit_restarts_only_the_edited_item(registry, restarts, path, field, values, edited):
    for value in values:
        restarts.clear()
        registry.config.apply_patch(path, [{"op": "replace", "path": f"/{field}", "value": value}])
        assert restarts == [edited]


@pytest.mark.parametrize("path, field, values, edited", EDITS, ids=["source", "pipeline"])
def test_stored_logging_block_matches_the_profile(registry, restarts, path, field, values, edited):
    for value in values:
        registry.config.apply_patch(path, [{"op": "replace", "path": f"/{field}", "value": value}])

    for item in [*registry.sources.sources.values(), *registry.pipelines.pipelines.values()]:
        assert item.logging == LOGGING
