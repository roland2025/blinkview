# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""core/config_schema.py against the real factory schemas (every registered source, parser and
rule type), not hand-written ones: what it accepts has to be what the app can build."""

from copy import deepcopy

import pytest

import blinkview.core.central_storage  # noqa: F401  (registers the central storage types)
import blinkview.core.reorderer  # noqa: F401  (registers the reorder types)
import blinkview.io  # noqa: F401  (registers the source types)
import blinkview.parsers  # noqa: F401  (registers the parser and rule types)
from blinkview.core.config_schema import (
    ConfigPatchError,
    changed_entries,
    check_change,
    check_config,
    for_agent,
    schema_at,
)
from blinkview.core.factory_category_registry import build_system_factory_registry


@pytest.fixture(scope="module")
def factories():
    return build_system_factory_registry()


def _config():
    return {
        "version": "0.2",
        "sources": {"src_a": {"enabled": True, "type": "tcp_client", "name": "a", "host": "127.0.0.1", "port": 5000}},
        "pipelines": {
            "pipe_a": {"enabled": True, "type": "default", "name": "a", "sources_": ["src_a"]},
            "pipe_kv": {
                "enabled": True,
                "type": "multi_rule_key_value",
                "name": "a_kv",
                "sources_": ["pipe_a"],
                "rules": [{"type": "key_value", "module_name": "a.battery"}],
            },
            "pipe_mcp": {"enabled": True, "type": "mcp_server", "name": "mcp"},
        },
        "reorder": {"enabled": True, "type": "default"},
        "central": {"enabled": True, "type": "default"},
    }


def test_a_working_config_has_no_errors_or_warnings(factories):
    assert check_config(factories, _config()) == ([], [])


def test_errors_name_the_path_and_what_was_expected(factories):
    config = _config()
    config["sources"]["src_a"]["port"] = "5000"
    config["sources"]["src_b"] = {"type": "tcp_cliant"}
    config["pipelines"]["pipe_a"]["sources_"] = ["src_gone"]
    config["pipelines"]["pipe_kv"]["rules"] = [
        {"type": "anchor_word", "module_name": "a.battery", "match": "anywhere"},
        {"type": "key_value"},
        "not a rule",
    ]
    config["central"] = {"enabled": "yes", "type": "default"}

    errors = check_config(factories, config).errors

    assert "/sources/src_a/port: expected integer, got string '5000'" in errors
    assert any(e.startswith("/sources/src_b: unknown type 'tcp_cliant', expected one of [") for e in errors)
    assert any(e.startswith("/pipelines/pipe_a/sources_/0: no entry 'src_gone' in /sources") for e in errors)
    assert any(e.startswith("/pipelines/pipe_kv/rules/0/match: 'anywhere' is not one of") for e in errors)
    assert "/pipelines/pipe_kv/rules/1: 'module_name' is required" in errors
    assert "/pipelines/pipe_kv/rules/2: expected an object, got string 'not a rule'" in errors
    assert "/central/enabled: expected boolean, got string 'yes'" in errors
    assert len(errors) == 7


def test_an_unknown_key_is_a_warning_and_null_means_not_set(factories):
    config = _config()
    config["pipelines"]["pipe_kv"]["rules"][0].update(modul_name="battery", prefix_strip=None)

    check = check_config(factories, config)

    assert check.errors == []
    assert len(check.warnings) == 1
    assert check.warnings[0].startswith("/pipelines/pipe_kv/rules/0: unknown key 'modul_name' (known: ")


def test_a_rule_module_needs_its_device_and_a_device_some_pipeline_logs_as(factories):
    config = _config()
    config["pipelines"]["pipe_kv"]["rules"] = [
        {"type": "key_value", "module_name": "battery"},
        {"type": "anchor_word", "module_name": "app.battery", "module_suffix": "vbat", "pattern": "Vbat:"},
        {"type": "positional", "module_name": "A.battery", "module_suffix": "soc"},
    ]

    check = check_config(factories, config)

    assert check.errors == [
        "/pipelines/pipe_kv/rules/0/module_name: 'battery' is not a 'device.module' name, e.g. 'board.app.battery'"
    ]
    assert len(check.warnings) == 1
    assert check.warnings[0].startswith(
        "/pipelines/pipe_kv/rules/1/module_name: 'app.battery' is read as module 'battery' of device 'app', "
        "and no pipeline is named 'app' (pipeline names: ['a', 'a_kv', 'mcp'])"
    )


def test_a_delimiter_is_one_character(factories):
    config = _config()
    config["pipelines"]["pipe_kv"]["rules"] = [
        {"type": "key_value", "module_name": "a.battery", "kv_delimiter": ": ", "field_delimiter": ""},
        {"type": "key_value", "module_name": "a.battery", "kv_delimiter": ":", "field_delimiter": "&"},
    ]

    assert check_config(factories, config).errors == [
        "/pipelines/pipe_kv/rules/0/kv_delimiter: ': ' has 2 characters, expected 1",
        "/pipelines/pipe_kv/rules/0/field_delimiter: '' has 0 characters, expected 1",
    ]


def test_pipelines_sharing_a_name_and_a_rename_are_warnings(factories):
    before = _config()
    after = deepcopy(before)
    after["pipelines"]["pipe_kv"]["name"] = "A"

    warnings = check_change(factories, before, after)

    assert len(warnings) == 2
    assert warnings[0].startswith("/pipelines: pipe_a, pipe_kv share the name 'a' and so log as one device.")
    assert warnings[1].startswith("/pipelines/pipe_kv/name: a running pipeline keeps logging under its old name 'a_kv'")
    assert check_change(factories, after, deepcopy(after)) == []


def test_changed_entries_names_each_entry_that_differs():
    before, after = _config(), _config()
    after["sources"]["src_b"] = {"type": "udp"}
    del after["pipelines"]["pipe_a"]
    after["central"]["enabled"] = False
    after["version"] = "0.3"

    assert changed_entries(before, after) == ["/central", "/pipelines/pipe_a", "/sources/src_b", "/version"]
    assert changed_entries(before, deepcopy(before)) == []


def test_check_change_refuses_only_what_the_edit_itself_breaks(factories):
    before = _config()
    before["sources"]["src_a"]["port"] = "5000"  # already wrong: not this edit's fault

    after = deepcopy(before)
    after["pipelines"]["pipe_kv"]["rules"].append({"type": "key_value", "module_name": "a.motor", "kv_delim": ":"})
    warnings = check_change(factories, before, after)
    assert len(warnings) == 1 and "unknown key 'kv_delim'" in warnings[0]

    after["pipelines"]["pipe_kv"]["rules"].append({"type": "regex"})
    with pytest.raises(ConfigPatchError, match="rules/2: unknown type 'regex'") as refused:
        check_change(factories, before, after)
    assert "port" not in str(refused.value)


def test_check_change_refuses_other_keys_and_the_mcp_server_pipeline(factories):
    before = _config()

    after = deepcopy(before)
    after["version"] = "9"
    with pytest.raises(ConfigPatchError, match="'/version' cannot be changed here"):
        check_change(factories, before, after)

    after = deepcopy(before)
    after["pipelines"]["pipe_mcp"]["allow_config"] = True
    with pytest.raises(ConfigPatchError, match="'/pipelines/pipe_mcp' is a 'mcp_server' pipeline"):
        check_change(factories, before, after)

    after = deepcopy(before)
    after["pipelines"]["pipe_mcp2"] = {"type": "mcp_server", "allow_config": True}
    with pytest.raises(ConfigPatchError, match="'/pipelines/pipe_mcp2' is a 'mcp_server' pipeline"):
        check_change(factories, before, after)

    after = deepcopy(before)
    after["pipelines"]["pipe_a"]["type"] = "mcp_server"
    with pytest.raises(ConfigPatchError, match="is a 'mcp_server' pipeline"):
        check_change(factories, before, after)


def test_schema_at_follows_the_types_the_config_names(factories):
    config = _config()

    assert schema_at(factories, config, "/pipelines")["additionalProperties"] == {
        "type": "object",
        "_factory": "parser",
    }
    assert "rules" in schema_at(factories, config, "/pipelines/pipe_kv")["properties"]
    assert schema_at(factories, config, "/pipelines/pipe_kv/rules")["items"]["_factory"] == "key_value_rule"
    assert "kv_delimiter" in schema_at(factories, config, "/pipelines/pipe_kv/rules/0")["properties"]
    assert schema_at(factories, config, "/sources/src_a/port")["type"] == "integer"
    assert "properties" in schema_at(factories, config, "/central")
    # Not set in the config: the default type's schema where there is one, else the open slot.
    assert "properties" in schema_at(factories, config, "/pipelines/pipe_a/frame_decoder")
    assert schema_at(factories, config, "/pipelines/pipe_kv/rules/5") == {
        "type": "object",
        "_factory": "key_value_rule",
        "title": "Extraction Rule Setup",
    }

    for path, message in [
        ("/version", "no schema for '/version'"),
        ("/", "no schema for '/'"),
        ("/sources/nope", "nothing at '/sources/nope'"),
        ("/sources/src_a/nope", "no schema for '/sources/src_a/nope'"),
    ]:
        with pytest.raises(ConfigPatchError, match=message):
            schema_at(factories, config, path)


def test_for_agent_drops_only_the_editor_only_keys(factories):
    schema = factories.get_schema("key_value_rule", "anchor_word")
    cleaned = for_agent(schema)

    assert "ui_order" in schema["properties"]["match"]
    assert cleaned["properties"]["match"] == {
        "type": "string",
        "enum": ["starts_with", "contains", "ends_with"],
        "default": "contains",
        "description": schema["properties"]["match"]["description"],
    }
    assert cleaned["required"] == schema["required"]


# Unfinished time-sync work; they get a description when they are done.
_UNDESCRIBED_TYPES = {
    ("time_sync", "adb_time_syncer"),
    ("time_sync", "serial_time_syncer"),
    ("parser", "adb_time_syncer"),
    ("parser", "serial_time_syncer"),
    ("parser", "time_sync_profiler"),
}


def test_every_type_has_a_description(factories):
    """`list_types` shows the first line of a type's description and `get_schema` all of it:
    for an agent that is the only documentation of what a type is for."""
    missing = []
    for category in factories.get_categories():
        for name, doc in factories.get_category_types(category):
            schema = factories.get_schema(category, name)  # {} for a type without settings
            described = doc != "No description" and (not schema or bool(schema.get("description")))
            if not described and (category, name) not in _UNDESCRIBED_TYPES:
                missing.append(f"{category}/{name}")
    assert missing == []
