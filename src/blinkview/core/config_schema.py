# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Profile configuration checked against the factory schemas, for edits that do not come from
the config editor (the mcp_server pipeline's `apply_config_patch`, see parsers/mcp_server.py).

The schemas are the ones the config editor draws its forms from (core/configurable.py): JSON
Schema, plus `_factory` on an object whose real schema depends on its `type` key, and
`_reference` on a string naming another entry. That is why this walks schema and config
together instead of handing both to `jsonschema`.

Pure functions on dicts and a FactoryRegistry: no Qt, no numba, no running registry."""

from typing import Any, List, NamedTuple, Optional, Tuple

from blinkview.core.constants import FactoryCategory


class ConfigPatchError(ValueError):
    """A config patch that was refused. Nothing was changed; the message says why."""


# Top-level profile keys with a schema: (factory category, holds entries by id).
CONFIG_ROOTS = {
    "sources": (FactoryCategory.SOURCE, True),
    "pipelines": (FactoryCategory.PARSER, True),
    "central": (FactoryCategory.CENTRAL, False),
    "reorder": (FactoryCategory.REORDER, False),
}

# The GUI config (a second ConfigManager, see BlinkMainWindow) holds the watch lists by id.
WATCHES_ROOT = "watches"
_BY_ID_ROOTS = frozenset([WATCHES_ROOT, *(root for root, (_category, by_id) in CONFIG_ROOTS.items() if by_id)])

# What each kind of watch entry needs (ui/widgets/TelemetryWatch.py reads these without a default)
# and may also have. A row shows the latest value of its modules; a button sends a command.
WATCH_ENTRY_FIELDS = {
    "section": (("label",), ("collapsed",)),
    "row": (("key", "label"), ("modules", "is_expanded")),
    "button": (("key", "label"), ("command_payload", "target_device")),
    "group_start": (("key",), ("label",)),
    "group_end": (("key",), ()),
}

# Pipelines an outside edit may not touch: they decide what the editor itself is allowed to do.
PROTECTED_PIPELINE_TYPES = ("mcp_server",)

# Schema keys that only tell the config editor how to draw a field.
_UI_ONLY_KEYS = ("ui_order", "ui_remember", "ui_type", "ui_file_filter", "enum_tooltips", "_factory_dropdown_hidden")

_JSON_TYPE_NAMES = {dict: "object", list: "array", str: "string", bool: "boolean", int: "integer", float: "number"}


class ConfigCheck(NamedTuple):
    errors: List[str]  # the config would not work as written
    warnings: List[str]  # probably a mistake, e.g. a key the schema does not know


def split_path(path: str) -> List[str]:
    """'/pipelines/p1/rules/0' -> ['pipelines', 'p1', 'rules', '0'] (JSON pointer)."""
    return [part.replace("~1", "/").replace("~0", "~") for part in path.split("/") if part]


def _type_names(factories, category: str) -> List[str]:
    return [name for name, _doc in factories.get_category_types(category)]


def _concrete_schema(factories, schema: dict, data: Any) -> Tuple[dict, Optional[str]]:
    """The schema of the type `data` names, for a `_factory` object; `schema` itself otherwise.
    Returns (schema, error)."""
    category = schema.get("_factory")
    if category is None or factories.get_factory(category) is None:
        return schema, None

    type_name = data.get("type") if isinstance(data, dict) else None
    if type_name is None:
        type_name = schema.get("_factory_default")
    if type_name is None:
        return schema, f"needs a 'type', one of {_type_names(factories, category)}"
    try:
        return factories.get_schema(category, type_name), None
    except (ValueError, KeyError, AttributeError):
        return schema, f"unknown type {type_name!r}, expected one of {_type_names(factories, category)}"


def _describe(value: Any) -> str:
    return f"{_JSON_TYPE_NAMES.get(type(value), type(value).__name__)} {value!r}"


def _matches_type(expected: str, value: Any) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return True  # an untyped field takes anything


def _check_node(factories, config: dict, schema: dict, data: Any, path: str, check: ConfigCheck):
    if data is None:
        return  # same as not set: hydration fills in the default

    expected = schema.get("type")
    if expected == "object":
        if not isinstance(data, dict):
            check.errors.append(f"{path}: expected an object, got {_describe(data)}")
            return
        is_factory = "_factory" in schema
        schema, error = _concrete_schema(factories, schema, data)
        if error:
            check.errors.append(f"{path}: {error}")
            return

        properties = schema.get("properties") or {}
        required = schema.get("required")
        for key in required if isinstance(required, list) else ():
            prop = properties.get(key) or {}
            # Objects, arrays and anything with a default are filled in when the config is applied.
            if data.get(key) is None and "default" not in prop and prop.get("type") not in ("object", "array"):
                check.errors.append(f"{path}: '{key}' is required")

        extra = schema.get("additionalProperties")
        for key, value in data.items():
            if key in properties:
                _check_node(factories, config, properties[key], value, f"{path}/{key}", check)
            elif isinstance(extra, dict):
                _check_node(factories, config, extra, value, f"{path}/{key}", check)
            elif properties and not (key == "type" and is_factory):
                check.warnings.append(f"{path}: unknown key '{key}' (known: {', '.join(properties)})")
        return

    if expected == "array":
        if not isinstance(data, list):
            check.errors.append(f"{path}: expected an array, got {_describe(data)}")
            return
        items = schema.get("items")
        if isinstance(items, dict):
            for index, item in enumerate(data):
                _check_node(factories, config, items, item, f"{path}/{index}", check)
        return

    if isinstance(expected, str) and not _matches_type(expected, data):
        check.errors.append(f"{path}: expected {expected}, got {_describe(data)}")
        return

    enum = schema.get("enum")
    if enum and data not in enum and not schema.get("_allow_custom"):
        check.errors.append(f"{path}: {data!r} is not one of {enum}")
    if isinstance(data, str):
        shortest, longest = schema.get("minLength", 0), schema.get("maxLength")
        if len(data) < shortest or (longest is not None and len(data) > longest):
            wanted = f"{shortest}" if shortest == longest else f"{shortest} to {longest or 'any number of'}"
            check.errors.append(f"{path}: {data!r} has {len(data)} characters, expected {wanted}")
        if schema.get("_module_path"):
            _check_module_path(config, data, path, check)
    if isinstance(data, (int, float)) and not isinstance(data, bool):
        if "minimum" in schema and data < schema["minimum"]:
            check.errors.append(f"{path}: {data} is below the minimum {schema['minimum']}")
        if "maximum" in schema and data > schema["maximum"]:
            check.errors.append(f"{path}: {data} is above the maximum {schema['maximum']}")

    reference = schema.get("_reference")
    if reference and isinstance(data, str):
        known = _reference_ids(config, reference)
        if data not in known:
            check.errors.append(f"{path}: no entry '{data}' in {reference} (have: {sorted(known)})")


def _pipeline_names(config: dict) -> dict:
    """Device name -> ids of the pipelines logging as it (PipelineManager: `name`, or the id)."""
    names = {}
    pipelines = config.get("pipelines")
    for entry_id, entry in pipelines.items() if isinstance(pipelines, dict) else ():
        if isinstance(entry, dict):
            names.setdefault(str(entry.get("name", entry_id)).lower(), []).append(entry_id)
    return names


def _check_module_path(config: dict, data: str, path: str, check: ConfigCheck):
    """A `_module_path` field: 'device.module', as IDRegistry.resolve_module splits it."""
    device, _, module = data.partition(".")
    if not device or not module:
        check.errors.append(f"{path}: {data!r} is not a 'device.module' name, e.g. 'board.app.battery'")
        return
    names = _pipeline_names(config)
    if device.lower() not in names:
        check.warnings.append(
            f"{path}: {data!r} is read as module '{module}' of device '{device}', and no pipeline is named "
            f"'{device}' (pipeline names: {sorted(names)}) - is the device name missing in front?"
        )


def _reference_ids(config: dict, reference: str) -> set:
    """Ids a `_reference` field may hold - what Registry.get_reference_target can resolve."""
    sources = set(config.get("sources") or {})
    pipelines = set(config.get("pipelines") or {})
    if reference == "/sources":
        return sources
    if reference == "/pipelines":
        return pipelines
    return sources | pipelines | {"central", "reorder"}


def _entries(config: dict):
    """(path, factory category, entry config) of every entry that has a schema."""
    for root, (category, by_id) in CONFIG_ROOTS.items():
        value = config.get(root)
        if value is None:
            continue
        if not by_id:
            yield f"/{root}", category, value
        elif isinstance(value, dict):
            for entry_id, entry in value.items():
                yield f"/{root}/{entry_id}", category, entry
        else:
            yield f"/{root}", None, value


def check_config(factories, config: dict) -> ConfigCheck:
    """Every source, pipeline, central and reorder entry of `config` against its schema."""
    check = ConfigCheck([], [])
    for path, category, entry in _entries(config):
        if category is None:
            check.errors.append(f"{path}: expected an object of entries by id, got {_describe(entry)}")
            continue
        _check_node(factories, config, {"type": "object", "_factory": category}, entry, path, check)

    for name, ids in _pipeline_names(config).items():
        if len(ids) > 1:
            check.warnings.append(
                f"/pipelines: {', '.join(sorted(ids))} share the name '{name}' and so log as one device. A pipeline "
                "reading another's lines (key/value extraction) ignores its own device: give it a name of its own"
            )
    return check


def changed_entries(before: dict, after: dict) -> List[str]:
    """Paths of what differs between two configs: '/pipelines/<id>', '/central', '/version', ..."""
    changed = []
    for root in sorted(set(before) | set(after)):
        old, new = before.get(root), after.get(root)
        if old == new:
            continue
        by_id = root in _BY_ID_ROOTS
        if by_id and isinstance(old, dict) and isinstance(new, dict):
            changed.extend(f"/{root}/{key}" for key in sorted(set(old) | set(new)) if old.get(key) != new.get(key))
        else:
            changed.append(f"/{root}")
    return changed


def check_change(factories, before: dict, after: dict) -> List[str]:
    """Decides whether an outside edit turning `before` into `after` may go ahead. Raises
    ConfigPatchError when it touches something it may not, or adds a schema error; returns the
    warnings it adds. Errors and warnings `before` already had are not held against the edit:
    a profile that loads today stays editable."""
    for path in changed_entries(before, after):
        parts = split_path(path)
        if parts[0] not in CONFIG_ROOTS:
            raise ConfigPatchError(f"'{path}' cannot be changed here, only {', '.join('/' + r for r in CONFIG_ROOTS)}")
        if parts[0] == "pipelines" and len(parts) == 2:
            for config in (before, after):
                entry = (config.get("pipelines") or {}).get(parts[1])
                if isinstance(entry, dict) and entry.get("type") in PROTECTED_PIPELINE_TYPES:
                    raise ConfigPatchError(
                        f"'{path}' is a '{entry['type']}' pipeline: it sets what an agent may do, "
                        "and only the user changes it"
                    )

    old, new = check_config(factories, before), check_config(factories, after)
    errors = [error for error in new.errors if error not in old.errors]
    if errors:
        raise ConfigPatchError(
            "the patch was not applied, it would leave the configuration invalid:\n" + "\n".join(errors)
        )
    warnings = [warning for warning in new.warnings if warning not in old.warnings]

    # PipelineManager binds a pipeline's device when it builds it, and never again.
    old_pipelines, new_pipelines = before.get("pipelines"), after.get("pipelines")
    if isinstance(old_pipelines, dict) and isinstance(new_pipelines, dict):
        for entry_id, entry in new_pipelines.items():
            was = old_pipelines.get(entry_id)
            if isinstance(was, dict) and isinstance(entry, dict):
                if was.get("name", entry_id) != entry.get("name", entry_id):
                    warnings.append(
                        f"/pipelines/{entry_id}/name: a running pipeline keeps logging under its old name "
                        f"'{was.get('name', entry_id)}' until BlinkView restarts. To rename it now, remove the "
                        "entry and add it under a new id"
                    )
    return warnings


def schema_at(factories, config: dict, path: str) -> dict:
    """The schema of what sits at `path` in `config`, with every `_factory` object on the way
    resolved to the type the config names there. Raises ConfigPatchError for a path without one."""
    parts = split_path(path)
    if not parts or parts[0] not in CONFIG_ROOTS:
        raise ConfigPatchError(
            f"no schema for '{path}': the path must start with one of {['/' + r for r in CONFIG_ROOTS]}"
        )

    category, by_id = CONFIG_ROOTS[parts[0]]
    data = config.get(parts[0])
    schema = {"type": "object", "_factory": category}
    rest = parts[1:]
    if by_id:
        if not rest:
            return {"type": "object", "description": "Entries by id.", "additionalProperties": schema}
        data = data.get(rest[0]) if isinstance(data, dict) else None
        if data is None:
            raise ConfigPatchError(f"nothing at '/{parts[0]}/{rest[0]}' in the profile's configuration")
        rest = rest[1:]

    walked = "/" + "/".join(parts[: len(parts) - len(rest)])
    for part in [None, *rest]:
        if part is not None:
            walked = f"{walked}/{part}"
            if schema.get("type") == "array":
                schema = schema.get("items")
                try:
                    data = data[int(part)] if isinstance(data, list) else None
                except (ValueError, IndexError):
                    data = None
            else:
                properties = schema.get("properties") or {}
                schema = properties.get(part) or schema.get("additionalProperties")
                data = data.get(part) if isinstance(data, dict) else None
            if not isinstance(schema, dict):
                raise ConfigPatchError(f"no schema for '{walked}'")
        resolved, error = _concrete_schema(factories, schema, data)
        # A factory field that is not set yet keeps its `_factory` marker: any type may go there.
        if error is None:
            schema = resolved
        elif data is not None:
            raise ConfigPatchError(f"{walked}: {error}")
    return schema


def for_agent(schema: Any) -> Any:
    """A copy of `schema` without the keys that only tell the config editor how to draw it."""
    if isinstance(schema, dict):
        return {key: for_agent(value) for key, value in schema.items() if key not in _UI_ONLY_KEYS}
    if isinstance(schema, list):
        return [for_agent(item) for item in schema]
    return schema


def check_watches(config: dict, known_modules=None) -> ConfigCheck:
    """The watch lists of a GUI config: what the watch window needs to load them. A row's
    module that is not in `known_modules` ('device.module' names) is a warning - it may only
    not have logged yet, but opening the watch registers it under that name."""
    check = ConfigCheck([], [])
    watches = config.get(WATCHES_ROOT)
    if watches is None:
        return check
    if not isinstance(watches, dict):
        check.errors.append(f"/{WATCHES_ROOT}: expected an object of watches by id, got {_describe(watches)}")
        return check

    for watch_id, watch in watches.items():
        path = f"/{WATCHES_ROOT}/{watch_id}"
        if not isinstance(watch, dict):
            check.errors.append(f"{path}: expected an object, got {_describe(watch)}")
            continue
        if not isinstance(watch.get("name"), str) or not watch["name"].strip():
            check.errors.append(f"{path}: 'name' must be a non-empty string")
        if watch.get("id", watch_id) != watch_id:
            check.errors.append(f"{path}: 'id' must be the watch's key '{watch_id}', got {_describe(watch['id'])}")
        entries = watch.get("entries", [])
        if not isinstance(entries, list):
            check.errors.append(f"{path}/entries: expected an array, got {_describe(entries)}")
            continue

        keys = set()
        open_groups = 0
        for index, entry in enumerate(entries):
            entry_path = f"{path}/entries/{index}"
            if not isinstance(entry, dict):
                check.errors.append(f"{entry_path}: expected an object, got {_describe(entry)}")
                continue
            kind = entry.get("type", "row")
            if kind not in WATCH_ENTRY_FIELDS:
                check.errors.append(f"{entry_path}: unknown type {kind!r}, expected one of {list(WATCH_ENTRY_FIELDS)}")
                continue
            required, optional = WATCH_ENTRY_FIELDS[kind]
            for field in required:
                if not isinstance(entry.get(field), str) or not entry[field]:
                    check.errors.append(f"{entry_path}: a {kind} needs '{field}', a non-empty string")
            for field in entry:
                if field != "type" and field not in required and field not in optional:
                    known = ", ".join((*required, *optional))
                    check.warnings.append(f"{entry_path}: unknown key '{field}' (a {kind} has: {known})")

            key = entry.get("key")
            if isinstance(key, str) and key:
                if key in keys:
                    check.errors.append(f"{entry_path}: key '{key}' is used twice in this watch")
                keys.add(key)
            if kind == "group_start":
                open_groups += 1
            elif kind == "group_end":
                open_groups -= 1
                if open_groups < 0:
                    check.errors.append(f"{entry_path}: group_end without a group_start before it")
                    open_groups = 0
            elif kind == "row":
                modules = entry.get("modules", [])
                if not isinstance(modules, list) or not all(isinstance(m, str) and "." in m for m in modules):
                    check.errors.append(f"{entry_path}/modules: expected a list of 'device.module' names")
                elif known_modules is not None:
                    for module in modules:
                        if module not in known_modules:
                            check.warnings.append(f"{entry_path}: no module '{module}' has logged in this session")
        if open_groups:
            check.errors.append(f"{path}/entries: {open_groups} group_start without a group_end")
    return check


def check_gui_change(before: dict, after: dict, known_modules=None) -> List[str]:
    """check_change for the GUI config: only the watch lists may change. Raises
    ConfigPatchError, or returns the warnings the edit adds."""
    for path in changed_entries(before, after):
        if split_path(path)[0] != WATCHES_ROOT:
            raise ConfigPatchError(f"'{path}' cannot be changed here, only /{WATCHES_ROOT}")

    old, new = check_watches(before, known_modules), check_watches(after, known_modules)
    errors = [error for error in new.errors if error not in old.errors]
    if errors:
        raise ConfigPatchError("the patch was not applied, it would leave a watch unusable:\n" + "\n".join(errors))
    return [warning for warning in new.warnings if warning not in old.warnings]
