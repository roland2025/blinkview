# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Named profile parameters - one profile, several identical devices.

A profile declares parameters that point at config fields:

    "parameters": {
        "rtt_serial": {"paths": ["/sources/src_1eb62594/serial_number"], "description": "..."}
    }

and each run fills them in, lowest to highest precedence: the value already stored in the
profile (the default), parameter files (`--params left` -> `<profile>.left.params.json`, or an
explicit path), then individual `--param name=value` arguments. Values are applied to the
in-memory config only - ConfigManager keeps them out of the profile file (see
ConfigManager.param_bindings).

Pure functions on dicts/paths, no Qt and no registry access, so the CLI can use them too."""

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Tuple

PARAMS_KEY = "parameters"
# Named sets are `<profile>.<set>.params.json` - the set name comes before the kind, so every
# set file has the same ending (one .gitignore rule: `*.params.json`).
PARAMS_FILE_SUFFIX = ".params.json"


class _Missing:
    """Sentinel for "no such key" - e.g. an optional field the profile never set. Survives
    deepcopy (bindings remember it as the profile's own value), unlike a bare object()."""

    def __repr__(self):
        return "<missing>"

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


MISSING = _Missing()


class ProfileParamError(ValueError):
    """Bad --param/--params input or a broken parameter declaration. Raised before anything
    starts, so the message is shown to the user as-is."""


# ==========================================
# CLI / file input
# ==========================================
def parse_param_args(items: Optional[Iterable[str]]) -> Dict[str, str]:
    """`["a=1", "b=x=y"]` -> `{"a": "1", "b": "x=y"}` (split on the first '=' only). Later
    duplicates win, like repeated --param flags would be expected to."""
    result = {}
    for item in items or []:
        name, sep, value = item.partition("=")
        name = name.strip()
        if not sep or not name:
            raise ProfileParamError(f"Invalid --param '{item}': expected NAME=VALUE")
        result[name] = value
    return result


def is_params_file_spec(spec: str) -> bool:
    """A --params value is a file path (instead of a named set) when it has a path separator
    or a .json suffix."""
    return "/" in spec or "\\" in spec or spec.lower().endswith(".json")


def params_set_path(config_dir: Path, config_file_name: str, set_name: str) -> Path:
    """Where the named set `set_name` of a profile lives: next to `<profile>.json`."""
    return Path(config_dir) / f"{config_file_name}.{set_name}{PARAMS_FILE_SUFFIX}"


def list_params_sets(config_dir: Path, config_file_name: str) -> List[str]:
    prefix = f"{config_file_name}."
    config_dir = Path(config_dir)
    if not config_dir.is_dir():
        return []
    return sorted(
        f.name[len(prefix) : -len(PARAMS_FILE_SUFFIX)]
        for f in config_dir.iterdir()
        if f.is_file()
        and f.name.startswith(prefix)
        and f.name.endswith(PARAMS_FILE_SUFFIX)
        and len(f.name) > len(prefix) + len(PARAMS_FILE_SUFFIX)
    )


def resolve_params_file(spec: str, config_dir: Path, config_file_name: str) -> Path:
    if is_params_file_spec(spec):
        return Path(spec).expanduser().resolve()
    return params_set_path(config_dir, config_file_name, spec)


class LoadedParams(NamedTuple):
    values: Dict[str, Any]  # merged parameter values, later files win
    paths: List[Path]  # resolved files, in load order
    session_name: Optional[str]  # default session name ("session" of the last file that has one)
    origins: Dict[str, str]  # parameter -> where its value came from, e.g. "set 'left'"


# Structured params file: {"session": "Left board", "params": {"rtt_serial": "51024923"}}.
# A flat file is just the "params" object. The two can't be confused: flat values must be
# scalars, so a top-level "params" key holding an object can only mean the structured form.
STRUCTURED_KEYS = ("session", "params")


def read_params_file(path: Path) -> Tuple[Dict[str, Any], Optional[str]]:
    """One params file -> (values, session_name or None). Raises ProfileParamError."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise ProfileParamError(f"Cannot read parameter file {path}: {e}") from e

    if not isinstance(data, dict):
        raise ProfileParamError(f"Parameter file {path} must contain a JSON object of name: value pairs")

    session = None
    values = data
    if isinstance(data.get("params"), dict):
        unknown = sorted(set(data) - set(STRUCTURED_KEYS))
        if unknown:
            raise ProfileParamError(
                f"Unknown key(s) {', '.join(unknown)} in {path} (expected: {', '.join(STRUCTURED_KEYS)})"
            )
        values = data["params"]
        session = data.get("session")
        if session is not None and (not isinstance(session, str) or not session.strip()):
            raise ProfileParamError(f"'session' in {path} must be a non-empty string")

    for name, value in values.items():
        if isinstance(value, (dict, list)):
            raise ProfileParamError(f"Parameter '{name}' in {path} must be a plain value, not {type(value).__name__}")
    return dict(values), session.strip() if session else None


def write_params_file(path: Path, values: Dict[str, Any], session_name: Optional[str] = None):
    """Flat when there's no session name (the simplest thing to hand-edit), else structured."""
    from blinkview.utils.atomic_json_dump import atomic_json_dump

    data = {"session": session_name, "params": values} if session_name else values
    atomic_json_dump(data, path)


def load_params_files(specs: Optional[Iterable[str]], config_dir: Path, config_file_name: str) -> LoadedParams:
    """Loads and merges every --params file in order (later files win, for values and for the
    session name). Raises ProfileParamError for a missing/unreadable/malformed file."""
    merged: Dict[str, Any] = {}
    paths: List[Path] = []
    session_name = None
    origins: Dict[str, str] = {}

    for spec in specs or []:
        path = resolve_params_file(spec, config_dir, config_file_name)
        if not path.is_file():
            if is_params_file_spec(spec):
                raise ProfileParamError(f"Parameter file not found: {path}")
            available = list_params_sets(config_dir, config_file_name)
            hint = f" Available sets: {', '.join(available)}." if available else ""
            raise ProfileParamError(f"Parameter set '{spec}' not found (expected {path}).{hint}")

        values, file_session = read_params_file(path)
        merged.update(values)
        origin = f"file '{path.name}'" if is_params_file_spec(spec) else f"set '{spec}'"
        origins.update(dict.fromkeys(values, origin))
        session_name = file_session or session_name
        paths.append(path)

    return LoadedParams(merged, paths, session_name, origins)


def params_label(specs: Optional[Iterable[str]], cli_params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Short human label for this run's parameters - used as the default session name and in
    the window title. Named sets / file stems first ("left", "left+bench"), else the --param
    values themselves ("rtt_serial_123")."""
    parts = []
    for spec in specs or []:
        if is_params_file_spec(spec):
            stem = Path(spec).stem
            # "rtt.left.params.json" -> "left"
            if stem.endswith(".params"):
                stem = stem[: -len(".params")].rsplit(".", 1)[-1]
            parts.append(stem)
        else:
            parts.append(spec)
    if parts:
        return "+".join(parts)
    if cli_params:
        return "_".join(f"{k}_{v}" for k, v in cli_params.items())
    return None


# ==========================================
# Declarations
# ==========================================
def get_declarations(config: dict) -> Dict[str, dict]:
    """`config["parameters"]`, normalized to `{name: {"paths": [...], "description": str}}`.
    A single `"path": "..."` is accepted as shorthand for `"paths": ["..."]`."""
    raw = (config or {}).get(PARAMS_KEY) or {}
    if not isinstance(raw, dict):
        raise ProfileParamError(f"Profile '{PARAMS_KEY}' must be an object of name: declaration")

    result = {}
    for name, decl in raw.items():
        if not isinstance(decl, dict):
            raise ProfileParamError(f"Parameter '{name}' declaration must be an object")
        paths = decl.get("paths")
        if paths is None and "path" in decl:
            paths = [decl["path"]]
        if isinstance(paths, str):
            paths = [paths]
        if not paths or not all(isinstance(p, str) and p.startswith("/") for p in paths):
            raise ProfileParamError(
                f"Parameter '{name}' needs 'paths': a list of config paths like '/sources/<id>/serial_number'"
            )
        result[name] = {"paths": list(paths), "description": decl.get("description", "")}
    return result


def add_declaration(config: dict, name: str, path: str, description: str = None) -> dict:
    """Adds `path` to parameter `name` (creating it), in place. The path must already exist in
    `config` - a parameter always has a default."""
    if not name or not name.replace("_", "").isalnum():
        raise ProfileParamError(f"Invalid parameter name '{name}' (letters, digits and '_' only)")
    if not path.startswith("/"):
        raise ProfileParamError(f"Invalid path '{path}': must start with '/'")
    if not target_exists(config, path):
        raise ProfileParamError(f"Path '{path}' does not exist in the profile")
    for other, decl in get_declarations(config).items():
        if other != name and path in decl["paths"]:
            # One field, two parameters: which value would win would depend on argument order.
            raise ProfileParamError(f"Path '{path}' already belongs to parameter '{other}'")

    params = config.setdefault(PARAMS_KEY, {})
    decl = params.setdefault(name, {"paths": []})
    paths = decl.setdefault("paths", [])
    if "path" in decl:  # normalize the shorthand form
        legacy = decl.pop("path")
        if legacy not in paths:
            paths.insert(0, legacy)
    if path not in paths:
        paths.append(path)
    if description is not None:
        decl["description"] = description
    return decl


# ==========================================
# JSON pointers (RFC 6901, same syntax as ConfigManager / jsonpatch paths)
# ==========================================
def _split_pointer(path: str) -> List[str]:
    if path in ("", "/"):
        return []
    return [p.replace("~1", "/").replace("~0", "~") for p in path.lstrip("/").split("/")]


def _step(container, key: str):
    if isinstance(container, dict):
        return container.get(key, MISSING)
    if isinstance(container, list):
        try:
            idx = int(key)
        except ValueError:
            return MISSING
        return container[idx] if 0 <= idx < len(container) else MISSING
    return MISSING


def get_pointer(data, path: str, default=MISSING):
    node = data
    for key in _split_pointer(path):
        node = _step(node, key)
        if node is MISSING:
            return default
    return node


def _parent_and_key(data, path: str):
    parts = _split_pointer(path)
    if not parts:
        return MISSING, None
    parent = get_pointer(data, "/" + "/".join(p.replace("~", "~0").replace("/", "~1") for p in parts[:-1]))
    return parent, parts[-1]


def target_exists(data, path: str) -> bool:
    """A parameter can point at `path` when the value exists - or when it's an object key that
    is simply not set yet (an optional field like an empty serial_number), whose parent exists."""
    if get_pointer(data, path) is not MISSING:
        return True
    parent, _key = _parent_and_key(data, path)
    return isinstance(parent, dict)


def set_pointer(data, path: str, value, create: bool = False) -> bool:
    """Replaces the existing value at `path` (with `create`, also adds a missing object key).
    Returns False (changes nothing) if it can't."""
    parent, last = _parent_and_key(data, path)
    if last is None:
        return False
    if isinstance(parent, dict) and (create or last in parent):
        parent[last] = value
        return True
    if isinstance(parent, list):
        try:
            idx = int(last)
        except ValueError:
            return False
        if 0 <= idx < len(parent):
            parent[idx] = value
            return True
    return False


def delete_pointer(data, path: str) -> bool:
    """Removes an object key. Returns False if there was nothing to remove."""
    parent, last = _parent_and_key(data, path)
    if isinstance(parent, dict) and last in parent:
        del parent[last]
        return True
    return False


def put_pointer(data, path: str, value):
    """Sets `path` to `value`, or removes the key when `value` is MISSING (restoring an
    optional field the profile never set)."""
    if value is MISSING:
        delete_pointer(data, path)
    else:
        set_pointer(data, path, value, create=True)


def schema_field_type(item_schema: dict, field_parts: List[str]) -> Optional[str]:
    """JSON-schema "type" of a field inside an item schema (a source/pipeline factory schema),
    following plain nested "properties". None when unknown - e.g. inside a nested factory."""
    node = item_schema or {}
    for key in field_parts:
        props = node.get("properties") if isinstance(node, dict) else None
        if not isinstance(props, dict) or key not in props:
            return None
        node = props[key]
    field_type = node.get("type") if isinstance(node, dict) else None
    return field_type if isinstance(field_type, str) else None


# Sample values standing in for a field's schema type when the profile has no value to take the
# type from (null, or not set at all).
_SCHEMA_SAMPLES = {"string": "", "integer": 0, "number": 0.0, "boolean": False, "array": [], "object": {}}


# ==========================================
# Coercion + apply
# ==========================================
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def coerce_value(value, current, name: str = "", path: str = "", schema_type: Optional[str] = None):
    """Converts `value` (a CLI string, or a JSON scalar from a params file) to the type of the
    value currently stored at the target path - or, when there is none (null / not set), to the
    field's `schema_type`. Raises ProfileParamError when it can't."""
    where = f"parameter '{name}' ({path})" if name else path
    if (current is None or current is MISSING) and schema_type in _SCHEMA_SAMPLES:
        current = _SCHEMA_SAMPLES[schema_type]

    try:
        if isinstance(current, bool):
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in _TRUE:
                return True
            if text in _FALSE:
                return False
            raise ValueError(f"expected true/false, got '{value}'")

        if isinstance(current, int):
            if isinstance(value, bool):
                raise ValueError("expected an integer, got a boolean")
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError(f"expected an integer, got {value}")
                return int(value)
            return int(str(value).strip(), 0)  # accepts 0x.. too (CAN ids, addresses)

        if isinstance(current, float):
            if isinstance(value, bool):
                raise ValueError("expected a number, got a boolean")
            return float(value)

        if isinstance(current, (dict, list)):
            parsed = json.loads(value) if isinstance(value, str) else value
            if not isinstance(parsed, type(current)):
                raise ValueError(f"expected a JSON {type(current).__name__}")
            return parsed

        if current is None or current is MISSING:
            return value

        # Strings (serial numbers, COM ports): a params file may well write them as numbers.
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    except ProfileParamError:
        raise
    except Exception as e:
        raise ProfileParamError(f"Invalid value for {where}: {e}") from e


def apply_params(data: dict, given: Dict[str, Any], type_of=None) -> Tuple[Dict[str, Tuple[str, Any]], Dict[str, Any]]:
    """Writes `given` parameter values into `data` in place.

    Returns `(bindings, effective)`:
      bindings:  {path: (param_name, original_value)} for every path that was set - original is
                 MISSING for an optional field the profile doesn't set
      effective: {param_name: value} for every given parameter (the first path's coerced value)

    `type_of(data, path)` may name a field's schema type, for fields without a value to take
    the type from. Validates everything before touching `data`, so on ProfileParamError `data`
    is unchanged."""
    if not given:
        return {}, {}

    declarations = get_declarations(data)
    unknown = sorted(set(given) - set(declarations))
    if unknown:
        declared = ", ".join(sorted(declarations)) or "(none - add one with 'blink switch <profile> --add-param')"
        raise ProfileParamError(f"Unknown parameter(s): {', '.join(unknown)}. Declared parameters: {declared}")

    planned = []  # (path, name, original, new)
    for name, value in given.items():
        for path in declarations[name]["paths"]:
            if not target_exists(data, path):
                raise ProfileParamError(f"Parameter '{name}' points at '{path}', which does not exist in the profile")
            current = get_pointer(data, path)
            schema_type = field_type(type_of, data, path, current)
            planned.append((path, name, deepcopy(current), coerce_value(value, current, name, path, schema_type)))

    bindings = {}
    effective = {}
    for path, name, original, new in planned:
        set_pointer(data, path, new, create=True)
        bindings[path] = (name, original)
        effective.setdefault(name, new)
    return bindings, effective


def field_type(type_of, data, path: str, current) -> Optional[str]:
    """Schema type for `path`, only looked up when the current value can't tell (null / unset)."""
    if type_of is None or (current is not None and current is not MISSING):
        return None
    try:
        return type_of(data, path)
    except Exception:
        return None
