# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

from blinkview.utils.atomic_json_dump import atomic_json_dump


class ConfigManager:
    def __init__(self, filepath, autosave_path, default_config=None, params: Dict[str, Any] = None, param_type_of=None):
        self.filepath = filepath
        self.autosave_path = autosave_path

        print(f"[ConfigManager] Initialized with filepath: {self.filepath}, autosave_path: {self.autosave_path}")

        self._lock = threading.RLock()

        self.default_config = default_config or {}

        self._data = self._load_or_create_default()

        # What this instance last read from / wrote to the profile file. Another BlinkView
        # instance running the same profile (or a text editor) may change it - see
        # poll_external_change() / reload_from_disk().
        self._disk_data = self._read_disk()
        self._disk_sig = self._disk_signature()
        self._external_pending = False

        # Profile parameters (--param/--params, see core/profile_params.py) live only in _data:
        # {path: (param_name, value_stored_in_the_profile)}. The profile file always keeps the
        # stored value - see _persistable() / _persist_patch(). Raises ProfileParamError.
        self.param_bindings: Dict[str, tuple] = {}
        self.param_values: Dict[str, Any] = {}
        # (data, path) -> JSON-schema type, for parameters on fields without a value (see
        # profile_params.field_type). Registry passes one backed by the factory schemas.
        self.param_type_of = param_type_of
        if params:
            from blinkview.core.profile_params import apply_params

            self.param_bindings, self.param_values = apply_params(self._data, params, param_type_of)

        # Maps path -> list of callback functions
        # e.g., {"/plugins": [on_plugin_change], "/devices/ABC": [on_abc_change]}
        self._subscriptions: Dict[str, List] = {}

        self.config_changed_cb = None  # Optional global callback for any config change
        self.get_schema_by_path = None  #

    def session_autosave(self):
        """Points the autosave to the current session folder."""
        self.save_full_config(self.autosave_path)

    def subscribe(self, path: str, callback):
        """Registers a component to be notified when a specific path changes."""

        if not hasattr(callback, "apply_config"):
            raise ValueError("Callback must have an 'apply_config' method.")

        with self._lock:
            if path not in self._subscriptions:
                self._subscriptions[path] = []
            if callback not in self._subscriptions[path]:
                self._subscriptions[path].append(callback)

    def unsubscribe(self, path: str, callback):
        """Removes a component subscription."""
        with self._lock:
            if path in self._subscriptions and callback in self._subscriptions[path]:
                self._subscriptions[path].remove(callback)

    def _load_or_create_default(self) -> dict:
        """Reads the JSON or generates a safe fallback template if missing/corrupt."""
        if self.filepath.exists():
            try:
                with open(self.filepath) as f:
                    return json.load(f)
            except Exception as e:
                print(f"[ConfigManager] Error: {e}")

        return self.default_config

    # ==========================================
    # PUBLIC API: Read Operations
    # ==========================================
    def get_device_names(self) -> List[str]:
        with self._lock:
            return list(self._data.get("devices", {}).keys())

    def get_device_config(self, device_name: str) -> Dict[str, Any]:
        with self._lock:
            return deepcopy(self._data.get("devices", {}).get(device_name, {}))

    def get_plugins(self) -> List[str]:
        with self._lock:
            return self._data.get("plugins", [])

    def get_reorder_config(self):
        with self._lock:
            return self._data.get("reorder", {"enabled": True})

    def get_central_storage_config(self):
        with self._lock:
            return self._data.get("central", {"enabled": True})

    def get_full_config(self) -> dict:
        with self._lock:
            return deepcopy(self._data)

    def get_by_path(
        self,
        path: str,
        default=None,
        drop_keys: list = None,
        make_deep_copy: bool = False,
        depth: int = None,
    ):
        from blinkview.utils.dict_utils import get_by_path

        with self._lock:
            return get_by_path(self._data, path, default, drop_keys, make_deep_copy, depth)

    # ==========================================
    # PUBLIC API: Write Operations
    # ==========================================
    def save_full_config(self, filepath=None):
        """Writes the whole config. The profile file itself (no `filepath`) gets the profile's own
        parameter defaults; any other target (session .start/.autosave/.final snapshots) gets the
        effective values of this run, so a replay shows e.g. the real serial number."""
        with self._lock:
            target = filepath if filepath else self.filepath
            try:
                if Path(target) == Path(self.filepath):
                    self._write_profile(self._persistable())
                else:
                    atomic_json_dump(self._data, target)
            except Exception as e:
                print(f"[ConfigManager] Failed to save {target}: {e}")

    def apply_patch(self, path: str, patch: list):
        """Applies patch and notifies affected subscribers."""
        print(f"[ConfigManager] Patching {path} with '{patch}'")
        if not patch:
            return

        with self._lock:
            try:
                # Promote relative paths to absolute paths for the global data
                base_path = "" if path == "/" else path.rstrip("/")
                global_patch = []

                for op in patch:
                    new_op = op.copy()

                    # We must prefix BOTH 'path' and 'from' (for move/copy ops)
                    for key in ("path", "from"):
                        if key in new_op:
                            rel_path = new_op[key]
                            if rel_path == "":
                                # If relative path is empty, it points exactly to the base path
                                new_op[key] = base_path if base_path != "" else "/"
                            elif rel_path.startswith("/"):
                                new_op[key] = f"{base_path}{rel_path}"
                            else:
                                new_op[key] = f"{base_path}/{rel_path}"

                    global_patch.append(new_op)

                # Apply the patch

                import jsonpatch

                self._data = jsonpatch.apply_patch(self._data, global_patch)
                try:
                    self._persist_patch(global_patch)
                except Exception as e:
                    print(f"[ConfigManager] Failed to save {self.filepath}: {e}")

                # print(f"[ConfigManager] FULL CONFIG: {json.dumps(self._data, indent=4)}")  # Debug print after patch application

                # Persistent Mirroring to Session
                self.session_autosave()

                # Notify Subscribers
                self._notify_subscribers(global_patch)

                if self.config_changed_cb is not None:
                    new_config = self.get_by_path(path, make_deep_copy=True)
                    # print(f"[Registry] Calling config_changed_cb for {path} with new_config: {new_config}")
                    schema = self.get_schema_by_path(path) if self.get_schema_by_path else None
                    self.config_changed_cb(path, new_config, schema)

            except Exception as e:
                print(f"[ConfigManager] Error applying patch: {e}")

    def _notify_subscribers(self, global_patch: list):
        """Checks which subscribed paths were touched by the patch operations."""
        from blinkview.utils.dict_utils import get_by_path

        # Get all paths affected by this patch
        affected_paths = {op["path"] for op in global_patch}

        current_subscriptions = list(
            self._subscriptions.items()
        )  # Snapshot to avoid issues if subscriptions change during iteration

        for sub_path, callbacks in current_subscriptions:
            # Enforce trailing slashes to prevent substring false-positives
            # e.g., "/devices/A" vs "/devices/ABC"
            sub_slashed = sub_path if sub_path.endswith("/") else sub_path + "/"

            should_notify = False
            for patch_path in affected_paths:
                patch_slashed = patch_path if patch_path.endswith("/") else patch_path + "/"

                # Did a child of the subscribed path change?
                is_child = patch_slashed.startswith(sub_slashed)
                # Did a parent of the subscribed path change?
                is_parent = sub_slashed.startswith(patch_slashed)

                if is_child or is_parent or sub_path == "/":
                    should_notify = True
                    break  # We found a match, stop checking paths for this subscriber

            if should_notify:
                # Extract directly from self._data safely
                new_val = get_by_path(self._data, sub_path, make_deep_copy=True)

                for cb in callbacks:
                    try:
                        hydrated = new_val
                        try:
                            hydrated = cb.hydrate_config(new_val)
                        except Exception:
                            pass

                        # Bonus: You might want to pass the global_patch to the callback
                        # so the component knows exactly what changed!

                        cb.apply_config(hydrated)

                        needs_restart = getattr(cb, "thread_needs_restart", False)
                        if needs_restart:
                            print(
                                f"[ConfigManager] Note: '{cb.__class__.__name__}' indicated it needs a thread restart after config change."
                            )
                            cb.restart()

                    except Exception as e:
                        print(f"[ConfigManager] Callback error for {sub_path}: {e}")

    def get_sub_file_path(self, sub: str) -> Path:
        """Returns a Path object for a sub-file in the same directory as the main config. Filename is derived from the main config name. E.g., if main config is 'blink_config.json' and name is 'devices', returns 'blink_config_devices.json'."""
        base_name = self.filepath.stem  # e.g., 'blink_config'
        new_name = f"{base_name}_{sub}.json"  # e.g., 'blink_config_devices.json'
        return self.filepath.parent / new_name

    def get_config_schema(
        self,
        path: str,
        drop_keys: list = None,
        editable: bool = True,
        depth: int = None,
    ):
        config = self.get_by_path(path, drop_keys=drop_keys, make_deep_copy=editable, depth=depth)
        schema = self.get_schema_by_path(path, drop_keys=drop_keys) if self.get_schema_by_path else None
        return config, schema

    def get_data(self):
        return self._data

    def get_param_binding(self, path: str):
        """Name of the --param bound to `path` for this run, or None."""
        binding = self.param_bindings.get(path.rstrip("/") or "/")
        return binding[0] if binding else None

    # ==========================================
    # Profile file persistence
    # ==========================================
    def _read_disk(self):
        """The profile file as currently on disk, or None if missing/unreadable."""
        try:
            if self.filepath.exists():
                with open(self.filepath) as f:
                    return json.load(f)
        except Exception as e:
            print(f"[ConfigManager] Could not re-read {self.filepath}: {e}")
        return None

    def _restore_bound(self, target: dict, source=None):
        """Puts the profile's own values back at every parameter-bound path of `target` - from
        `source` (the on-disk profile) where it has the path, else the value the parameter
        replaced at startup."""
        from blinkview.core.profile_params import MISSING, get_pointer, put_pointer, target_exists

        for path, (_name, original) in self.param_bindings.items():
            if not target_exists(target, path):
                continue  # the whole parent is gone - nothing to restore into
            value = get_pointer(source, path) if source is not None else MISSING
            if source is not None and value is MISSING and target_exists(source, path):
                put_pointer(target, path, MISSING)  # the profile doesn't set this field
            else:
                put_pointer(target, path, deepcopy(original) if value is MISSING else deepcopy(value))

    def _persistable(self) -> dict:
        """_data as it should be written to the profile file (parameter values swapped out)."""
        data = deepcopy(self._data)
        self._restore_bound(data)
        return data

    def _persist_patch(self, global_patch: list):
        """Saves one edit to the profile file by applying `global_patch` to the file's *current*
        content rather than dumping _data - another BlinkView instance running the same profile
        (e.g. a second board via --params) may have saved its own edits since we loaded it, and
        a whole-dict dump would silently revert them."""
        import jsonpatch

        disk = self._read_disk()
        if disk is not None:
            if disk != self._disk_data:
                # Someone else changed the file and we haven't reloaded yet. Our write below
                # merges into their content, after which the file matches what we last wrote -
                # so remember the change here or poll_external_change() would never report it.
                self._external_pending = True
            try:
                # deepcopy: jsonpatch inserts the op's value objects as-is, and the same objects
                # already live in _data - _restore_bound below must not touch those.
                updated = jsonpatch.apply_patch(disk, deepcopy(global_patch))
                self._restore_bound(updated, disk)
                self._write_profile(updated)
                return
            except Exception as e:
                print(f"[ConfigManager] Patch does not apply to {self.filepath} on disk ({e}); writing full config")
        self._write_profile(self._persistable())

    def _write_profile(self, data: dict):
        atomic_json_dump(data, self.filepath)
        self._disk_data = deepcopy(data)
        # Not our own stat(): another instance may write between our write and a stat() here.
        # A cleared signature makes the next poll re-read and compare the content instead.
        self._disk_sig = None

    def _disk_signature(self):
        try:
            st = Path(self.filepath).stat()
            return st.st_mtime_ns, st.st_size
        except OSError:
            return None

    # ==========================================
    # External changes (another instance / an editor)
    # ==========================================
    def poll_external_change(self) -> bool:
        """True if the profile file now differs from what this instance last read or wrote -
        i.e. someone else saved it. Cheap when nothing changed (one stat()). Keeps returning
        True until reload_from_disk() is called."""
        with self._lock:
            if self._external_pending:
                return True
            sig = self._disk_signature()
            if sig is None or sig == self._disk_sig:
                return False
            disk = self._read_disk()
            if disk is None:  # mid-write / unreadable: look again next time
                return False
            self._disk_sig = sig
            if disk != self._disk_data:
                self._external_pending = True
            return self._external_pending

    def reload_from_disk(self) -> bool:
        """Re-reads the profile file and applies whatever changed to the running system, like
        an edit would (subscribers, session autosave, config_changed_cb). This run's --param
        values stay in effect. Returns True if anything changed."""
        import jsonpatch

        from blinkview.core.profile_params import get_pointer, set_pointer, target_exists

        with self._lock:
            disk = self._read_disk()
            if disk is None:
                print(f"[ConfigManager] Reload skipped: cannot read {self.filepath}")
                return False

            effective = deepcopy(disk)
            bindings = {}
            for path, (name, _old_profile_value) in self.param_bindings.items():
                if not target_exists(disk, path):
                    print(f"[ConfigManager] Parameter '{name}': '{path}' no longer exists in the profile")
                    continue
                profile_value = get_pointer(disk, path)  # MISSING: an optional field left unset
                set_pointer(effective, path, deepcopy(get_pointer(self._data, path)), create=True)
                bindings[path] = (name, deepcopy(profile_value))

            self.param_bindings = bindings
            self._disk_data = deepcopy(disk)
            self._disk_sig = self._disk_signature()
            self._external_pending = False

            patch = jsonpatch.make_patch(self._data, effective).patch
            if not patch:
                return False

            print(f"[ConfigManager] Reloaded {self.filepath} from disk ({len(patch)} change(s))")
            self._data = effective
            self.session_autosave()
            self._notify_subscribers(patch)

            if self.config_changed_cb is not None:
                # "/" makes every open config node re-fetch (ConfigNode.recv_config_schema).
                schema = self.get_schema_by_path("/") if self.get_schema_by_path else None
                self.config_changed_cb("/", self.get_by_path("/", make_deep_copy=True), schema)
            return True

    def set_param_values(self, values: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
        """Changes this run's parameter values live, like --param would have at startup -
        `None` puts a parameter back to the profile's own value. Never saved to the profile.
        Validates everything first (ProfileParamError, nothing changed). Returns the new
        effective values. `dry_run` only validates - cheap enough for the UI thread, while the
        real call may restart sources/pipelines."""
        import jsonpatch

        from blinkview.core.profile_params import (
            MISSING,
            ProfileParamError,
            coerce_value,
            field_type,
            get_declarations,
            get_pointer,
            target_exists,
        )

        with self._lock:
            declarations = get_declarations(self._data)
            unknown = sorted(set(values) - set(declarations))
            if unknown:
                raise ProfileParamError(f"Unknown parameter(s): {', '.join(unknown)}")

            bindings = dict(self.param_bindings)
            effective = dict(self.param_values)
            ops = []
            for name, value in values.items():
                for path in declarations[name]["paths"]:
                    if not target_exists(self._data, path):
                        raise ProfileParamError(f"Parameter '{name}' points at '{path}', which does not exist")
                    current = get_pointer(self._data, path)  # MISSING: optional field left unset
                    if value is None:
                        if path in bindings:
                            _, profile_value = bindings.pop(path)
                            if profile_value is MISSING:
                                if current is not MISSING:
                                    ops.append({"op": "remove", "path": path})
                            else:
                                ops.append({"op": "add", "path": path, "value": deepcopy(profile_value)})
                        continue
                    schema_type = field_type(self.param_type_of, self._data, path, current)
                    new = coerce_value(value, current, name, path, schema_type)
                    bindings.setdefault(path, (name, deepcopy(current)))
                    if path == declarations[name]["paths"][0]:
                        effective[name] = new
                    if current is MISSING or new != current:
                        ops.append({"op": "add", "path": path, "value": new})  # add = set or create
                if value is None:
                    effective.pop(name, None)

            if dry_run:
                return dict(effective)
            self.param_bindings = bindings
            self.param_values = effective
            if ops:
                self._data = jsonpatch.apply_patch(self._data, ops)
                self.session_autosave()
                self._notify_subscribers(ops)
                if self.config_changed_cb is not None:
                    schema = self.get_schema_by_path("/") if self.get_schema_by_path else None
                    self.config_changed_cb("/", self.get_by_path("/", make_deep_copy=True), schema)
            return dict(effective)

    # ==========================================
    # Parameter declarations ("Make profile parameter" in the config editor)
    # ==========================================
    def get_param_declaration(self, path: str):
        """Name of the profile parameter declared for `path` (whether or not this run sets it)."""
        from blinkview.core.profile_params import ProfileParamError, get_declarations

        path = path.rstrip("/") or "/"
        try:
            declarations = get_declarations(self._data)
        except ProfileParamError:
            return None
        for name, decl in declarations.items():
            if path in decl["paths"]:
                return name
        return None

    def declare_param(self, name: str, path: str, description: str = None):
        """Declares profile parameter `name` for `path` and saves it. Raises ProfileParamError
        for a bad name/path - nothing is saved then."""
        from blinkview.core.profile_params import PARAMS_KEY, add_declaration

        with self._lock:
            scratch = dict(self._data)  # add_declaration only writes scratch[PARAMS_KEY]
            scratch[PARAMS_KEY] = deepcopy(self._data.get(PARAMS_KEY) or {})
            add_declaration(scratch, name, path, description)
            self.apply_patch("/", [{"op": "add", "path": f"/{PARAMS_KEY}", "value": scratch[PARAMS_KEY]}])

    def remove_param(self, path: str):
        """Unbinds `path` from whichever parameter declares it (dropping the parameter when it
        has no paths left) and saves. This run's value, if any, stays until restart."""
        from blinkview.core.profile_params import PARAMS_KEY, get_declarations

        with self._lock:
            path = path.rstrip("/") or "/"
            params = deepcopy(self._data.get(PARAMS_KEY) or {})
            for name, decl in get_declarations(self._data).items():
                if path not in decl["paths"]:
                    continue
                remaining = [p for p in decl["paths"] if p != path]
                if not remaining:
                    params.pop(name, None)
                    continue
                params[name] = {"paths": remaining}
                if decl["description"]:
                    params[name]["description"] = decl["description"]
            self.apply_patch("/", [{"op": "add", "path": f"/{PARAMS_KEY}", "value": params}])
