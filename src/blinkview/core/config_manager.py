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
    def __init__(self, filepath, autosave_path, default_config=None):
        self.filepath = filepath
        self.autosave_path = autosave_path

        print(
            f"[ConfigManager] Initialized with filepath: {self.filepath}, autosave_path: {self.autosave_path}"
        )

        self._lock = threading.RLock()

        self.default_config = default_config or {}

        self._data = self._load_or_create_default()

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
            return get_by_path(
                self._data, path, default, drop_keys, make_deep_copy, depth
            )

    # ==========================================
    # PUBLIC API: Write Operations
    # ==========================================
    def save_full_config(self, filepath=None):
        with self._lock:
            target = filepath if filepath else self.filepath
            try:
                atomic_json_dump(self._data, target)
            except Exception as e:
                print(f"[ConfigManager] Failed to save {target}: {e}")

    def apply_patch(self, path: str, patch: list):
        """Applies patch and notifies affected subscribers."""
        if not patch:
            return

        with self._lock:
            try:
                # 1. Promote relative paths to absolute paths for the global data
                base_path = "" if path == "/" else path.rstrip("/")
                global_patch = []
                for op in patch:
                    new_op = op.copy()
                    rel_path = new_op["path"]
                    new_op["path"] = (
                        f"{base_path}{rel_path}"
                        if rel_path.startswith("/")
                        else f"{base_path}/{rel_path}"
                    )
                    global_patch.append(new_op)

                # 2. Apply the patch

                import jsonpatch

                self._data = jsonpatch.apply_patch(self._data, global_patch)
                self.save_full_config()

                # print(f"[ConfigManager] FULL CONFIG: {json.dumps(self._data, indent=4)}")  # Debug print after patch application

                # Persistent Mirroring to Session
                self.session_autosave()

                # 3. Notify Subscribers
                self._notify_subscribers(global_patch)

                if self.config_changed_cb is not None:
                    new_config = self.get_by_path(path, make_deep_copy=True)
                    # print(f"[Registry] Calling config_changed_cb for {path} with new_config: {new_config}")
                    schema = (
                        self.get_schema_by_path(path)
                        if self.get_schema_by_path
                        else None
                    )
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
                patch_slashed = (
                    patch_path if patch_path.endswith("/") else patch_path + "/"
                )

                # 1. Did a child of the subscribed path change?
                is_child = patch_slashed.startswith(sub_slashed)
                # 2. Did a parent of the subscribed path change?
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
                        except Exception as e:
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
        config = self.get_by_path(
            path, drop_keys=drop_keys, make_deep_copy=editable, depth=depth
        )
        schema = (
            self.get_schema_by_path(path, drop_keys=drop_keys)
            if self.get_schema_by_path
            else None
        )
        return config, schema

    def get_data(self):
        return self._data
