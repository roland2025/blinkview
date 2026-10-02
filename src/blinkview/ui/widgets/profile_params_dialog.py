# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Main menu -> Profile Parameters...: this instance's profile parameters (core/profile_params.py).

Shows every declared parameter with the value this run uses, the profile's own value and where
the run's value came from (a --params set, --param, or this window). Values can be changed live
(like --param at startup - never saved to the profile) and saved as a parameter set, so the next
`blink -p <profile> --params <set>` starts the same way."""

import json
from typing import Any, Dict, Optional

from qtpy.QtCore import Qt, Signal
from qtpy.QtGui import QColor
from qtpy.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from blinkview.core.profile_params import (
    MISSING,
    ProfileParamError,
    get_declarations,
    get_pointer,
    params_set_path,
    write_params_file,
)
from blinkview.ui.utils.window_title import titled
from blinkview.ui.widgets.message_box import MessageBox

COL_NAME, COL_VALUE, COL_PROFILE, COL_ORIGIN, COL_PATHS = range(5)
PENDING_COLOR = QColor("#d39e00")  # same amber as the config editor's "set by parameter" hint
HEADERS = ["Parameter", "Value (this run)", "Profile value", "Set by", "Config paths"]
CHANGED_HERE = "this window"


def quote_arg(arg: str) -> str:
    """Quotes a command-line argument only when needed, with double quotes - understood by
    cmd.exe, PowerShell and POSIX shells alike (shlex.quote's single quotes aren't by cmd.exe)."""
    if arg and not any(c in arg for c in (" ", "\t", '"')):
        return arg
    return '"' + arg.replace('"', '\\"') + '"'


NOT_SET = "(not set)"


def format_value(value) -> str:
    """Cell text for a config value: strings as-is (serials, ports), everything else as JSON."""
    if value is MISSING or value is None:
        return NOT_SET  # an optional field the profile leaves empty
    return value if isinstance(value, str) else json.dumps(value)


class ProfileParamsDialog(QDialog):
    # Emitted from the worker thread that applied new values; Qt queues it to the UI thread.
    signal_applied = Signal(object)

    def __init__(self, gui_context, parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self.registry = gui_context.registry
        self.config = self.registry.config
        self.fm = self.registry.file_manager

        self.setWindowTitle(titled("Profile Parameters"))
        self.resize(900, 360)

        layout = QVBoxLayout(self)

        self.info_label = QLabel()
        self.info_label.setWordWrap(True)
        self.info_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(self.info_label)

        self.table = QTableWidget(0, len(HEADERS))
        self.table.setHorizontalHeaderLabels(HEADERS)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(
            QAbstractItemView.DoubleClicked | QAbstractItemView.EditKeyPressed | QAbstractItemView.AnyKeyPressed
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        header.setStretchLastSection(True)
        self.table.itemChanged.connect(self._update_buttons)
        layout.addWidget(self.table, 1)

        self.empty_label = QLabel(
            "This profile declares no parameters. Right-click a field's label in a config editor "
            "and choose 'Make profile parameter...'."
        )
        self.empty_label.setWordWrap(True)
        self.empty_label.setStyleSheet("color: #888;")
        layout.addWidget(self.empty_label)

        session_row = QHBoxLayout()
        session_row.addWidget(QLabel("Session name saved with the set:"))
        self.session_edit = QLineEdit()
        self.session_edit.setPlaceholderText("(none - the set name is used)")
        session_row.addWidget(self.session_edit, 1)
        layout.addLayout(session_row)

        buttons = QHBoxLayout()
        self.btn_reset = QPushButton("Use Profile Value")
        self.btn_reset.setToolTip("Clear the selected parameters' values for this run")
        self.btn_reset.clicked.connect(self.reset_selected)
        self.btn_copy = QPushButton("Copy Launch Command")
        self.btn_copy.clicked.connect(self.copy_launch_command)
        self.btn_save = QPushButton("Save to Set")
        self.btn_save.clicked.connect(self.save_to_current_set)
        self.btn_save_as = QPushButton("Save as New Set...")
        self.btn_save_as.clicked.connect(self.save_as_new_set)
        self.btn_apply = QPushButton("Apply")
        self.btn_apply.setToolTip("Use these values in this instance now (not saved to the profile)")
        self.btn_apply.clicked.connect(self.apply)
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)
        for b in (self.btn_reset, self.btn_copy):
            buttons.addWidget(b)
        buttons.addStretch()
        for b in (self.btn_save, self.btn_save_as, self.btn_apply, btn_close):
            buttons.addWidget(b)
        layout.addLayout(buttons)
        # Enter (e.g. right after typing a value) must apply - QDialog would otherwise make the
        # first button ("Use Profile Value") the default and clear the selection instead.
        for b in (self.btn_reset, self.btn_copy, self.btn_save, self.btn_save_as, btn_close):
            b.setAutoDefault(False)
        self.btn_apply.setDefault(True)

        self.signal_applied.connect(self._on_applied)
        self._applying = False

        # Any profile config change (a reload from disk, a newly declared parameter, ...) -
        # queued to the UI thread by Qt. Edits in progress here are never thrown away.
        node_manager = getattr(gui_context, "config_manager", None)
        if node_manager is not None and hasattr(node_manager, "signal_received_config_schema"):
            node_manager.signal_received_config_schema.connect(self._on_config_changed)

        self.refresh()

    def _on_config_changed(self, *_):
        if self.isVisible() and not self._applying and not self.pending_changes():
            self.refresh()

    # ------------------------------------------------------------------ state

    def declarations(self) -> Dict[str, dict]:
        try:
            return get_declarations(self.config.get_by_path("/", make_deep_copy=True))
        except ProfileParamError:
            return {}

    def current_set_file(self):
        """The one params file this instance was started with, if exactly one."""
        files = list(getattr(self.fm, "params_files", []) or [])
        return files[0] if len(files) == 1 else None

    def _profile_value(self, decl: dict):
        path = decl["paths"][0]
        binding = self.config.param_bindings.get(path)
        if binding is not None:
            return binding[1]
        return get_pointer(self.config.get_by_path("/", make_deep_copy=False), path)

    def refresh(self):
        """Rebuilds the table from the running config (e.g. after Apply or a profile reload)."""
        decls = self.declarations()
        origins = getattr(self.fm, "param_origins", {}) or {}

        self.table.blockSignals(True)
        self.table.setRowCount(len(decls))
        for row, (name, decl) in enumerate(sorted(decls.items())):
            bound = name in self.config.param_values
            cells = {
                COL_NAME: name,
                COL_VALUE: format_value(self.config.param_values[name]) if bound else "",
                COL_PROFILE: format_value(self._profile_value(decl)),
                COL_ORIGIN: origins.get(name, "") if bound else "(profile value)",
                COL_PATHS: "\n".join(decl["paths"]),
            }
            for col, text in cells.items():
                item = QTableWidgetItem(text)
                if col != COL_VALUE:
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if col == COL_NAME and decl["description"]:
                    item.setToolTip(decl["description"])
                self.table.setItem(row, col, item)
        self.table.resizeRowsToContents()
        self.table.blockSignals(False)

        self.table.setVisible(bool(decls))
        self.empty_label.setVisible(not decls)

        set_file = self.current_set_file()
        if not self.session_edit.isModified():
            self.session_edit.setText(getattr(self.fm, "params_session", None) or "")

        started = [f"'{p.name}'" for p in getattr(self.fm, "params_files", []) or []]
        parts = [f"Started with parameter file {', '.join(started)}" if started else "Started without --params"]
        # The params file's "Left board" rather than its folder-safe form "Left_board", when that's
        # what named this session.
        session = getattr(self.fm, "params_session", None)
        if not session or self.fm._sanitize(session) != self.fm.session_display_name:
            session = self.fm.session_display_name
        parts.append(f"session '{session}'")
        self.info_label.setText(
            " · ".join(parts) + ".\nValues apply to this instance only and are never written to the profile; "
            "save them as a set to start with them next time."
        )
        self.btn_save.setText(f"Save to '{set_file.name}'" if set_file else "Save to Set")
        self._update_buttons()

    def pending_changes(self) -> Dict[str, Optional[str]]:
        """{name: new text, or None = back to the profile value} for edited rows."""
        changes = {}
        for row in range(self.table.rowCount()):
            name = self.table.item(row, COL_NAME).text()
            text = self.table.item(row, COL_VALUE).text().strip()
            bound = name in self.config.param_values
            if not text:
                if bound:
                    changes[name] = None
            elif not bound or text != format_value(self.config.param_values[name]):
                changes[name] = text
        return changes

    def _update_buttons(self, *_):
        self._mark_pending()
        has_rows = self.table.rowCount() > 0
        self.btn_apply.setEnabled(bool(self.pending_changes()) and not self._applying)
        self.btn_reset.setEnabled(has_rows)
        self.btn_save.setEnabled(self.current_set_file() is not None)
        self.btn_save_as.setEnabled(has_rows)
        self.btn_copy.setEnabled(has_rows)

    def _mark_pending(self):
        """Edited-but-not-applied values in amber, with "(not applied)" as their origin."""
        pending = self.pending_changes()
        origins = getattr(self.fm, "param_origins", {}) or {}
        self.table.blockSignals(True)
        for row in range(self.table.rowCount()):
            name = self.table.item(row, COL_NAME).text()
            value_item = self.table.item(row, COL_VALUE)
            origin_item = self.table.item(row, COL_ORIGIN)
            if name in pending:
                value_item.setForeground(PENDING_COLOR)
                origin_item.setText("(not applied)")
            else:
                value_item.setData(Qt.ForegroundRole, None)
                bound = name in self.config.param_values
                origin_item.setText(origins.get(name, "") if bound else "(profile value)")
        self.table.blockSignals(False)

    # ------------------------------------------------------------------ actions

    def reset_selected(self):
        rows = {i.row() for i in self.table.selectedItems()}
        for row in rows:
            self.table.item(row, COL_VALUE).setText("")

    def apply(self):
        """Validates on the UI thread, then applies in the background (sources may reconnect).
        Returns the task's future (None if there was nothing to do or it was invalid)."""
        changes = self.pending_changes()
        if not changes or self._applying:
            return None
        try:
            self.config.set_param_values(changes, dry_run=True)
        except ProfileParamError as e:
            MessageBox.warning(self, "Profile Parameters", str(e))
            return None

        self._applying = True
        self._update_buttons()

        def task():
            try:
                values = self.config.set_param_values(changes)
                origins = dict(getattr(self.fm, "param_origins", {}) or {})
                for name, value in changes.items():
                    if value is None:
                        origins.pop(name, None)
                    else:
                        origins[name] = CHANGED_HERE
                self.fm.record_params(
                    values,
                    getattr(self.fm, "params_files", []),
                    getattr(self.fm, "params_label", None),
                    getattr(self.fm, "params_session", None),
                    origins,
                )
                self.signal_applied.emit(None)
            except Exception as e:  # surfaced on the UI thread
                self.signal_applied.emit(e)

        return self.registry.system_ctx.tasks.run_task(task)

    def _on_applied(self, error):
        self._applying = False
        if error is not None:
            MessageBox.warning(self, "Profile Parameters", f"Could not apply: {error}")
        self.refresh()

    def values_to_save(self) -> Dict[str, Any]:
        """What a saved set contains: every parameter that has a value in the table (pending
        edits included, coerced to the fields' types). Raises ProfileParamError."""
        changes = self.pending_changes()
        effective = self.config.set_param_values(changes, dry_run=True) if changes else dict(self.config.param_values)
        return effective

    def _write_set(self, path) -> bool:
        try:
            values = self.values_to_save()
        except ProfileParamError as e:
            MessageBox.warning(self, "Profile Parameters", str(e))
            return False
        if not values:
            MessageBox.warning(self, "Profile Parameters", "No parameter has a value - nothing to save.")
            return False
        session = self.session_edit.text().strip() or None
        write_params_file(path, values, session)
        return True

    def save_to_current_set(self) -> bool:
        path = self.current_set_file()
        if path is None:
            return False
        return self._write_set(path)

    def _ask_set_name(self, default: str):
        """(name, ok) - a seam for tests, like QInputDialog.getText."""
        return QInputDialog.getText(self, titled("Save Parameter Set"), "Parameter set name:", text=default)

    def _confirm_overwrite(self, path) -> bool:
        answer = MessageBox.question(self, "Save Parameter Set", f"'{path.name}' already exists. Overwrite it?")
        return answer == MessageBox.Btn.Yes

    def save_as_new_set(self) -> Optional[str]:
        name, ok = self._ask_set_name(getattr(self.fm, "params_label", None) or "")
        name = (name or "").strip()
        if not ok or not name:
            return None
        if not name.replace("_", "").replace("-", "").isalnum():
            MessageBox.warning(self, "Profile Parameters", "Use letters, digits, '_' and '-' for a set name.")
            return None
        path = params_set_path(self.fm.config_dir, self.fm.config_file_name, name)
        if path.exists() and not self._confirm_overwrite(path):
            return None
        return name if self._write_set(path) else None

    def launch_command(self) -> str:
        """`blink -p <profile> --params <set>` while every value still comes from the one file
        this instance was started with, else the values as explicit --param arguments."""
        provided = getattr(self.fm, "provided_config_path", None)
        target = f"-c {quote_arg(str(provided))}" if provided else f"-p {self.fm.profile_name}"

        set_file = self.current_set_file()
        origins = getattr(self.fm, "param_origins", {}) or {}
        if set_file is not None and self.config.param_values and not self.pending_changes():
            file_origins = {f"set '{getattr(self.fm, 'params_label', None)}'", f"file '{set_file.name}'"}
            if all(origins.get(name) in file_origins for name in self.config.param_values):
                is_named_set = set_file == params_set_path(
                    self.fm.config_dir, self.fm.config_file_name, getattr(self.fm, "params_label", "")
                )
                spec = self.fm.params_label if is_named_set else quote_arg(str(set_file))
                return f"blink {target} --params {spec}"

        try:
            values = self.values_to_save()
        except ProfileParamError:
            values = dict(self.config.param_values)
        args = " ".join(f"--param {quote_arg(f'{k}={format_value(v)}')}" for k, v in sorted(values.items()))
        return f"blink {target} {args}".rstrip()

    def copy_launch_command(self):
        QApplication.clipboard().setText(self.launch_command())
