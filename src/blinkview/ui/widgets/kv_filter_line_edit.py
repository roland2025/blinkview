# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import QTimer, Signal
from qtpy.QtWidgets import QLineEdit

from blinkview.utils.log_filter import LogFilter


class KvFilterLineEdit(QLineEdit):
    """QLineEdit for logfmt `key=value` filter queries, self-contained so LogViewerWidget and
    LogTableViewerWidget don't each duplicate the debounce/readiness plumbing.

    Every keystroke restarts a debounce timer instead of driving a reload directly - each commit
    a listener reacts to triggers a real backend re-fetch and view repaint (not a cheap in-memory
    re-filter like the plain text search box), so firing on every keystroke would visibly
    thrash the table while the user is still typing. Once the timer fires, LogFilter.is_kv_query_ready()
    additionally holds off emitting while the text ends mid-pair (a trailing "key=" with no value
    character yet) - a query in that shape is guaranteed to filter out every row, so committing it
    would just flash the view empty for the fraction of a second before the next character lands."""

    filterTextCommitted = Signal(str)

    DEBOUNCE_MS = 200

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setPlaceholderText('logfmt filter: key=value key2="val 2"...')
        self.setClearButtonEnabled(True)

        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(self.DEBOUNCE_MS)
        self._timer.timeout.connect(self._maybe_commit)

        self.textChanged.connect(lambda _text: self._timer.start())

    def _maybe_commit(self):
        text = self.text()
        if not LogFilter.is_kv_query_ready(text):
            return  # mid-pair (trailing "key=") - wait for a value character; typing will restart us
        self.filterTextCommitted.emit(text)
