---
name: qt-log-table-viewer
description: Use when building or reviewing a Qt (PySide6/qtpy) QAbstractTableModel/QTableView backed by streaming/high-throughput backend data in blinkview (e.g. log viewers, telemetry tables, anything fed by CentralStorage/CircularLogPool). Covers the performance and correctness pitfalls hit while building the table-based log viewer (LogTableViewerWidget) - Qt paint-path traps, Numba/array_pool hot-loop allocation, throttling, bounded live/history data-fetch design, and scroll-signal reentrancy.
---

# Qt table/model views over streaming backend data (blinkview)

Lessons from building `ui/widgets/log_table_viewer.py` as a table-based alternative to
`LogViewerWidget`. Apply these before writing a new `QAbstractTableModel`/`QTableView` pair in
this codebase, or when a Qt table view feels laggy under load.

## 1. Bound your data fetch — don't hold unbounded scrollback

Naively accumulating every row a backend produces (an ever-growing numpy buffer, evicting the
oldest quarter when full) works but wastes memory and CPU on data nobody's looking at. Prefer a
**two-tier design**:

- **Live mode**: fetch only enough of the most-recent matching rows to fill the visible viewport
  (compute this from `view.viewport().height() // row_height`, not a guessed constant). Re-fetch
  whenever the backend's sequence counter advances. No scrollbar needed — there's nothing to
  scroll to.
- **History mode**: entered when the user scrolls away from the live tail. Fetch a bounded window
  of rows (a few hundred) before and after an anchor sequence, in two passes — backward from the
  anchor for "before", forward for "after" — using an early-exit bound so scan cost never depends
  on total backend history size. This window is static (no periodic refresh) until the user
  scrolls near its edges or returns to the live tail.

See `LogTableModel.apply_updates`/`_fetch_live`/`_fetch_history` in `log_table_viewer.py` for the
concrete implementation, and `ops/segments.py`'s `nb_segment_filter_reversed` (backward, with
`start_seq`/`end_seq` bounds) and `filter_segment` (forward, `start_seq` bound) for the kernels.

## 2. Never allocate inside a per-tick hot path

If a model's `apply_updates()`-style method runs on every GUI heartbeat tick (~10-60Hz), any
`np.zeros`/`np.empty` call inside it is a recurring cost, even if each call looks cheap in
isolation. Pre-allocate scratch/permanent arrays once in `__init__`, sized to the model's bound
(viewport rows, history window size, etc.), and write kernel output directly into them using an
`out_row_offset`-style parameter. This mirrors the codebase's existing `array_pool` philosophy
(see how `LogViewerWidget.apply_updates` borrows from `system_ctx.array_pool` instead of
allocating raw arrays).

**Trick for combining reversed + forward scan results without an extra copy**: when scanning
newest-to-oldest across multiple segments but needing the final result oldest-first, write each
segment's rows starting from the *right end* of the output array and decrementing a cursor per
segment, instead of collecting into a list and reversing afterward. The result naturally lands in
chronological order in the tail region `[cursor:capacity]` — see `_fetch_live`'s `write_cursor`.

## 3. Throttle and skip-if-unchanged

Match `LogViewerWidget`'s pattern: throttle heavy per-tick work to ~100ms (`time_ns()` delta
check), and additionally skip the whole fetch if a cheap comparison shows nothing changed (e.g.
`pool.latest_sequence() == self._last_backend_seq`). Do the comparison *before* touching any
Numba kernels or numpy arrays.

## 4. Qt's default item delegate has a hidden per-cell cost

`QStyledItemDelegate::paint()` auto-detects "rich text" (any cell value containing `<`, `>`, `&`)
and lays it out via a full `QTextDocument` if detected — done **per cell, on every repaint of the
visible viewport**, not just newly-changed rows. Log messages routinely contain `<`/`>`
(comparisons, tags, macros), so this silently costs real time on every scroll/insert repaint.

Fix: install a delegate that draws text directly (`painter.drawText(...)`) and hardcodes
`sizeHint()` instead of using font-metric calculation. `TelemetryDelegate`
(`ui/widgets/action_button_delegate.py`) already does this for `TelemetryTable`'s VALUE column;
`LogTablePlainTextDelegate` in `log_table_viewer.py` follows the same pattern. When adding a new
text-heavy table column in this codebase, use this pattern from the start rather than discovering
the cost later via profiling.

## 5. Fixed row height avoids O(row_count) relayout

Leave the vertical header's resize mode at its Qt default and every row insert/change can trigger
Qt recomputing row geometry for the *whole table*, not just the changed rows — a real cost once a
table holds thousands of rows. Set:

```python
v_header = view.verticalHeader()
v_header.setSectionResizeMode(QHeaderView.Fixed)
v_header.setDefaultSectionSize(ROW_HEIGHT)  # e.g. 22
```

`TelemetryTable` already does this; match it for any new table view over streaming data.

## 6. Diagnosing "lag" that isn't in your code

If your own timers (wrap the suspect method in `time_ns()` deltas, log via
`gui_context.logger.child(...)`) show sub-millisecond cost but an app-level lag monitor still
fires, **and the lag disappears when the widget isn't visible**, the cost is in Qt's own
paint/layout machinery (deferred via `QWidget::update()`, runs between your timer ticks, invisible
to Python-side profiling), not in your fetch/model logic. That's the signal to look at delegates,
row-height config, or excessive full-viewport repaints — not to keep optimizing the model.

Add temporary per-phase debug loggers directly on the model (`self.logger = gui_context.logger.child(...)`,
sub-children per phase) rather than guessing blind; this is cheap to add and immediately tells you
which side of the Qt/Python boundary a lag report belongs to.

## 7. Scroll/signal reentrancy: guard programmatic UI changes

Any code path that both (a) reacts to a Qt signal (e.g. `scrollbar.valueChanged`) and (b)
*programmatically* changes the same state the signal reports (e.g. calls `scrollTo()`, or
`beginResetModel()`/`endResetModel()`, both of which can move the scrollbar) is at risk of
infinite reentrant recursion — the programmatic change re-fires the signal, which re-enters the
handler, which changes the state again... This can crash the process outright (stack overflow
through Qt's C++ signal dispatch, not a catchable Python `RecursionError`), rather than just
misbehaving.

Fix: wrap any such programmatic-change sequence in a boolean re-entrancy guard, and check it first
thing in the signal handler:

```python
def _reanchor(self, ...):
    self._programmatic_scroll = True
    try:
        self.model.enter_history_mode(...)   # triggers a model reset -> scrollbar moves
        self._scroll_to(...)                 # explicit scrollTo -> scrollbar moves again
    finally:
        self._programmatic_scroll = False

def _on_scroll_value_changed(self, value):
    if self._programmatic_scroll:
        return
    ...
```

## 8. Don't rely solely on reactive signals for state transitions

A "have we reached the end?" check driven only by `scrollbar.valueChanged` will silently stop
firing once the content shrinks to fit the viewport with nothing left to scroll (the scrollbar's
value stops changing, so the signal never fires again) — even though the condition it was meant
to detect ("we're caught up to live data") is now true. Add an eager check right after any fetch
that could produce this situation (e.g. check "did we just reach the live edge?" immediately after
a history-window re-fetch), rather than only reacting to future scroll events that may never come.

## 9. Testing Qt models without a running app

- Session-scoped `qapp` fixture (`QT_QPA_PLATFORM=offscreen`, one `QApplication` for the test
  session) is enough to construct `QAbstractTableModel`/`QWidget` subclasses in tests.
- Build a `FakeGuiContext`/`FakeRegistry`/`FakeLogPool` (context-manager methods matching
  `CircularLogPool`'s `get_reversed_snapshot`/`get_snapshot`/`acquire_indices_buffer`) rather than
  standing up the real `Registry`/`IDRegistry` machinery, but use the **real** `IDRegistry` for
  device/module lookups (`IDRegistry(NumpyArrayPool())`) since that part is cheap and exercising
  the real resolution logic is worth it.
- For kernel-level tests, build a minimal `LogBundle` by hand (see `make_bundle` helper in
  `tests/test_ops_segments.py` / `tests/test_log_table_viewer.py`) rather than going through the
  real ingestion pipeline.
- `PrintLogger` (from `core.logger`) is a good stand-in for `gui_context.logger` in tests — it has
  no `enabled` kwarg on `__init__` (unlike `SystemLogger`), just construct it plain.

## 10. Known pre-existing gotcha (not a regression)

Importing certain `blinkview.ui.widgets.*` or `blinkview.core.device_identity` modules as the
*very first* touch of that dependency cluster in a fresh process raises `ImportError: cannot
import name 'DeviceIdentity' from partially initialized module` — a circular import between
`core.device_identity` and `core.id_registry.registry` that only avoids tripping when something
else (e.g. `ops.formatting` importing `core.id_registry.types`) happens to warm the `id_registry`
package first. This is pre-existing (reproduces identically on unmodified `telemetry_table.py`),
not something a new widget file introduces. Don't chase it; it doesn't affect the real app entry
point (`ui.main_window`), which always imports fine.
