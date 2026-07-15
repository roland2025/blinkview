---
name: qt-log-table-viewer
description: Use when building or reviewing a Qt (PySide6/qtpy) table/grid view backed by streaming/high-throughput backend data in blinkview (e.g. log viewers, telemetry tables, anything fed by CentralStorage/CircularLogPool) - whether QAbstractTableModel/QTableView or a direct-paint QAbstractScrollArea. Covers the performance and correctness pitfalls hit while building the table-based log viewer (LogTableViewerWidget) - Qt paint-path traps (including QAbstractItemView's always-repaints-the-whole-viewport behavior that motivated moving off it entirely), Numba/array_pool hot-loop allocation, throttling, bounded live/history data-fetch design, scroll-signal reentrancy, debounced backend-driven filter fields, tiered column auto-sizing, and a direct-paint QAbstractScrollArea design (LogTableCanvas/LogTableStore) with seq-based selection.
---

# Qt table/model views over streaming backend data (blinkview)

Lessons from building `ui/widgets/log_table_viewer.py` as a table-based alternative to
`LogViewerWidget`. Apply these before writing a new `QAbstractTableModel`/`QTableView` pair in
this codebase, or when a Qt table view feels laggy under load.

**Architecture note**: `log_table_viewer.py` no longer uses `QAbstractTableModel`/`QTableView` at
all - §6's empirical finding that `QAbstractItemView` always repaints its *entire* viewport on any
content change (no partial-paint path exists, regardless of mechanism) led to replacing it with a
direct-paint `QAbstractScrollArea` subclass (`LogTableCanvas`) painting straight from a plain
Python row store (`LogTableStore`, no longer a `QObject`). See §14 for that design. §§1-3, 5, 7, 9
below still apply as-is (bounded fetch design, no-alloc hot path, throttling, fixed row height
reasoning, diagnosing paint-vs-model lag). §§4, 6, 8, 11, 12 describe the superseded
`QTableView`/delegate/proxy-based approach - the *lessons about Qt's own behavior* in them are
still correct and apply to any *other* `QTableView`-based widget in this codebase (e.g.
`TelemetryTable`), but they no longer describe what `log_table_viewer.py` actually does.

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

## 4. Qt's default item delegate has a hidden per-cell cost — but don't over-correct `sizeHint()` (superseded in log_table_viewer.py - see §14)

`QStyledItemDelegate::paint()` auto-detects "rich text" (any cell value containing `<`, `>`, `&`)
and lays it out via a full `QTextDocument` if detected — done **per cell, on every repaint of the
visible viewport**, not just newly-changed rows. Log messages routinely contain `<`/`>`
(comparisons, tags, macros), so this silently costs real time on every scroll/insert repaint.

Fix: install a delegate that draws text directly (`painter.drawText(...)`). `TelemetryDelegate`
(`ui/widgets/action_button_delegate.py`) already does this for `TelemetryTable`'s VALUE column;
`LogTablePlainTextDelegate` in `log_table_viewer.py` follows the same pattern. When adding a new
text-heavy table column in this codebase, use this pattern from the start rather than discovering
the cost later via profiling.

**Don't extend that fix to `sizeHint()` too.** An earlier version of `LogTablePlainTextDelegate`
also hardcoded `sizeHint()` to a fixed `QSize(50, ROW_HEIGHT)`, reasoning "row height is fixed
anyway, skip the font-metric cost." That broke double-click-to-resize-to-contents on the header
border — Qt calls `sizeHint()` (not `paint()`) to compute that width, so every column just
snapped to the same 50px placeholder instead of fitting its actual text. `sizeHint()` runs far
less often than `paint()` (only on an explicit resize request or initial layout, not every
repaint), so a real `QFontMetrics(...).horizontalAdvance(text)` measurement there is cheap enough
— it's specifically the paint-path's per-repaint, per-cell `QTextDocument` layout that's
expensive, not sizeHint()'s occasional font-metric call. Only the row *height* half of
`sizeHint()`'s return value should stay a fixed constant (the vertical header's own
`setDefaultSectionSize` already governs actual row height when its resize mode is `Fixed` — see
§5 — so `sizeHint()`'s height component is close to unused; its width component is what "resize
to contents" actually reads).

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

## 6. Never call beginResetModel()/endResetModel() from a per-tick hot path (superseded in log_table_viewer.py - see §14)

`LogTableModel`'s live-mode fetch originally called `beginResetModel()`/`endResetModel()` on
*every* successful tick (~10Hz), even the cheap incremental path (§1) that only ever adds a
handful of rows. A full reset forces Qt to drop all selection/persistent indices and fully
relayout+repaint the entire visible viewport on every call, regardless of how few rows actually
changed - this was the real source of "high CPU during live mode, looks like Qt drawing" reports,
not the fetch/kernel logic itself (confirmed via §5's technique: the cost disappeared from the
model's own timers but showed up as sustained CPU only while the widget was visible).

Fix: use `beginInsertRows`/`beginRemoveRows` (bracketing state mutation exactly like the reset
calls did) instead, so Qt only invalidates the rows that actually changed. This works even with a
ping-ponged double-buffer design (§1) where the underlying array object changes identity every
tick - Qt's row-insert/remove signals only care that `rowCount()` is consistent at each
begin/end bracket, not about where the backing data physically lives. For a tick that both evicts
oldest rows (buffer was already at capacity) and appends new ones, do it as two brackets in
sequence - remove first (shifts remaining rows' persistent indices down for free), then insert:

```python
if evicted > 0:
    self.beginRemoveRows(QModelIndex(), 0, evicted - 1)
    # ... flip to the buffer holding only the carried-forward rows, update row_count to carry_count ...
    self.endRemoveRows()

self.beginInsertRows(QModelIndex(), carry_count, new_row_count - 1)
# ... update row_count to new_row_count ...
self.endInsertRows()
```

Also skip the flip and all signals entirely when nothing actually changed (e.g. the backend
sequence advanced but no new row matched the current filter) - don't even do a no-op buffer swap.

Gotcha: if any UI code hooks `modelReset` for side effects (e.g. §12's throttled column
auto-sizing), switching a hot path off `beginResetModel` means that signal stops firing on that
path - hook `rowsInserted` (and `rowsRemoved` if relevant) too, or the side effect silently stops
happening during steady-state incremental ticks while still working on the rarer full-reset paths
(first fetch, filter change, history mode).

**Don't reach for `view.setUpdatesEnabled(False)`/`True` to "batch" a tick's remove+insert
brackets - it makes this specific case worse, not better.** Tried immediately after the fix above,
on the theory that suppressing repaints across both brackets would coalesce two partial-region
repaints into one. Measured effect was the opposite: `setUpdatesEnabled(True)` does not replay the
specific dirty rects that were suppressed while updates were off - per Qt's own docs, re-enabling
after any suppressed paint forces "the widget... to redraw itself in full". So every tick went
from "repaint only the actual dirty rows via the remove/insert brackets' own partial invalidation"
to "unconditionally repaint the entire viewport", strictly more work per tick, not less. This
pattern (`setUpdatesEnabled(False)` around a batch of item-view row mutations) is a reasonable Qt
idiom in general, but it's a net loss specifically when the underlying signals you're wrapping
*already* do fine-grained partial invalidation (as `beginInsertRows`/`beginRemoveRows` do) - it
only pays off when the alternative is many separate *full* repaints, which isn't the case here.

**`QAbstractItemView` has no partial-repaint path at all - every content-affecting operation
repaints the *entire* viewport, regardless of mechanism.** Tried (and reverted) an "over-fetch a
small margin beyond the viewport, never evict on a normal tick, and `scrollToBottom()` every live
tick instead" scheme, on the theory that Qt's native scrolling would blit the retained region and
only repaint the newly-exposed strip - cheaper than a structural `beginRemoveRows` invalidating a
row. That theory is wrong for `QAbstractItemView`/`QTableView`, and it's worth verifying directly
rather than trusting the assumption, since it isn't documented anywhere obvious. Instrument a
throwaway offscreen `QTableView` (`QT_QPA_PLATFORM=offscreen`) with a `QObject` event filter on
`view.viewport()` logging `QEvent.Paint` events' `event.rect()`, then compare a single-row
`beginInsertRows`, a `beginRemoveRows`+`beginInsertRows` pair, and `scrollToBottom()` (in both
`ScrollPerItem` and `ScrollPerPixel` vertical scroll modes) - all four report the *full* viewport
rect as dirty, every time, no matter how small the actual change. There is no cheaper mechanism to
reach for here: `beginInsertRows`/`beginRemoveRows` (§ above) is still worth keeping over
`beginResetModel` for the *non-paint* costs it avoids (selection/persistent-index churn, deferred
full geometry recompute), but it buys nothing on the repaint side, and schemes built on the
assumption that it would (the margin+scroll approach, or reaching for
`view.doItemsLayout()`/`setVerticalScrollMode` to try to unlock a blit path) are chasing a cost
that isn't actually avoidable that way.

**The actual lever once repaint is confirmed viewport-wide: throttle interval scaled to
`viewport_rows`, not a flat rate.** Since repaint cost per tick is proportional to how many rows
are currently visible (not how many rows changed), a flat ~10Hz live-mode throttle is fine for a
small/default window but translates directly into sustained high CPU once the window is maximized
and several hundred rows are visible - "efficient on a small screen, pegs a core when maximized"
is the tell that this is the mechanism in play, not a logic bug. Fix is a throttle interval that
grows with `viewport_rows` (e.g. flat up to a baseline row count, then scaled proportionally
beyond it) so total repainted-rows-per-second stays roughly bounded regardless of window size,
rather than trying to make each individual repaint cheaper.

## 7. Diagnosing "lag" that isn't in your code

If your own timers (wrap the suspect method in `time_ns()` deltas, log via
`gui_context.logger.child(...)`) show sub-millisecond cost but an app-level lag monitor still
fires, **and the lag disappears when the widget isn't visible**, the cost is in Qt's own
paint/layout machinery (deferred via `QWidget::update()`, runs between your timer ticks, invisible
to Python-side profiling), not in your fetch/model logic. That's the signal to look at delegates,
row-height config, or excessive full-viewport repaints — not to keep optimizing the model.

Add temporary per-phase debug loggers directly on the model (`self.logger = gui_context.logger.child(...)`,
sub-children per phase) rather than guessing blind; this is cheap to add and immediately tells you
which side of the Qt/Python boundary a lag report belongs to.

## 8. Scroll/signal reentrancy: guard programmatic UI changes (pattern still applies in log_table_viewer.py's new widget - see §14)

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

## 9. Don't rely solely on reactive signals for state transitions

A "have we reached the end?" check driven only by `scrollbar.valueChanged` will silently stop
firing once the content shrinks to fit the viewport with nothing left to scroll (the scrollbar's
value stops changing, so the signal never fires again) — even though the condition it was meant
to detect ("we're caught up to live data") is now true. Add an eager check right after any fetch
that could produce this situation (e.g. check "did we just reach the live edge?" immediately after
a history-window re-fetch), rather than only reacting to future scroll events that may never come.

## 10. Testing Qt models without a running app

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

## 11. Debounced filter fields should drive the backend fetch, not a Qt proxy over already-fetched rows (proxy part superseded in log_table_viewer.py - see §14)

A `QSortFilterProxyModel.filterAcceptsRow()` sitting between a streaming model and the view can
only filter rows that already made it into the model's small bounded fetch window (§1's
`viewport_rows`/history window) — typing a search term that doesn't match anything currently
fetched just empties the view, even if plenty of matches exist further back in the backend. Once
the model already supports fetch-time filtering (an `effective_mask`-style condition baked into
the Numba scan kernel — see `numba-njit` skill §2), route free-text search through the *same*
mechanism instead: `LogFilter.set_text_filter()`/`bake_text_search()` feed
`nb_segment_filter_reversed`/`nb_filter_segment` directly, so a live-mode fetch actually re-scans
the backend and fills the viewport with real matches. Once nothing routes through the proxy's
`filterAcceptsRow` anymore, simplify it down to a bare identity `QSortFilterProxyModel` — it's
still needed structurally for `mapToSource`/`mapFromSource` row-index translation the view's
scroll/selection/context-menu code relies on, but don't leave dead filtering logic behind (see
`LogTableFilterProxy` in `log_table_viewer.py`).

For the text field itself, debounce at the widget layer rather than firing a backend re-fetch on
every keystroke:

```python
self._search_timer = QTimer(self)
self._search_timer.setSingleShot(True)
self._search_timer.setInterval(200)
self.search_box.textChanged.connect(lambda _text: self._search_timer.start())
self._search_timer.timeout.connect(self._apply_search_text)  # connect once log_filter exists
```

If the query syntax has a "mid-token" state that's guaranteed to match nothing useful yet (e.g.
`KvFilterLineEdit`'s `key=value` field holding off while the text ends in a bare trailing `key=`
with no value character typed), gate the debounce's fire on a readiness check
(`LogFilter.is_kv_query_ready`) rather than committing every debounce tick — otherwise the view
visibly flashes empty for the fraction of a second between the `=` keystroke and the next
character.

## 12. Column auto-sizing: tier columns by how often their content width actually changes (mechanism superseded in log_table_viewer.py - tiering logic still applies, see §14)

Calling `view.resizeColumnToContents(col)` for every column on every model reset is wasteful once
the model resets at ~10Hz in live mode (§3). Split columns into two tiers instead:

- **Once-only**: columns whose content width is effectively fixed for the session (a fixed-
  precision timestamp, a bounded set of level names, a numeric id). Size them the first time real
  data appears, track it in a `set`, and never touch them again — *except* when something that
  actually changes their displayed text format happens (e.g. the user changes timestamp
  precision), in which case explicitly discard that column from the "already sized" set and
  re-trigger a measurement right there, since a plain formatting-only change
  (`dataChanged`-only, not a full model reset) won't fire the reset-driven hook at all.
- **Throttled**: columns whose content genuinely varies in length as new rows stream in (a device
  or module name column, for instance) — re-measure on a time throttle (e.g. every 2s) tied to
  the model-reset signal, not on every single reset.

```python
def _maybe_autosize_columns(self):
    if self.model.row_count == 0:
        return
    for col in self._ONCE_COLS:
        if col not in self._col_autosized_once:
            self.view.resizeColumnToContents(col)
            self._col_autosized_once.add(col)
    now = time_ns()
    for col in self._THROTTLED_COLS:
        if now - self._col_autosize_last_ns.get(col, 0) >= THROTTLE_NS:
            self.view.resizeColumnToContents(col)
            self._col_autosize_last_ns[col] = now
```

Connect this to `self.proxy.modelReset` (fires on every live/history fetch reset). A column that
was hidden and just became visible via a toggle action should have its "once"/throttle state
reset too, so it gets a sensible width immediately rather than inheriting a stale or never-set
size from while it was hidden. Exclude the column doing `setStretchLastSection` (it self-sizes)
and any column whose width essentially never matters enough to bother.

## 13. Known pre-existing gotcha (not a regression)

Importing certain `blinkview.ui.widgets.*` or `blinkview.core.device_identity` modules as the
*very first* touch of that dependency cluster in a fresh process raises `ImportError: cannot
import name 'DeviceIdentity' from partially initialized module` — a circular import between
`core.device_identity` and `core.id_registry.registry` that only avoids tripping when something
else (e.g. `ops.formatting` importing `core.id_registry.types`) happens to warm the `id_registry`
package first. This is pre-existing (reproduces identically on unmodified `telemetry_table.py`),
not something a new widget file introduces. Don't chase it; it doesn't affect the real app entry
point (`ui.main_window`), which always imports fine.

## 14. Direct-paint replacement for QTableView: QAbstractScrollArea + a plain-object row store

§6 found that `QAbstractItemView` (`QTableView`'s base) always repaints its *entire* viewport on
any content change - insert, remove, or scroll alike, in both `ScrollPerItem` and `ScrollPerPixel`
modes - so per-cell overhead on top of that (`QModelIndex` construction, delegate virtual dispatch,
per-cell role queries, `QSortFilterProxyModel` row-mapping) was pure waste layered on a repaint
that was always going to cover the whole visible area regardless. `log_table_viewer.py` now
side-steps the whole `QAbstractItemModel`/`QTableView`/delegate/proxy stack (§§4, 6, 8, 11, 12
above describe that superseded approach) in favor of painting the visible rows directly, the way
Wireshark's packet list does.

**Split into two classes:**
- `LogTableStore` - a plain Python object (not a `QObject`/`QAbstractTableModel`), holding exactly
  the same columnar ping-ponged buffers, fetch logic (§1/§2/§3), and message/name caches as before.
  Exposes `get_cell(row, col) -> str` and `get_color(row) -> QColor | None` instead of
  `data(index, role)` - same per-column dispatch, no `QModelIndex`/role wrapper. Since there are no
  Qt structural signals anymore, every mutating call (`apply_updates`, `enter_live_mode`,
  `enter_history_mode`, `clear_logs`, `reload_and_redraw`) just mutates state directly; a
  `last_fetch_changed` bool (reset at the top of `apply_updates()`, set `True` only when a fetch
  actually changed row content) lets the widget skip scheduling a repaint on a throttled/no-op
  tick, replacing what `rowsInserted`/`modelReset` used to signal implicitly.
- `LogTableCanvas(QAbstractScrollArea)` - owns painting, scrolling, selection, and input.
  `QAbstractScrollArea` **routes viewport paint/mouse/resize/wheel events to the corresponding
  handler overridden on the scroll-area subclass itself** (`paintEvent`, `mousePressEvent`,
  `resizeEvent`, `wheelEvent`, `contextMenuEvent`) - this is documented `QAbstractScrollArea`
  behavior (see `viewportEvent()`), not a workaround. `paintEvent` does one `QPainter(self.viewport())`,
  loops the currently-visible row range, and calls `store.get_cell`/`get_color` + one
  `painter.drawText()` per visible column per row directly - no delegate, no per-cell object
  construction. `verticalScrollBar()`/`horizontalScrollBar()` come free from the base class.

**No Qt structural signals means the widget must explicitly trigger repaints.** There's no
`beginInsertRows`/`modelReset` to hook anymore, so every place that used to rely on one now calls a
plain method after mutating the store:
- `LogTableCanvas.request_repaint()` - recomputes the scrollbar range from `store.row_count`,
  recomputes the stretch-to-fill MESSAGE column width, and calls `viewport().update()`. Call this
  after any store mutation, gated on `store.last_fetch_changed` in the per-tick path so a quiet
  live tail doesn't schedule a repaint every heartbeat for nothing.
- `LogTableCanvas.autosize_columns()` - same tiered once/throttled logic as §12, just measuring
  `QFontMetrics.horizontalAdvance` over `store.get_cell(...)` text for the *currently visible* rows
  directly, instead of `view.resizeColumnToContents(col)`.

**Selection is keyed by backend sequence id, not row index**: `self.selected_seq: int | None`,
resolved to a row via `store.row_for_seq()` at paint time. A live tick evicting/shifting rows would
otherwise silently select the wrong row if selection were a plain row index - Qt's
`QAbstractItemView` gave persistent-index tracking for free; a hand-rolled replacement needs an
identity that survives the shift, and the backend sequence id is exactly that (already used
elsewhere for anchoring history mode - `seq_for_row`/`row_for_seq` were pre-existing store methods,
reused as-is).

**History-mode scrolling is simpler than the old `QTableView` version, not just a port.** The old
`_reanchor_history` needed `view.doItemsLayout()` between `enter_history_mode()` and `scrollTo()`
to force `QTableView`'s post-`beginResetModel()` deferred row-geometry recompute (a 0ms timer) to
happen synchronously before scrolling, or `scrollTo()` would land on stale geometry (observed:
always row 0). With no `QAbstractItemView` there's no such deferred geometry cache - row position
is just `HEADER_HEIGHT + (row - first_visible_row) * ROW_HEIGHT`, always current - so
`verticalScrollBar().setValue(row)` is synchronous and exact with no workaround needed. Delete
workarounds like this outright when a rewrite removes their root cause, rather than porting them
forward out of caution.

**Testing**: construct `LogTableCanvas(store)` directly with the same `qapp`/`FakeGuiContext`
fixtures §10 describes, `canvas.resize(w, h)`, then either call `canvas.paintEvent(QPaintEvent(canvas.viewport().rect()))`
directly (deterministic, no event-loop timing dependency) or exercise `mousePressEvent`/`wheelEvent`
with real (`QMouseEvent`) or duck-typed fake event objects - a hand-written Python override doesn't
type-check its `event` argument, so a fake object exposing only the methods actually called
(`.angleDelta().y()`, `.accept()`) works fine for branches that don't fall through to
`super().wheelEvent(event)` (which does need a real Qt event object). Use the non-deprecated
6-argument `QMouseEvent(type, localPos, globalPos, button, buttons, modifiers)` constructor, not
the 5-argument form (deprecated in favor of one accepting a `QPointingDevice`).
