# Add key==value (logfmt) row filtering on top of the device/module/level filter

## Context
The current filtering system (`LogFilter` + per-module `effective_mask` + the `nb_segment_filter_reversed`/`filter_segment` Numba kernels) only filters by device, module, and level. We want to add filtering by arbitrary `key=value` pairs found in the log message body itself (logfmt-style, e.g. `status=ok user_id=42`), with multiple conditions ANDed together, for **any** key present in a message — not just keys that have already been promoted into modules by an extraction rule. This lets users narrow a view the same way they'd write a logfmt line, without pre-configuring anything.

## Why not a secondary SoA (the option raised)
A precomputed secondary numpy SoA (hashing known keys into `ext_u64_1` etc. at ingest time) is a strong design *if* the key set is small and known in advance — but it doesn't fit "arbitrary ad-hoc keys": you can't precompute a hash for a key nobody's declared interest in yet, and hashing "all k=v pairs combined" per row doesn't let you test one key's equality without collisions. That approach only pays off for the "pre-promoted keys" scope, which was explicitly ruled out in favor of arbitrary keys. Recommendation: **skip the SoA/ingest-time approach and add an on-demand per-row logfmt scan inside the existing filter kernels instead** — cheap because it only runs on rows that already pass the device/module/level gate, and it requires zero changes to `LogBundle`'s format or the ingestion pipeline.

## Existing building blocks (confirmed via exploration)
- `utils/log_filter.py`: `LogFilter` — device/module/level descriptor, `set_level()` mutator. This is the natural place to add `kv_conditions` state.
- `ops/segments.py`: `nb_segment_filter_reversed` (segments.py:108) and `filter_segment` (segments.py:233) share one predicate line — `is_match = levels[i] >= effective_mask[modules[i]]` (segments.py:157 and :274). Both already have the full `LogBundle` (including `buffer`/`offsets`/`lengths`) in scope inside the loop — this is the clean, single insertion point for an additional predicate, confirmed by reading both functions.
- `parsers/multi_rule_key_value.py`'s `KeyValueExtractionRule.process()` (lines 98-201): a **pure-Python** (not Numba) quote-aware logfmt tokenizer already used at ingest time to promote keys into modules. It cannot be called directly from a Numba `njit` filter kernel, but its *algorithm* (scan for `field_delimiter`, split on first `kv_delimiter`, trim whitespace, strip matching quotes) is exactly what a new Numba-native tokenizer should replicate.
- `core/id_registry/tables.py`'s `IndexedStringTable` / `utils/fnv1a_64.py`'s `fnv1a_64_fast` exist for interning known strings by hash — not needed here since the condition list is small (a handful of typed conditions) and doesn't need a persistent ID table; a flat byte-array comparison per condition is simpler and sufficient.
- `ops/telemetry.py`'s backward-segment-scan pattern is numeric-specific and not directly reusable, but confirms the "scan segment, call a per-row extractor" shape already exists elsewhere in this codebase.

## Implementation

### 1. New Numba kernel: row-level logfmt condition matcher (`ops/segments.py` or a new `ops/kv_filter.py`)
```python
@app_njit()
def nb_row_matches_kv_conditions(
    buffer, offset, length,          # the row's raw message bytes (segment.buffer[offset:offset+length])
    cond_keys_buf, cond_keys_off, cond_keys_len,     # flat condition "key" byte arrays
    cond_vals_buf, cond_vals_off, cond_vals_len,     # flat condition "value" byte arrays
    num_conditions,
    field_delim_int, kv_delim_int,
) -> bool:
```
- Tokenizes `buffer[offset:offset+length]` the same way `KeyValueExtractionRule.process()` does (delimiter scan, first `=` split, whitespace trim, matching-quote strip), but written as pure Numba (typed loops, no Python objects/exceptions/lists).
- For each `key=value` token found, compares it against each not-yet-satisfied condition (byte-equality on both key and value spans); tracks satisfaction via a small bitmask (`num_conditions` capped at e.g. 8).
- Returns `True` only once every condition bit is set; can early-exit the outer scan as soon as that happens.
- No allocation: works purely off the row's existing byte window, same as every other kernel in this file.

### 2. Thread the new predicate through the two existing filter kernels
Add new parameters to `nb_segment_filter_reversed` and `filter_segment` (defaulted so existing callers without a KV filter are unaffected):
```python
def nb_segment_filter_reversed(segment, effective_mask, out_indices, max_matches,
                                start_seq=SEQ_NONE, end_seq=SEQ_NONE, start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED,
                                kv_cond_keys_buf=EMPTY_BYTES, kv_cond_keys_off=EMPTY_IDX, kv_cond_keys_len=EMPTY_IDX,
                                kv_cond_vals_buf=EMPTY_BYTES, kv_cond_vals_off=EMPTY_IDX, kv_cond_vals_len=EMPTY_IDX,
                                kv_num_conditions=0, kv_field_delim=32, kv_kv_delim=61):
```
Change the match line to gate the (more expensive) KV scan behind the cheap level/module check:
```python
level_ok = levels[i] >= effective_mask[modules[i]]
is_match = level_ok and (kv_num_conditions == 0 or nb_row_matches_kv_conditions(
    segment.buffer, segment.offsets[i], segment.lengths[i],
    kv_cond_keys_buf, kv_cond_keys_off, kv_cond_keys_len,
    kv_cond_vals_buf, kv_cond_vals_off, kv_cond_vals_len,
    kv_num_conditions, kv_field_delim, kv_kv_delim,
))
```
Apply the same change to `filter_segment`'s forward-scan predicate (segments.py:274).

### 3. `LogFilter` gains KV condition state (`utils/log_filter.py`)
- Add `self.kv_conditions = []` and a `set_kv_filter(text: str)` method that parses a single logfmt-syntax string (e.g. `status=ok user_id=42`) into `(key_bytes, value_bytes)` pairs using the same trim/quote rules as the ingest-time tokenizer (small, pure-Python parse — this runs once per filter-text-change, not per row, so it doesn't need to be Numba).
- Add a `bake_kv_arrays()` helper (or inline in the consuming widgets, mirroring `_bake_effective_mask`'s existing "bake once, reuse until changed" pattern) that flattens the parsed conditions into the `cond_keys_buf/off/len` + `cond_vals_buf/off/len` arrays the kernel expects. Rebake only when the filter text changes (cache invalidation identical in spirit to `_effective_mask`/`_filter_cache` in `log_table_viewer.py`/`log_viewer.py`).

### 4. Wire into both existing viewers
- `LogViewerWidget` (`ui/widgets/log_viewer.py`) and `LogTableViewerWidget`/`LogTableModel` (`ui/widgets/log_table_viewer.py`) both already call `nb_segment_filter_reversed` (and the table viewer's history mode also calls `filter_segment`) — pass the baked KV arrays through from the shared `LogFilter` at each call site.
- Add one new toolbar widget to each: a `QLineEdit` "Key=Value filter" (placeholder text like `key=value key2=value2`), wired to call `log_filter.set_kv_filter(text)` + trigger the same reload/redraw path used by the existing search box / level combo (`_reload_and_redraw()` in the table viewer, `reload_and_redraw()`/`_redraw_history()` in the text viewer).
- Persist the KV filter text in `get_state()`/`restore()` alongside the existing filter fields, same pattern as `log_level`/`filter_sidebar`.

### 5. Out of scope for this pass
- OR semantics / parenthesized boolean expressions (only AND, per current scope).
- Hashing/interning keys for a UI autocomplete/picker (arbitrary ad-hoc keys means we can't enumerate "known keys" up front without a separate discovery pass — a possible future enhancement, not needed for filtering itself).
- Any change to `LogBundle`'s format, `ext_u32_1/2`/`ext_u64_1`, or the ingestion/parser pipeline — this feature is purely a filter-time addition.
- The inverted-index approach described in Phase 2 below.

## Phase 2 (future, not built now): inverted key→value→seq_id index

If profiling ever shows the on-demand scan is the actual bottleneck (unlikely given this codebase's bounded retained-history and view-sized windows, but worth documenting the escape hatch), the next step up is a genuine inverted index rather than a bigger/smarter scan:

- **Structure**: at ingest time, tokenize every row's logfmt pairs (reusing the same tokenizer algorithm) and, for each distinct `(key, value)`, append the row's `seq_id` to a growable posting-list array. Requires interning both keys and values by hash — this is exactly what `IndexedStringTable` (`core/id_registry/tables.py`) + `fnv1a_64_fast` (`utils/fnv1a_64.py`) already provide, so the interning half is close to a drop-in reuse.
- **Filtering becomes**: parse the query into conditions, hash-lookup each condition's posting list, intersect them (AND) — O(matches), independent of total retained history size, instead of O(rows scanned).
- **Real costs that make this a bigger investment, not a free upgrade**:
  - Ingest-time tokenizing of *every* row, paid whether or not anyone ever filters (today's design has zero always-on cost for this feature).
  - High-cardinality values (UUIDs, timestamps-as-values) produce near-useless single-entry posting lists — index bloat for no lookup benefit.
  - Eviction: `CircularLogPool` currently drops old data for free when segments roll off the ring; an index would need matching eviction of stale seq_ids, or it grows unbounded and drifts out of sync with what's actually still retained.
- **Recommendation**: don't build this speculatively. Ship Phase 1 (on-demand kernel scan) first, measure real usage, and only invest in the index if a concrete performance ceiling is hit.

## Verification
- Unit tests (mirroring `tests/test_ops_segments.py`'s `make_bundle` helper) for `nb_row_matches_kv_conditions`: single condition match/no-match, multiple ANDed conditions (all-match vs one-missing), quoted values containing the delimiter, whitespace around `=`, key present but wrong value, condition key absent from the row entirely.
- Unit tests for `LogFilter.set_kv_filter()`'s parsing (quoting, multiple pairs, malformed input ignored gracefully) and for the bake-array flattening.
- Manual GUI check (not run by the assistant): type a `key=value` filter into each viewer's new field and confirm only matching rows show, that it combines correctly with the existing device/module/level filters (AND), and that it persists across tab-state save/restore.

## Follow-up context from later discussion (not yet folded into the design above)
- This filtering primitive is independently validated by real usage: the same `key=value` structured-filtering need already exists in the user's other projects (an Elixir webapp and a Python client-server tool used to correlate client/server state discrepancies across high-volume logs) — this is not a speculative feature, it's a pattern already proven useful in an adjacent context.
- Worth checking whether those other tools use a specific logfmt convention/library (quoting rules, `=` vs `:` delimiter) that this tokenizer should match, for mental-model consistency across the user's toolchain — open question, not yet resolved.
- This also reinforces the "arbitrary ad-hoc keys" scoping decision: client/server state-discrepancy debugging is exactly the case where you don't know in advance which field will reveal a mismatch, so on-demand scanning over arbitrary keys (not a pre-declared schema) is the right fit.
