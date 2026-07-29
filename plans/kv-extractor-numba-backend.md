# Multi-Rule Key-Value Extractor: Numba backend

## Status: implemented (2026-07-27)

Migrated `parsers/multi_rule_key_value.py`'s five extraction rule types (`key_value`,
`anchor_word`, `json_lite`, `dsv`, `positional`) off pure-Python per-row closures onto a single
Numba-JIT kernel call per input-batch chunk, following `parsers/binary_parser.py`'s
`nb_process_batch_kernel` as the reference for both "one call processes the whole batch" and "how
to add a new module efficiently without a per-row Python registry call."

## Design

- **One flat config shape for every rule type** (`core/types/kv_extraction.py`'s `KvRuleConfig`) -
  the same "one NamedTuple shape fits every step, unused fields stay default" convention
  `UnifiedParserConfig` already uses for the frame-parser pipeline. A `numba.typed.List` needs one
  homogeneous element type, so bundles are flat `(module_id, rule_id, config)` tuples - the exact
  shape `ParserPipelineBundle.pipeline` already uses (`ops/pipeline.py`).
- **Static vs. dynamic module resolution.** Of the five rule types, only `key_value` genuinely
  needs a per-row dynamic module lookup - the key text varies per row. The other four
  (`anchor_word`, `json_lite`, `dsv`, `positional`) all have a fully rule-config-determined target
  module name, which the original pure-Python implementation still resolved lazily via a
  per-instance dict cache on first use; the rewrite resolves them once, eagerly, in `bundle()` -
  removing an entire class of per-row Python work these rule types never actually needed.
- **"Add a new module efficiently"** (`key_value` only) reuses `ops/modules.py`'s established
  discovery-tracker pattern verbatim: `ops/discovery.py`'s `nb_resolve_module_id` hash-checks the
  permanent per-device registry first, then an in-batch temp-id cache, promoting genuinely new
  names to a `MODULE_TEMP_ID_BASE`-offset placeholder id (`core/types/modules.py`). A new
  `nb_write_prefixed_key_lower` helper (`ops/kv_extraction.py`) writes the candidate
  `"parent.key"` string into the shared scratch buffer before handing off to that existing
  resolver - no new discovery mechanism, just its established call shape.
- **`nb_process_kv_batch`** (the single whole-batch entry point) mirrors
  `nb_process_batch_kernel`'s resumable-chunk contract exactly: it stops *before* starting a row
  once the output batch is at/near capacity (never mid-row), reports `out_is_full`, and leaves a
  resume cursor (`KvExtractState.in_idx`) so the caller can flush, acquire a fresh output batch,
  and call again to continue from the exact same row - guaranteeing a row is never partially
  re-emitted (and thus never duplicated) across a flush boundary.
- **`MultiRuleKeyValueParser._post_process`** mirrors `ModuleNameParserBase.post_process`
  (`parsers/frame_parsers.py`) line-for-line: one real `get_module()` call per *distinct new name*
  discovered this cycle (not per row), then a single vectorized swap
  (`active_modules[active_modules == temp_id] = mod_id`) across the whole output batch.

## What changed for each rule type

| Rule | Old per-row Python | New Numba kernel | Module resolution |
|---|---|---|---|
| `key_value` | closure w/ per-instance dict cache | `nb_extract_key_value_row` | dynamic (tracker) |
| `anchor_word` | closure w/ per-instance dict cache | `nb_extract_anchor_word_row` | static (bundle-time) |
| `json_lite` | closure w/ per-instance dict cache | `nb_extract_json_lite_row` | static (bundle-time) |
| `dsv` | closure, ids resolved at `bundle()` already | `nb_extract_dsv_row` | static (unchanged) |
| `positional` | closure, single static id already | `nb_extract_positional_row` | static (unchanged) |

## Testing

- Per-rule-type kernel tests (`tests/test_multi_rule_key_value.py`) calling each
  `nb_extract_*_row` function directly against a real `PooledLogBatch`, covering the same cases
  the old closure-based tests did (quoting, custom delimiters, prefix strip, startswith gates,
  word-count-zero-to-end, etc.) plus new coverage for the temp-id reuse path (two rows with the
  same new key resolve to the same id within one cycle).
- `TestWholeBatchSingleKernelCall` - the explicit "whole batch, one call" requirement: multiple
  rows across multiple rule types and modules resolved in exactly one `nb_process_kv_batch`
  invocation, plus a resumability test (an intentionally tiny output batch forces a flush/resume
  cycle mid-input, asserting no row is dropped or duplicated across it).
- `TestMultiRuleKeyValueParserRealThread` - real parser thread, real queue, including a new
  multi-rule/multi-row case exercising the real `run()` loop end to end.
- Full warmup path (`MultiRuleKeyValueParser.warmup`) verified via a real `Registry`'s
  `get_warmup().run_all()` - confirms the new kernel compiles cleanly alongside every other
  registered warmup, not in isolation.

Full suite: 1944 passed, 0 regressions.
