---
name: blinkview-ingestion-architecture
description: Use when working on an io/*.py reader (a "source") in blinkview and you need to know which device it represents, tag rows with a device id, or otherwise reach registry/pipeline state that "feels like it should be on the reader but isn't". Covers the sources-vs-pipelines split - two independently-constructed objects per logical device, only one of which has a DeviceIdentity - discovered while wiring ADB PID history through AdbReader.
---

# Sources vs. pipelines: two objects per logical device, not one

A logical "device" in a blinkview config (e.g. one physical ADB-connected phone) is represented by
**two separate Python objects**, built by two separate managers, from two separate config
sections, with **no direct object reference between them**:

- **A source** (`io/*.py`, e.g. `AdbReader`) — built by `SourcesManager`
  (`core/sources.py`) from the `"sources"` config section. Reads raw bytes off the wire/device and
  pushes them into the pipeline via `push_log`/queues. Its `self.local` (a `SimpleNamespace`) only
  gets `get_logger`, `push_log`, `logging_id` — **no `device_id`**.
- **A pipeline** (a `BaseParser` subclass, e.g. `BinaryParser`) — built by `PipelineManager`
  (`core/pipeline_manager.py`) from the `"pipelines"` config section. Decodes/parses the raw bytes
  a source produced into structured `LogBundle` rows. Its `self.local.device_id` **is** a real
  `DeviceIdentity`, resolved via `self.shared.id_registry.get_device(name)` where `name` comes
  from the *pipeline's own* config entry (`pipe_f0efb97f`'s `"name": "android"`) — not the
  source's name (`src_c28d4adf`'s `"name": "adb"`, a completely different string that never gets
  registered as a `DeviceIdentity` at all).

Every row's `LogBundle.devices` value ultimately comes from the **pipeline's** `device_id`
(`nb_process_batch_kernel` in `ops/dispatch.py` does `out_b.devices[...] = device_id` from
`p_cfg.device_id`, sourced from the pipeline's `local.device_id`). The source layer never touches
device identity.

## The gotcha this caused

Code was written inside `AdbReader` (a source) assuming `self.local.device_id.id` would give "this
device's" registry id, to key a PID→process-name history table. That attribute doesn't exist on a
source at all — `AttributeError: 'types.SimpleNamespace' object has no attribute 'device_id'`.
Worse, even a naive fallback like `self.shared.id_registry.get_device(self.name)` would have
**silently produced the wrong id** — it would register a *new*, unrelated `DeviceIdentity` under
the source's own name (`"adb"`), which would never match the id actually stamped onto that
device's rows (`"android"`, chosen independently by the pipeline config).

A source that genuinely needs "my own" device id has to look it up by finding the pipeline(s)
configured to consume from it:

```python
def _owning_pipeline_device_ids(self) -> list[int]:
    """A source can legitimately feed more than one differently-named pipeline (e.g. two
    frame_parser configs decoding the same raw bytes two different ways) - return every match,
    not just the first."""
    pipelines = getattr(self.shared.registry.pipelines, "pipelines", None)
    if not pipelines:
        return []

    device_ids = []
    for pipeline in pipelines.values():
        sources = getattr(pipeline, "sources_", None)
        if isinstance(sources, str):
            sources = [sources]
        if sources and self.reference_id in sources:
            device_ids.append(pipeline.local.device_id.id)
    return device_ids
```

`self.reference_id` (set by both `SourcesManager` and `PipelineManager` to the config dict key,
e.g. `"src_c28d4adf"`) is the only stable link between the two — a pipeline's `sources_` list
names sources by their `reference_id`, not by any device-name string. `self.shared.registry.
pipelines` may be `None` before pipeline config is applied (e.g. very early during a source's
`open()`) — handle that by returning an empty list and letting a periodic re-check pick it up
later, rather than crashing.

**One source can feed multiple pipelines** (confirmed against a real profile: one ADB source fed
both an enabled `"android"` pipeline and a disabled `"clean"` pipeline, both listing the same
source in `sources_`) — so any state keyed by "this source's device" should loop over every
returned id, not assume exactly one.

## Where to verify this against real config

`.blinkview/profiles/<profile>/<profile>.json` has top-level `"sources"` and `"pipelines"` dicts —
read one directly rather than guessing at the relationship from code alone; the `name` fields in
each section are independent and commonly differ (as above).

To sanity-check a device-id lookup like the one above without a real connected device, build a
real `Registry(session_name=...)`, call `.start()`, and inspect `r.pipelines.pipelines` / a
source's `_owning_pipeline_device_ids()` directly - see the debugging session that led to this
skill for the exact script shape (construct `Registry`, `.start()`, grab the source instance from
`r.sources.sources`, call the method, print the result).
