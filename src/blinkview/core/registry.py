# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from queue import Queue
from threading import RLock
from types import SimpleNamespace
from typing import TYPE_CHECKING, Callable, Optional

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.config_manager import ConfigManager
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory_category_registry import build_system_factory_registry
from blinkview.core.id_history import IdHistory
from blinkview.core.id_registry import IDRegistry
from blinkview.core.logger import PrintLogger, SystemLogger
from blinkview.core.module_snapshot import LatestModuleValueTracker
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.plugin_manager import PluginManager
from blinkview.core.settings_manager import SettingsManager
from blinkview.core.sources import SourcesManager
from blinkview.core.system_context import SystemContext
from blinkview.core.task_manager import TaskManager
from blinkview.storage.file_manager import FileManager
from blinkview.subscribers.subscriber import SubscriberFactory
from blinkview.utils.time_utils import TimeUtils

if TYPE_CHECKING:
    from blinkview.core.central_storage import CentralStorage
    from blinkview.core.pipeline_manager import PipelineManager
    from blinkview.core.playback_clock import PlaybackClock
    from blinkview.core.playback_ranges import PlaybackRangeStore
    from blinkview.storage.file_logger import FileLogger


def _import_registerable_modules():
    """Triggers the @XFactory.register(...)/@register_factory_category(...) import-time side
    effects that build_system_factory_registry() below depends on. Deliberately kept as a plain
    function call rather than top-level imports: isort/ruff will happily re-sort a top-level
    `from blinkview import io as io` ahead of `from blinkview.core.id_registry import IDRegistry`
    (shorter dotted path sorts first), and doing so reintroduces a real circular import
    (core.types.parsing <-> core.id_registry <-> utils.level_map) that only "works" when
    core.id_registry finishes resolving before blinkview.io/parsers/storage pull in
    core.types.parsing themselves. This function runs after every normal import above has already
    completed, so it can't be silently reordered into the broken position by an autoformatter.

    core.central_storage is imported here too, deliberately duplicating the TYPE_CHECKING import
    above: CentralStorage is only referenced in a string-quoted annotation (no runtime need), so
    an "optimize imports" pass will happily move it under TYPE_CHECKING - which is correct for the
    annotation but silently drops CentralFactory's @register_factory_category(FactoryCategory.
    CENTRAL) registration, since TYPE_CHECKING imports never execute. Import the module here,
    independent of the annotation's import, so the registration survives regardless of what an
    autoformatter decides about the annotation-only reference.

    subscribers.subscriber is imported here too, for the same reason, pre-emptively: the module
    is currently imported at the top for a real, direct use (`SubscriberFactory.build(...)` in
    build_subscriber()), which incidentally also registers TimeSyncerFactory
    (@register_factory_category(FactoryCategory.TIME_SYNC)) defined in that same module - but that
    registration would silently vanish the moment build_subscriber() stops calling
    SubscriberFactory directly and someone removes the now "unused" top-level import. Duplicating
    it here decouples the registration from whatever direct use of SubscriberFactory happens to
    exist elsewhere in this file."""
    from blinkview import io as io
    from blinkview import parsers as parsers
    from blinkview import storage as storage
    from blinkview.core import central_storage as central_storage
    from blinkview.core.reorderer import Reorder as Reorder
    from blinkview.subscribers import subscriber as subscriber
    from blinkview.utils import level_map as level_map


_import_registerable_modules()


class Registry:
    # Name of the auto-created "whole recording" range (see _enter_replay_mode_if_detected) -
    # matched by name (not id) to avoid re-adding a duplicate on every subsequent replay-of-a-
    # replay generation, since a fresh add() always mints a new uuid.
    DEFAULT_REPLAY_RANGE_NAME = "Full recording"

    def __init__(
        self,
        session_name: str = None,
        config_path: str = None,
        profile_name: str = None,
        log_dir: str | Path = None,
        settings=None,
        replay_mode: bool = False,
    ):
        # ==========================================
        # LAYER 1: Core Services
        # ==========================================

        self.initialized = False
        self.replay_mode = replay_mode

        self._temp_log_queue: Queue = Queue()

        np_pool = NumpyArrayPool(max_bytes=64 * 1024 * 1024)

        self.log_lock = RLock()
        self.log_batch: Optional[PooledLogBatch] = None
        self.log_buffer_bytes = 4096
        self.log_capacity = self.log_buffer_bytes * 1024 / 32  # 32 chars per msg

        self.time_utils = TimeUtils()
        self.now = self.time_utils.now
        self.now_ns = self.time_utils.now_ns

        self.logger = self.logger_creator("registry")()

        factories = build_system_factory_registry()

        self.file_manager = FileManager(
            session_name=session_name,
            profile_name=profile_name,
            log_dir=log_dir,
            config_path=config_path,
            replay_mode=replay_mode,
        )

        default_config = {
            "version": "0.2",
            "sources": {},
            "pipelines": {},
            "plugins": {},
            "reorder": {"enabled": True, "type": "default"},
            "central": {"enabled": True, "type": "default"},
        }
        self.config = ConfigManager(
            self.file_manager.get_config_path(), self.file_manager.get_session_path(suffix="autosave"), default_config
        )
        self.config.save_full_config(self.file_manager.get_session_path(suffix="start"))
        self.config.get_schema_by_path = self.get_schema_by_path

        self.plugins = PluginManager(self, self.logger_creator("plugins")())

        self.key_to_base_class = {
            "central": factories.get_produced_type(FactoryCategory.CENTRAL),
            "reorder": factories.get_produced_type(FactoryCategory.REORDER),
        }

        # print(f"[Registry] key_to_base_class mapping: {self.key_to_base_class}")

        for key, base_cls in self.key_to_base_class.items():
            if base_cls is not None and hasattr(base_cls, "get_config_schema"):
                # print(f"[Registry] Base class for '{key}' has config schema: {json.dumps(base_cls.get_config_schema(), indent=4)}")
                pass
            else:
                print(f"[Registry] Base class for '{key}' does not have a config schema or is None.")

        self.session_name = session_name

        self.warmup_helper = None

        self.warmup_success = False
        self.warmup_error = None
        self._warmup_done = False

        # ==========================================
        # LAYER 2: Storage & Sinks
        # ==========================================
        # Initialize the file manager for this session

        # Snapshot the logic immediately
        # self.file_manager.save_snapshot(["src/", "configs/"])

        self.id_registry = IDRegistry(np_pool)
        self.pid_history = IdHistory()

        self.system_ctx = SystemContext(
            time_ns=self.now_ns,
            registry=self,
            id_registry=self.id_registry,
            factories=factories,
            tasks=TaskManager(),
            settings=settings or SettingsManager(),
            array_pool=np_pool,
            pid_history=self.pid_history,
        )
        self.file_manager.set_context(self.system_ctx)

        self.system_device = self.id_registry.get_device("SYSTEM", essential=False)
        self.log_device_id = self.system_device.id

        self.sources = None

        # ==========================================
        # LAYER 4: Hardware Pipelines
        # ==========================================
        self.pipelines: "PipelineManager" = None

        self._is_running = False

        self.central: "CentralStorage" = None
        self.reorder = None
        self.playback_clock: Optional["PlaybackClock"] = None
        self.playback_ranges: Optional["PlaybackRangeStore"] = None

        self.module_value_tracker: LatestModuleValueTracker = None

        self._subscribers = []

    def _create_and_bind(self, cls, name, config):
        print(f"[Registry] _create_and_bind name={name} cls={cls.__name__}  config={config}")
        local_ctx = SimpleNamespace(get_logger=self.logger_creator(name))
        instance = cls()
        if hasattr(instance, "bind_system"):
            instance.bind_system(self.system_ctx, local_ctx)
        else:
            print(f"[Registry] _create_and_bind name={name} cls={cls.__name__} does not have bind_system method.")
        if hasattr(instance, "apply_config"):
            instance.apply_config(config)
        else:
            print(f"[Registry] _create_and_bind name={name} cls={cls.__name__} does not have apply_config method.")
        return instance

    def reinit_logger(self, target):
        if target is None:
            return

        if hasattr(target, "init_logger") and callable(target.init_logger):
            target.init_logger(self.logger_creator)
            return

        # Safely extract context depending on whether the previous logger was PrintLogger or SystemLogger
        ctx = getattr(target.logger, "ctx", getattr(target.logger, "module_path", ""))

        # Extract the essential flag to pass it over
        is_essential = getattr(target.logger, "is_essential", False)

        target.logger = self.logger_creator(category=ctx, essential=is_essential)()

    def logger_creator(self, category: str, name: str = None, essential: bool = False):
        if not self.initialized:
            return lambda: PrintLogger(category, name, self._temp_log_queue.put, self.now_ns, essential=essential)

        return lambda: SystemLogger(category, name, self, essential=essential)

    def get_device(self, device_name):
        return self.id_registry.get_device(device_name)

    def get_registry_schema(self, key: str):
        if hasattr(self, key):
            obj = getattr(self, key, None)
            if obj is not None and hasattr(obj, "get_config_schema"):
                return obj.get_config_schema()
            else:
                base_cls_type = self.key_to_base_class.get(key, None)
                if base_cls_type is not None:
                    return base_cls_type.get_config_schema()
        return {}

    def get_schema_by_path(self, path: str, drop_keys: list = None):
        # path splitted by /, e.g., "devices/ABC/reader"
        # if root is requested, return the full schema
        root_keys = ("plugins", "central", "reorder")
        schema = {}

        if path == "/":
            schema = {"type": "object", "title": "Configuration", "description": "", "properties": {}}
            # drop keys
            if drop_keys is not None:
                root_keys = [k for k in root_keys if k not in drop_keys]

            required = []
            # if central is not dropped, add to reqiored
            if "central" in root_keys:
                required.append("central")

            if "reorder" in root_keys:
                required.append("reorder")

            schema["required"] = required
            print(f"[Registry] get_schema_by_path: path={path} drop_keys={drop_keys} root_keys={root_keys}")

            for key in root_keys:
                sub_schema = self.get_registry_schema(key)
                # print(f"[Registry] get_schema_by_path: key={key} sub_schema={json.dumps(sub_schema, indent=4)}")
                schema["properties"][key] = sub_schema

        else:
            splitted = path.strip("/").split("/")
            print(f"[Registry] get_schema_by_path: path={path}, splitted={splitted}")
            if len(splitted) == 1:
                schema = self.get_registry_schema(splitted[0])
            elif len(splitted) == 2:
                if splitted[0] == "sources":
                    # e.g., /sources/Camera1
                    # we don't know the device type until runtime, so we return a generic schema with all possible fields
                    return self.sources.get_schema(splitted[1])
                elif splitted[0] == "pipelines":
                    # e.g., /pipelines/Camera1
                    return self.pipelines.get_schema(splitted[1])

        return schema

    def _save_playback_ranges(self):
        """PlaybackRangeStore's on_change callback - fires on every add/remove/rename. Always
        writes to *this* session's own folder (sits right alongside the raw captured log data),
        regardless of whether this session is itself a live capture or a replay of an older one -
        see core/playback_ranges.py's module docstring."""
        if self.playback_ranges is None or self.file_manager is None:
            return
        try:
            self.playback_ranges.save_to_file(self.file_manager.get_playback_ranges_path())
        except OSError as e:
            self.logger.warning(f"Failed to save playback ranges: {e}")

    def _iter_replay_source_dirs(self):
        """Yields the resolved parent directory of every configured source's `file_path` (duck-
        typed - BinaryFileReader/FileTailReader shaped - rather than importing those reader
        classes, so it picks up any future file-based reader for free). Shared basis for
        detecting "this session is a replay of a file" and for finding sibling files
        (playback_ranges.json, metadata.json) a previous session saved alongside its own captured
        data."""
        if self.sources is None:
            return

        from blinkview.utils.paths import resolve_config_path

        for source in self.sources.sources.values():
            file_path = getattr(source, "file_path", None)
            if not file_path:
                continue
            try:
                yield resolve_config_path(file_path).parent
            except Exception:
                continue

    def _is_replay_session(self) -> bool:
        """True if any configured source reads from a file rather than a live device - the
        trigger for auto-entering DVR playback mode (see _enter_replay_mode_if_detected)."""
        return next(self._iter_replay_source_dirs(), None) is not None

    def _discover_replay_ranges_path(self) -> Optional[Path]:
        """Best-effort: if any configured source is replaying a file that lives inside a
        previous blinkview session's folder, returns that folder's playback_ranges.json if one
        exists there."""
        for source_dir in self._iter_replay_source_dirs():
            candidate = source_dir / "playback_ranges.json"
            if candidate.exists():
                return candidate
        return None

    def _load_replay_playback_ranges(self):
        """Opportunistically preloads ranges saved during/after a previous capture, when this
        run is replaying that capture's file. Merges (replace=False) rather than overwrites,
        since this session's own (currently empty, freshly-created) ranges file was already the
        load target for anything saved earlier in this same run. Superseded by
        load_replay_session() for actually driving DVR mode, but kept as a standalone method
        since it's simple, self-contained, and already covered by its own tests."""
        if self.playback_ranges is None:
            return

        path = self._discover_replay_ranges_path()
        if path is not None:
            self.playback_ranges.load_from_file(path, replace=False)

    @staticmethod
    def _parse_iso_utc_to_epoch_ns(iso_str) -> Optional[int]:
        """Parses a metadata.json wall-clock timestamp (created_at/finished_at, ISO 8601 with a
        trailing 'Z') into epoch-ns - the same units as log row timestamps/PlaybackClock bounds,
        on the assumption (true for this app's normal ingestion path) that log timestamps are
        real epoch time rather than device-relative. A close-enough default marker, not a
        precise one: clock sync lag between session start and the first actual log row means the
        range's exact edges may be off by a small amount - the user can always re-mark it via
        Mark In/Mark Out."""
        if not iso_str:
            return None
        from datetime import datetime, timezone

        try:
            dt = datetime.fromisoformat(iso_str.rstrip("Z")).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return int(dt.timestamp() * 1_000_000_000)

    def load_replay_session(self, session_dir):
        """Wires this registry's DVR playback mode and named ranges to a previously-recorded
        session living at `session_dir`: merges in that session's own saved
        playback_ranges.json (if any), adds a DEFAULT_REPLAY_RANGE_NAME range spanning the whole
        recording from its metadata.json created_at/finished_at (if a cleanly-finished one
        exists - without waiting for the replay to actually stream all the way back in), and
        switches PlaybackClock into REPLAY (deferred via enter_replay_when_ready() until real
        data exists).

        Two callers, because this app has two distinct replay mechanisms:
        - _enter_replay_mode_if_detected() - auto-triggered at configure_system() time for the
          dev-replay workflow: a configured BinaryFileReader/FileTailReader source whose
          file_path happens to live inside a previous session's folder.
        - MainWindow.start_replay() - the production "Load Session..." menu path, which starts a
          UnifiedLogReplay directly against registry.central and never touches self.sources at
          all, so it can't be auto-detected the same way and must call this explicitly once it
          already knows the session's folder (session_info.path from utils/session_lister.py).
        """
        if self.playback_clock is None:
            return

        session_dir = Path(session_dir)

        # Everything this registry might write from here on (playback ranges, gui state, the
        # watchlist, ...) gets redirected into session_dir/replay/ instead of session_dir itself
        # or the live workspace profile - see FileManager._redirect_to_replay_scratch. The
        # original session's own files are never opened for writing.
        self.file_manager.replay_source_dir = session_dir

        if self.playback_ranges is not None:
            # Goes through the same replay/ scratch redirect as saving (get_playback_ranges_path,
            # now that replay_source_dir is set above) rather than session_dir/playback_ranges.json
            # directly - otherwise a range added during a previous replay of this same session
            # (which only ever gets written into the scratch copy, never the original file) would
            # be silently lost the next time this session is replayed.
            ranges_path = self.file_manager.get_playback_ranges_path()
            if ranges_path.exists():
                self.playback_ranges.load_from_file(ranges_path, replace=False)

        default_start_ts_ns = None
        metadata_path = session_dir / "metadata.json"
        if metadata_path.exists() and self.playback_ranges is not None:
            import json

            try:
                with metadata_path.open("r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except (OSError, json.JSONDecodeError):
                metadata = None

            if metadata is not None:
                start_ts_ns = self._parse_iso_utc_to_epoch_ns(metadata.get("created_at"))
                end_ts_ns = self._parse_iso_utc_to_epoch_ns(metadata.get("finished_at"))
                if start_ts_ns is not None and end_ts_ns is not None and end_ts_ns > start_ts_ns:
                    default_start_ts_ns = start_ts_ns
                    already_present = any(r.name == self.DEFAULT_REPLAY_RANGE_NAME for r in self.playback_ranges.ranges)
                    if not already_present:
                        self.playback_ranges.add(self.DEFAULT_REPLAY_RANGE_NAME, start_ts_ns, end_ts_ns)

        self.playback_clock.enter_replay_when_ready(default_start_ts_ns)

    def _enter_replay_mode_if_detected(self):
        """Auto-activates DVR playback mode (see load_replay_session) the moment the dev-replay
        workflow is detected: any configured source reading from a file rather than a live
        device - the user shouldn't have to notice they loaded a replay and manually click into
        REPLAY mode. Searches every candidate source directory (not just the first) for one with
        metadata.json/playback_ranges.json to load_replay_session with, same as
        _discover_replay_ranges_path used to; falls back to a bare enter_replay_when_ready() (DVR
        mode with no session-specific data) if a file-based source is configured but neither
        sidecar file is found anywhere."""
        if self.playback_clock is None:
            return

        candidate_dirs = list(self._iter_replay_source_dirs())
        if not candidate_dirs:
            return

        session_dir = next(
            (d for d in candidate_dirs if (d / "metadata.json").exists() or (d / "playback_ranges.json").exists()),
            None,
        )
        if session_dir is not None:
            self.load_replay_session(session_dir)
        else:
            self.playback_clock.enter_replay_when_ready()

    def stop(self):
        """Cleanly tear down the session."""
        if self._is_running:
            if self.sources is not None:
                self.sources.stop()

            if self.pipelines is not None:
                self.pipelines.stop()

            if self.reorder is not None:
                self.reorder.stop()
            if self.central is not None:
                self.central.stop()
                # CentralStorage.stop() only stops the ingestion thread - log data (and any cold-
                # storage mmaps) deliberately survive it, see plans/mmap-coldstore.md, so a plain
                # daemon .stop() (as CentralStorage's own unit tests exercise in isolation) stays
                # queryable. But Registry.stop() is the real end-of-session teardown (BlinkMainWindow
                # .closeEvent, CLI shutdown) - nothing queries this pool again after this point, so
                # release it here. This is what actually lets ColdStorageArchiver's atexit cleanup
                # delete the session's cold-storage files: on Windows a memory-mapped file can't be
                # deleted, and without this, every cold segment's mmap was still open (release_all()
                # was otherwise only ever called by warmup.py's throwaway dummy pool).
                if self.central.log_pool is not None:
                    self.central.log_pool.release_all()

            self.file_manager.stop()

            for sub in self._subscribers.copy():
                stop_fn = getattr(sub, "stop", None)
                if stop_fn is not None:
                    sub.stop()
                self.unsubscribe(sub)

            self._is_running = False
            print("Session stopped.")

        # TaskManager's scheduler thread is started unconditionally in __init__, independent
        # of start()/_is_running, so it must always be shut down here - even if start() was
        # never called - or it leaks as a zombie thread that races interpreter shutdown.
        self.system_ctx.tasks.shutdown()

    def configure_system(self):
        try:
            print(f"[Registry] Configuring system with session name: {self.session_name}")
            # base configuration
            if self.initialized:
                return

            # print()
            self.plugins.apply_config(self.config.get_by_path("/plugins"))
            print("[Registry] Applied plugin configuration.")

            system_ctx = self.system_ctx
            factories = system_ctx.factories

            try:
                reorder_config = self.config.get_by_path("/reorder")
                if reorder_config:  # is not None and reorder_config.get("enabled", True):
                    self.logger.info(f"[System] reorder_config: {reorder_config}")
                    if reorder_config.get("type") is None:
                        reorder_config["type"] = "default"

                    local_ctx = SimpleNamespace(get_logger=self.logger_creator("reorder"))

                    self.reorder = factories.build(FactoryCategory.REORDER, reorder_config, system_ctx, local_ctx)

                    self.config.subscribe("/reorder", self.reorder)

                    self.reorder.reference_id = "reorder"
            except Exception as e:
                print(f"[Registry] Error configuring reorder buffer: {e}")
                self.logger.error("Error configuring reorder buffer:", e)

            try:
                central_storage_config = self.config.get_by_path("/central")
                if central_storage_config:  # is not None and central_storage_config.get("enabled", True):
                    self.logger.info(f"[System] central_storage_config: {central_storage_config}")
                    if central_storage_config.get("type") is None:
                        central_storage_config["type"] = "default"

                    local_ctx = SimpleNamespace(
                        get_logger=self.logger_creator("central"),
                        logging_id="session",
                    )

                    self.central = factories.build(
                        FactoryCategory.CENTRAL, central_storage_config, system_ctx, local_ctx
                    )
                    if self.reorder is not None:
                        self.reorder.subscribe(self.central)

                    self.config.subscribe("/central", self.central)

                    self.central.reference_id = "central"

                    from blinkview.core.playback_clock import PlaybackClock
                    from blinkview.core.playback_ranges import PlaybackRangeStore

                    self.playback_clock = PlaybackClock(self.central.log_pool)
                    self.playback_ranges = PlaybackRangeStore(on_change=self._save_playback_ranges)
            except Exception as e:
                # print(f"[Registry] Error configuring central storage: {e}")
                self.logger.exception("Error configuring central storage", e)

            self.initialized = True

            self._dump_temp_logs()

            self.logger.info(f"[System] System initialized with session name: {self.session_name}")

            self.reinit_logger(self)
            self.logger.info("[System] Registry logger initialized.")

            self.reinit_logger(self.id_registry)
            self.reinit_logger(self.reorder)
            self.reinit_logger(self.central)

            try:
                print("[Registry] Configuring sources")
                self.sources = self._create_and_bind(SourcesManager, "sources", self.config.get_by_path("/sources"))
                self.config.subscribe("/sources", self.sources)
            except Exception as e:
                print(f"[Registry] Error during sources configuration: {e}")
                self.logger.error("Error during sources configuration", e)

            # load_replay_session (called by this) already covers what _load_replay_playback_
            # ranges() alone used to - that standalone method is kept as its own tested unit,
            # not called separately here to avoid loading the same ranges file twice.
            self._enter_replay_mode_if_detected()
            try:
                from blinkview.core.pipeline_manager import PipelineManager

                self.pipelines = self._create_and_bind(
                    PipelineManager, "pipelines", self.config.get_by_path("/pipelines")
                )

                self.config.subscribe("/pipelines", self.pipelines)
                self.pipelines.apply_targets()
            except Exception as e:
                print(f"[Registry] Error during pipelines configuration: {e}")
                self.logger.error("Error during pipelines configuration", e)

            try:
                if self.sources is not None:
                    self.sources.apply_targets()
            except Exception as e:
                print(f"[Registry] Error during applying source targets: {e}")
                self.logger.error("Error during applying source targets", e)

        except Exception as e:
            print(f"[Registry] Error during system configuration: {e}")
            self.logger.error("Error during system configuration", e)

    def _dump_temp_logs(self):
        get_module = self.system_device.get_module
        log_append = self.log_append
        get_nowait = self._temp_log_queue.get_nowait

        while True:
            try:
                timestamp, module_name, level_id, msg = get_nowait()
                log_append(timestamp, level_id.value, get_module(module_name).id, msg)
            except Exception:
                break

        self._temp_log_queue = None  # Release the temporary log queue

    def warmup(self):
        """Compiles Numba kernels. Safe to call once, ahead of start(); start() will
        call it itself if it hasn't run yet."""
        if self._warmup_done:
            return

        try:
            self.warmup_success = False
            self.logger.warn("NUMBA: compiling kernels")

            self.get_warmup().run_all()

            self.logger.warn("NUMBA: compiling done")
            self.warmup_success = True
        except Exception as e:
            self.warmup_error = str(e)
            self.warmup_success = False
            self.logger.exception("Error during compiling kernels", e)
        finally:
            self.warmup_helper = None
            self._warmup_done = True

    def start(self, configure=True):
        if self._is_running:
            return

        self.warmup()

        self.logger.warn(f"--- Starting Session: {self.session_name} ---")

        if configure:
            self.configure_system()

        # This allows for plugin registration between __init__ and start()
        # self.pipelines.build_from_config()

        # self.reinit_logger(self.pipelines)

        if self.central is not None:
            print("[Registry] Starting central storage...")
            self.central.start()

        if self.reorder is not None:
            print("[Registry] Starting reorder buffer...")
            self.reorder.start()
        # self.parser_thread.start()
        # Start Hardware Pipelines (Readers + Parsers)

        if self.pipelines is not None and not self.replay_mode:
            self.pipelines.start()

        if self.sources is not None and not self.replay_mode:
            self.sources.start()

        tasks = self.system_ctx.tasks
        # tasks.run_periodic(1, self.buffer_stats)
        flush_interval = self.reorder.delay / 2 / 1000 if self.reorder is not None and self.reorder.enabled else 0.1
        tasks.run_periodic(flush_interval, self.flush_log_queue)

        tasks.run_periodic(60, self.system_ctx.array_pool.cleanup, max_age_seconds=55.0)

        if self.module_value_tracker is None:
            self.module_value_tracker = LatestModuleValueTracker(
                self.central.log_pool, self.id_registry.modules_table, self.system_ctx.array_pool, self.now_ns
            )

        tasks.run_periodic(1.0 / 60, self.module_value_tracker.update_and_print)

        self._is_running = True
        self.logger.warn("BlinkView is now live.")

    #
    # def add_parser_consumer(self, consumer):
    #     self.parser_thread.add_consumer(consumer.put_many)
    #
    # def add_raw_parser_consumer(self, consumer):
    #     self.parser_thread.add_raw_consumer(consumer.put_many)

    def build_subscriber(self, name, subscriber_type: str, config=None, **kwargs):
        print(
            f"[System] Building subscriber name='{name}' type='{subscriber_type}' with config: {config} and kwargs: {kwargs}"
        )
        if config is None:
            config = {"type": subscriber_type, "enabled": True}

        local_ctx = SimpleNamespace(get_logger=self.logger_creator(subscriber_type))
        subscriber = SubscriberFactory.build(config, self.system_ctx, local_ctx, **kwargs)
        subscriber.reference_id = name
        self.pipelines.subscribe(name, subscriber)
        self._subscribers.append(subscriber)
        return subscriber

    def subscribe(self, subscriber):
        self.central.subscribe(subscriber)

        self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber):
        self.central.unsubscribe(subscriber)
        self._subscribers.remove(subscriber)

    add_file_logger: Callable[[any, str, Optional[str]], FileLogger]
    now: Callable[[], float]
    now_ns: Callable[[], int]

    def get_reference_values(self, name):
        values = []
        if name == "/sources":
            for item_id, item in self.sources.sources.items():
                values.append((item_id, item.name))
        elif name == "/targets":
            if self.pipelines is not None:
                for item_id, item in self.pipelines.pipelines.items():
                    values.append((item_id, item.name))
            if self.central is not None:
                values.append(("central", "Central Storage"))

            if self.reorder is not None:
                values.append(("reorder", "Reorder Buffer"))
        elif name == "/pipelines":
            if self.pipelines is not None:
                for item_id, item in self.pipelines.pipelines.items():
                    values.append((item_id, item.name))

        print(f"[Registry] get_reference_values '{name}': {values}")
        return values

    def get_source(self, source_id: str):
        if self.sources is not None:
            source = self.sources.get(source_id)
            if source is not None:
                return source

        if source_id == "central" and self.central is not None:
            return self.central

        if source_id == "reorder" and self.reorder is not None:
            return self.reorder

        return None

    def get_target(self, target_id: str):
        if self.pipelines is not None:
            target = self.pipelines.get(target_id)
            if target is not None:
                return target

        if self.central is not None and "central" == target_id:
            return self.central

        if self.reorder is not None and "reorder" == target_id:
            return self.reorder

        return None

    def get_reference_target(self, target_id: str):
        if self.pipelines is not None:
            target = self.pipelines.get(target_id)
            if target is not None:
                return target

        if self.sources is not None:
            source = self.sources.get(target_id)
            if source is not None:
                return source

        if self.central is not None and "central" == target_id:
            return self.central

        if self.reorder is not None and "reorder" == target_id:
            return self.reorder

        return None

    def buffer_stats(self, rate_width=12):
        """
        Prints buffer statistics.
        :param rate_width: The character width for Push/s and Pop/s columns.
        """
        if not hasattr(self, "_prev_buffer_stats"):
            self._prev_buffer_stats = {}

        try:
            # Collect all queues
            queue_map = {}

            def collect(obj_group):
                if not obj_group:
                    return
                # Handle dictionaries (like self.sources.sources)
                if isinstance(obj_group, dict):
                    items = obj_group.values()
                # Handle lists or sets (like self._subscribers)
                elif isinstance(obj_group, (list, set)):
                    items = obj_group
                else:
                    return

                for item in items:
                    q = getattr(item, "input_queue", None)
                    ref_id = getattr(item, "reference_id", None)
                    if q and ref_id:
                        queue_map[ref_id] = q

            collect(self.pipelines.pipelines)
            collect(self.sources.sources)
            collect(self._subscribers)

            if self.reorder:
                queue_map[self.reorder.reference_id] = self.reorder.input_queue
            if self.central:
                queue_map[self.central.reference_id] = self.central.input_queue

            # Build Table Header
            # Using dynamic width for rates;
            # format: < ensures left align for header, > ensures right align for numbers
            header = (
                f"{'Queue Name':<20} | "
                f"{'Count':<7} | "
                f"{'% Full':<8} | "
                f"{'Push/s':>{rate_width}} | "
                f"{'Pop/s':>{rate_width}} | "
                f"{'State':<10}"
            )
            lines = [header, "-" * len(header)]

            for name, q in queue_map.items():
                curr = q.get_stats()
                prev = self._prev_buffer_stats.get(name)

                push_rate = 0.0
                pop_rate = 0.0
                is_dropping = False

                if prev:
                    dt = curr["now"] - prev["now"]
                    if dt > 0:
                        push_rate = (curr["pushed"] - prev["pushed"]) / dt
                        pop_rate = (curr["popped"] - prev["popped"]) / dt
                        is_dropping = curr["dropped"] > prev["dropped"]

                fill_pct = (curr["total"] / curr["maxlen"]) * 100 if curr["maxlen"] > 0 else 0

                # Determine State
                if is_dropping:
                    state = "⚠️ DROP"
                elif fill_pct > 90:
                    state = "🔥 CRIT"
                elif fill_pct > 70:
                    state = "WARN"
                elif push_rate > pop_rate * 1.1 and fill_pct > 20:
                    state = "📈 FILL"
                else:
                    state = "✅ OK"

                # Format Row
                # {value:>{rate_width},.1f} adds commas and ensures 1 decimal place
                row = (
                    f"{name[:20]:<20} | "
                    f"{curr['total']:<7} | "
                    f"{fill_pct:>6.1f}% | "
                    f"{push_rate:>{rate_width},.0f} | "
                    f"{pop_rate:>{rate_width},.0f} | "
                    f"{state:<10}"
                )
                lines.append(row)

                # Store for next delta
                self._prev_buffer_stats[name] = curr

            print("\n[BUFFER_STATS]\n" + "\n".join(lines) + "\n")

        except Exception as e:
            self.logger.exception(f"buffer_stats failed: {e}")

    def flush_log_queue(self):
        with self.log_lock:
            batch = self.log_batch
            if batch is not None and batch.size > 0:
                with batch:
                    put_fn = self.reorder.put if self.reorder is not None and self.reorder.enabled else self.central.put
                    put_fn(batch)

            self.log_batch = None

    def log_create_batch(self):
        batch = self.system_ctx.array_pool.create(
            PooledLogBatch,
            self.log_capacity,
            self.log_buffer_bytes,
            has_levels=True,
            has_modules=True,
            has_devices=True,
        )
        with self.log_lock:
            self.log_batch = batch
        return batch

    def log_append(self, timestamp, level_id, module_id, msg):
        with self.log_lock:
            batch = self.log_batch
            if batch is None:
                batch = self.log_create_batch()
            encoded = msg.encode()
            if not batch.insert(timestamp, timestamp, encoded, level_id, module_id, self.log_device_id):
                # batch full, flush and create new batch
                self.flush_log_queue()
                batch = self.log_create_batch()
                batch.insert(timestamp, timestamp, encoded, level_id, module_id, self.log_device_id)

    def get_warmup(self):
        if self.warmup_helper is None:
            from blinkview.core.warmup import NumbaWarmupHelper

            self.warmup_helper = NumbaWarmupHelper(self.system_ctx)
        return self.warmup_helper


def run_memory_test():
    import gc
    import os
    from time import sleep

    import psutil

    process = psutil.Process(os.getpid())

    def get_stats():
        gc.collect()  # Ensure we measure actual retained memory
        full_info = process.memory_full_info()
        basic_info = process.memory_info()

        # .private is Windows specific. Fallback to rss if on Linux/macOS
        private_bytes = getattr(basic_info, "private", basic_info.rss)
        uss = full_info.uss
        return private_bytes, uss

    # 1. Baseline
    print("--- Starting Test ---")
    base_private, base_uss = get_stats()

    # 2. Setup and Execution
    # registry = Registry()
    # registry.configure_system()
    # registry.start()

    # SIMULATION: Replace this with your actual registry logic
    sleep(2)

    # 3. Final Measurement
    # registry.stop()
    final_private, final_uss = get_stats()

    # 4. Formatting Output
    def to_mb(b):
        return b / (1024 * 1024)

    print(f"\n{'Metric':<20} | {'Baseline':<12} | {'Final':<12} | {'Delta':<12}")
    print("-" * 65)

    p_delta = final_private - base_private
    print(
        f"{'Private Bytes':<20} | {to_mb(base_private):>8.2f} MB | {to_mb(final_private):>8.2f} MB | {to_mb(p_delta):>+8.2f} MB"
    )

    u_delta = final_uss - base_uss
    print(
        f"{'USS (Unique Set)':<20} | {to_mb(base_uss):>8.2f} MB | {to_mb(final_uss):>8.2f} MB | {to_mb(u_delta):>+8.2f} MB"
    )

    print("-" * 65)
    if p_delta > u_delta * 1.5:
        print("\n[!] WARNING: Private Bytes are significantly higher than USS.")
        print("    This suggests heavy heap fragmentation or memory claimed by C-extensions/Pools")
        print("    that hasn't been mapped to the physical Working Set yet.")


if __name__ == "__main__":
    run_memory_test()
