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

from blinkview.core.base_reorder import ReorderFactory
from blinkview.core.id_registry import IDRegistry
from blinkview.core.module_snapshot import LatestModuleValueTracker
from blinkview.core.reorderer import Reorder
from blinkview.parsers import adb_decoder, frame_decoders, frame_parsers
from blinkview.subscribers import subscriber

from ..io import *
from ..io.BaseReader import DeviceFactory
from ..parsers import *
from ..storage import *
from ..storage.file_logger import FileLogger
from ..storage.file_manager import FileManager
from ..subscribers.subscriber import SubscriberFactory
from ..utils import level_map
from ..utils.time_utils import TimeUtils
from .array_pool import NumpyArrayPool
from .central_storage import CentralFactory
from .config_manager import ConfigManager
from .factory_registry import FactoryRegistry
from .logger import PrintLogger, SystemLogger
from .numpy_batch_manager import PooledLogBatch
from .plugin_manager import PluginManager
from .reusable_batch_pool import PoolManager
from .settings_manager import SettingsManager
from .sources import SourcesManager
from .system_context import SystemContext
from .task_manager import TaskManager

if TYPE_CHECKING:
    from .central_storage import CentralStorage
    from .pipeline_manager import PipelineManager


class Registry:
    def __init__(
        self,
        session_name: str = None,
        config_path: str = None,
        profile_name: str = None,
        log_dir: str | Path = None,
        settings=None,
    ):
        # ==========================================
        # LAYER 1: Core Services
        # ==========================================

        self.initialized = False

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

        factories = FactoryRegistry()
        factories.register("reorder", ReorderFactory)
        factories.register("central", CentralFactory)
        factories.register("source", DeviceFactory)
        factories.register("parser", parser.ParserFactory)
        factories.register("time_sync", subscriber.TimeSyncerFactory)
        factories.register("pipeline_transformer", transformer.TransformerFactory)
        factories.register("pipeline_assembler", assembler.AssemblerFactory)
        factories.register("pipeline_printable", transformer.PipelinePrintableFactory)
        factories.register("pipeline_decode", transformer.PipelineDecodeFactory)
        factories.register("pipeline_transform", transformer.PipelineTransformFactory)
        factories.register("can_parser", can_bus.CanParserFactory)
        factories.register("can_assembler", can_bus.CanAssemblerFactory)
        factories.register("can_decode", can_bus.CanDecoderFactory)
        factories.register("can_transform", can_bus.CanTransformFactory)
        # factories.register("log_level_map", level_map.LogLevelMapFactory)
        factories.register("logging_processor", file_logger.BatchProcessorFactory)
        factories.register("file_logging", file_logger.FileLoggerFactory)
        factories.register("frame_decoder", frame_decoders.FrameDecoderFactory)
        factories.register("frame_parser", frame_parsers.FrameParserFactory)
        factories.register("frame_section_parser", frame_parsers.FrameSectionParserFactory)

        self.file_manager = FileManager(
            session_name=session_name, profile_name=profile_name, log_dir=log_dir, config_path=config_path
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
            "central": factories.get_produced_type("central"),
            "reorder": factories.get_produced_type("reorder"),
        }

        print(f"[Registry] key_to_base_class mapping: {self.key_to_base_class}")

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

        # ==========================================
        # LAYER 2: Storage & Sinks
        # ==========================================
        # Initialize the file manager for this session

        # Snapshot the logic immediately
        # self.file_manager.save_snapshot(["src/", "configs/"])

        self.id_registry = IDRegistry(np_pool)

        self.system_ctx = SystemContext(
            time_ns=self.now_ns,
            registry=self,
            id_registry=self.id_registry,
            factories=factories,
            tasks=TaskManager(),
            settings=settings or SettingsManager(),
            pool=PoolManager(),
            array_pool=np_pool,
        )
        self.file_manager.set_context(self.system_ctx)

        self.system_device = self.id_registry.get_device("SYSTEM")
        self.log_device_id = self.system_device.id

        self.sources = None

        # ==========================================
        # LAYER 4: Hardware Pipelines
        # ==========================================
        self.pipelines: "PipelineManager" = None

        self._is_running = False

        self.central: "CentralStorage" = None
        self.reorder = None

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

        ctx = target.logger.ctx
        target.logger = self.logger_creator(ctx)()

    def logger_creator(self, category: str, name: str = None):
        if not self.initialized:
            return lambda: PrintLogger(category, name, self._temp_log_queue.put, self.now_ns)

        return lambda: SystemLogger(category, name, self)

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

    def stop(self):
        """Cleanly tear down the session."""
        if not self._is_running:
            return

        if self.sources is not None:
            self.sources.stop()

        if self.pipelines is not None:
            self.pipelines.stop()

        if self.reorder is not None:
            self.reorder.stop()
        if self.central is not None:
            self.central.stop()

        self.file_manager.stop()

        for sub in self._subscribers.copy():
            stop_fn = getattr(sub, "stop", None)
            if stop_fn is not None:
                sub.stop()
            self.unsubscribe(sub)

        self.system_ctx.tasks.shutdown()

        self._is_running = False
        print("Session stopped.")

    def configure_system(self):
        try:
            print(f"[Registry] Configuring system with session name: {self.session_name}")
            # base configuration
            if self.initialized:
                return

            # print()
            self.plugins.apply_config(self.config.get_by_path("/plugins"))
            print(f"[Registry] Applied plugin configuration.")

            system_ctx = self.system_ctx
            factories = system_ctx.factories

            try:
                reorder_config = self.config.get_by_path("/reorder")
                if reorder_config:  # is not None and reorder_config.get("enabled", True):
                    self.logger.info(f"[System] reorder_config: {reorder_config}")
                    if reorder_config.get("type") is None:
                        reorder_config["type"] = "default"

                    local_ctx = SimpleNamespace(get_logger=self.logger_creator("reorder"))

                    self.reorder = factories.build("reorder", reorder_config, system_ctx, local_ctx)

                    self.config.subscribe("/reorder", self.reorder)

                    self.reorder.reference_id = "reorder"
            except Exception as e:
                print(f"[Registry] Error configuring reorder buffer: {e}")
                self.logger.error(f"Error configuring reorder buffer:", e)

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

                    self.central = factories.build("central", central_storage_config, system_ctx, local_ctx)
                    if self.reorder is not None:
                        self.reorder.subscribe(self.central)

                    self.config.subscribe("/central", self.central)

                    self.central.reference_id = "central"
            except Exception as e:
                # print(f"[Registry] Error configuring central storage: {e}")
                self.logger.exception("Error configuring central storage", e)

            self.initialized = True

            self._dump_temp_logs()

            self.logger.info(f"[System] System initialized with session name: {self.session_name}")

            self.reinit_logger(self)
            self.logger.info(f"[System] Registry logger initialized.")

            self.reinit_logger(self.id_registry)
            self.reinit_logger(self.reorder)
            self.reinit_logger(self.central)

            try:
                print(f"[Registry] Configuring sources")
                self.sources = self._create_and_bind(SourcesManager, "sources", self.config.get_by_path("/sources"))
                self.config.subscribe("/sources", self.sources)
            except Exception as e:
                print(f"[Registry] Error during sources configuration: {e}")
                self.logger.error(f"Error during sources configuration", e)
            try:
                from blinkview.core.pipeline_manager import PipelineManager

                self.pipelines = self._create_and_bind(
                    PipelineManager, "pipelines", self.config.get_by_path("/pipelines")
                )

                self.config.subscribe("/pipelines", self.pipelines)
                self.pipelines.apply_targets()
            except Exception as e:
                print(f"[Registry] Error during pipelines configuration: {e}")
                self.logger.error(f"Error during pipelines configuration", e)

            try:
                if self.sources is not None:
                    self.sources.apply_targets()
            except Exception as e:
                print(f"[Registry] Error during applying source targets: {e}")
                self.logger.error(f"Error during applying source targets", e)

        except Exception as e:
            print(f"[Registry] Error during system configuration: {e}")
            self.logger.error(f"Error during system configuration", e)

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

    def start(self, configure=True):
        if self._is_running:
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

        if self.pipelines is not None:
            self.pipelines.start()

        if self.sources is not None:
            self.sources.start()

        # self.system_ctx.tasks.run_periodic(1, self.buffer_stats)
        tasks = self.system_ctx.tasks
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

            print(f"\n[BUFFER_STATS]\n" + "\n".join(lines) + "\n")

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
