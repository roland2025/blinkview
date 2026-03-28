# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
from typing import TYPE_CHECKING, Callable, Optional

from ..io import *
from ..io.BaseReader import DeviceFactory
from ..parsers import *
from ..storage import *
from ..storage.file_logger import FileLogger, LogRowBatchProcessor
from ..storage.file_manager import FileManager
from ..subscribers.subscriber import SubscriberFactory
from ..utils import level_map
from ..utils.time_utils import TimeUtils
from .bisect_reorder import Reorder
from .central_storage import BaseCentralStorage, CentralFactory, CentralStorage
from .config_manager import ConfigManager
from .factory_registry import FactoryRegistry
from .id_registry import IDRegistry
from .log_row import LogRow
from .logger import PrintLogger, SystemLogger
from .plugin_manager import PluginManager
from .reorder_buffer import ReorderBuffer, ReorderFactory
from .settings_manager import SettingsManager
from .sources import SourcesManager
from .system_context import SystemContext
from .task_manager import TaskManager

if TYPE_CHECKING:
    from .pipeline_manager import PipelineManager


class Registry:
    def __init__(
        self, session_name: str, config_path: str = None, profile_name: str = None, log_dir: str | Path = None
    ):
        # ==========================================
        # LAYER 1: Core Services
        # ==========================================

        self.initialized = False
        self._temp_log_queue: Queue = Queue()

        self.time_utils = TimeUtils()
        self.now = self.time_utils.now
        self.now_ns = self.time_utils.now_ns

        self.logger = self.logger_creator("registry")()

        factories = FactoryRegistry()
        factories.register("reorder", ReorderFactory)
        factories.register("central", CentralFactory)
        factories.register("source", DeviceFactory)
        factories.register("parser", parser.ParserFactory)
        factories.register("pipeline_transformer", transformer.TransformerFactory)
        factories.register("pipeline_assembler", assembler.AssemblerFactory)
        factories.register("pipeline_printable", transformer.PipelinePrintableFactory)
        factories.register("pipeline_decode", transformer.PipelineDecodeFactory)
        factories.register("pipeline_transform", transformer.PipelineTransformFactory)
        factories.register("can_parser", can_bus.CanParserFactory)
        factories.register("can_assembler", can_bus.CanAssemblerFactory)
        factories.register("can_decode", can_bus.CanDecoderFactory)
        factories.register("can_transform", can_bus.CanTransformFactory)
        factories.register("log_level_map", level_map.LogLevelMapFactory)
        factories.register("logging_processor", file_logger.BatchProcessorFactory)
        factories.register("file_logging", file_logger.FileLoggerFactory)

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

        # ==========================================
        # LAYER 2: Storage & Sinks
        # ==========================================
        # Initialize the file manager for this session

        # Snapshot the logic immediately
        # self.file_manager.save_snapshot(["src/", "configs/"])

        self.id_registry = IDRegistry()

        self.system_ctx = SystemContext(
            time_ns=self.now_ns,
            registry=self,
            id_registry=self.id_registry,
            factories=factories,
            tasks=TaskManager(),
            settings=SettingsManager(),
        )
        self.file_manager.set_context(self.system_ctx)

        self.sources = None

        # ==========================================
        # LAYER 4: Hardware Pipelines
        # ==========================================
        self.pipelines: "PipelineManager" = None

        self._is_running = False

        self.central = None
        self.reorder = None

    def _create_and_bind(self, cls, name, config):
        local_ctx = SimpleNamespace(get_logger=self.logger_creator(name))
        instance = cls()
        if hasattr(instance, "bind_system"):
            instance.bind_system(self.system_ctx, local_ctx)
        if hasattr(instance, "apply_config"):
            instance.apply_config(config)
        self.config.subscribe(f"/{name}", instance)
        return instance

    def reinit_logger(self, target):
        if target is None:
            return

        ctx = target.logger.ctx
        target.logger = self.logger_creator(ctx)()

    def logger_creator(self, category: str, name: str = None):
        if not self.initialized:
            return lambda: PrintLogger(category, name, self._temp_log_queue, self.now_ns)

        return lambda: SystemLogger(category, name, self)

    def get_device(self, device_name):
        return self.id_registry.get_device(device_name)

    def get_registry_schema(self, key: str):
        if hasattr(self, key):
            obj = getattr(self, key)
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
            except Exception as e:
                print(f"[Registry] Error configuring central storage: {e}")
                self.logger.error(f"Error configuring central storage", e)

            self.initialized = True

            self._dump_temp_logs()

            self.logger.info(f"[System] System initialized with session name: {self.session_name}")

            self.reinit_logger(self)
            self.logger.info(f"[System] Registry logger initialized.")

            self.reinit_logger(self.id_registry)
            self.reinit_logger(self.reorder)
            self.reinit_logger(self.central)

            try:
                self.sources = self._create_and_bind(SourcesManager, "sources", self.config.get_by_path("/sources"))
            except Exception as e:
                print(f"[Registry] Error during sources configuration: {e}")
                self.logger.error(f"Error during sources configuration", e)
            try:
                from blinkview.core.pipeline_manager import PipelineManager

                self.pipelines = self._create_and_bind(
                    PipelineManager, "pipelines", self.config.get_by_path("/pipelines")
                )
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
        log_put_fn = self.reorder.put if self.reorder else self.central.put
        module = self.get_device("SYSTEM")
        log_batch = []
        while True:
            try:
                timestamp, module_name, level_id, msg = self._temp_log_queue.get_nowait()
                log_batch.append(LogRow(timestamp, level_id, module.get_module(module_name), msg))
            except Exception:
                break
        log_put_fn(log_batch)

    def start(self):
        if self._is_running:
            return
        self.logger.warn(f"--- Starting Session: {self.session_name} ---")

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

        self._is_running = True
        self.logger.warn("BlinkView is now live.")

    #
    # def add_parser_consumer(self, consumer):
    #     self.parser_thread.add_consumer(consumer.put_many)
    #
    # def add_raw_parser_consumer(self, consumer):
    #     self.parser_thread.add_raw_consumer(consumer.put_many)

    def build_subscriber(self, name, subscriber_type: str, config=None, **kwargs):
        if config is None:
            config = {"type": subscriber_type}

        local_ctx = SimpleNamespace(get_logger=self.logger_creator(subscriber_type))
        subscriber = SubscriberFactory.build(config, self.system_ctx, local_ctx, **kwargs)
        self.pipelines.subscribe(name, subscriber)
        return subscriber

    def subscribe(self, subscriber):
        self.central.subscribe(subscriber)

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
