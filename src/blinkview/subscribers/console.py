# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Thread

from .subscriber import BaseSubscriber, SubscriberFactory
from ..core.log_row import LogRow
from rich.text import Text
from time import sleep

from ..utils.level_map import LogLevel
from ..utils.time_utils import ConsoleTimestampFormatter


@SubscriberFactory.register("console")
class ConsoleSubscriber(BaseSubscriber):
    def __init__(self, console):
        super().__init__()

        self.console = console

        self.streaming = True

        self.log_level = LogLevel.ALL

        self.enabled = True

    def set_level(self, level: LogLevel):
        self.log_level = level

    def run(self):
        timestamp_formatter = ConsoleTimestampFormatter()
        format_ts = timestamp_formatter.format  # Localized!
        queue_get = self._queue.get
        c_print = self.console.print  # Localized!
        TextCls = Text

        batch_text = None

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            msg_batch = queue_get()
            if msg_batch is not None:
                for msg in msg_batch:
                    msg: LogRow

                    if msg.level < self.log_level:
                        continue

                    if self.streaming:
                        if batch_text is None:
                            batch_text = TextCls()

                        # Format line
                        ts_str = format_ts(msg.timestamp_ns)
                        line = f"{ts_str} {msg.level} {msg.module.device} {msg.module.name} \t{msg.message}\n"

                        # Append to the consolidated Rich Text object
                        batch_text.append(line, style=msg.level.color)
                    else:
                        batch_text = None  # Drop batch if not streaming

            if batch_text is not None:
                # ONE call to rule them all.
                # soft_wrap=True prevents Rich from doing expensive line-break calculations
                c_print(batch_text, soft_wrap=True, end="")
                batch_text = None

            sleep(1.0 / 20)
