# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import signal
import sys
from queue import Empty, Queue
from threading import Thread

import readchar

# Inject CWD before importing local blinkview modules
cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.insert(0, cwd)

from rich.console import Console

# Internal BlinkView imports
from .core.registry import Registry
from .io.uart import UARTReader
from .parsers.line_parser import LineParser
from .parsers.text_filter import TextFilter
from .storage.file_logger import BinaryBatchProcessor, FileLogger, LogRowBatchProcessor
from .subscribers.console import ConsoleSubscriber
from .utils.level_map import LogLevel


class BlinkViewApp:
    def __init__(self):
        self.console = Console()
        self.registry = Registry("test")
        self.console_sub = None

        self.input_queue = Queue()
        self.running = False

    def init_console(self, level=LogLevel.ALL):
        """Lazy-loads and configures the console subscriber."""
        if self.console_sub is None:
            self.console_sub = self.registry.build_subscriber("CLI", "Console", console=self.console)
            # self.console_sub = ConsoleSubscriber(self.console, self.registry)
            self.console_sub.start()

        self.console_sub.set_level(level)

    def _signal_handler(self, signum, frame):
        """Catches OS signals to shut down cleanly."""
        self.input_queue.put("q")

    def _keyboard_listener(self):
        """Background thread to catch raw keystrokes."""
        while self.running:
            try:
                key = readchar.readkey().lower()
                self.input_queue.put(key)
                if key == "q":
                    break
            except KeyboardInterrupt:
                self.input_queue.put("q")
                break

    def handle_input(self, key: str):
        """Routes the parsed keystrokes to the correct actions."""
        if key == "q":
            self.console.print("\n[bold yellow]Quit signal received...[/bold yellow]")
            self.running = False

        elif key in ("i", "a", "d", "e", "w"):
            # Map keys directly to LogLevels
            level_map = {
                "a": LogLevel.ALL,
                "d": LogLevel.DEBUG,
                "i": LogLevel.INFO,
                "w": LogLevel.WARN,
                "e": LogLevel.ERROR,
            }
            if self.console_sub:
                self.console_sub.set_level(level_map[key])

        elif key == "r":
            # destroy and recreate registry
            is_console_enabled = self.console_sub is not None
            if is_console_enabled:
                log_level = self.console_sub.log_level
                self.console_sub.stop()
            self.registry.stop()
            self.console_sub = None
            self.registry = Registry("test")
            self.registry.start()
            self.console.print("[bold cyan]Registry restarted.[/bold cyan]")
            if is_console_enabled:
                self.init_console(log_level)

        elif key == "s":
            if self.console_sub is None:
                self.init_console()
            else:
                self.console_sub.streaming = not self.console_sub.streaming
                state = "resumed" if self.console_sub.streaming else "paused"
                self.console.print(f"Streaming {state}. Press 's' to toggle.")

    def run(self):
        """Main application loop."""
        self.console.print("Starting BlinkView CLI...")

        # Wire up OS signals
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.registry.start()
        self.init_console(LogLevel.WARN)

        self.console.print('[bold green]BLINK ACTIVE[/bold green] | [dim]Press "s" to pause, "q" to quit[/dim]')

        self.running = True
        listener_thread = Thread(target=self._keyboard_listener, daemon=True)
        listener_thread.start()

        try:
            while self.running:
                try:
                    # Timeout is required here to make the UI loop responsive
                    key = self.input_queue.get(timeout=0.05)
                    self.handle_input(key)
                except Empty:
                    # Do your 20Hz background UI updates here if needed
                    pass

        finally:
            self.console.print("Exiting...")
            if self.console_sub:
                self.console_sub.stop()
            self.registry.stop()
            self.console.print("[grey50]BlinkView session ended.[/grey50]")


def main():
    app = BlinkViewApp()
    app.run()


if __name__ == "__main__":
    main()
