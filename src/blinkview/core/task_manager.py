# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

import threading
import time
from typing import Callable


class TaskManager:
    def __init__(self, max_workers: int = 5):
        from concurrent.futures import ThreadPoolExecutor

        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        self._periodic_tasks: dict[str, dict] = {}

        # Replace the Lock and Event with a single Condition object
        self._condition = threading.Condition()
        self._running = True

        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()

    def run_task(self, func: Callable, *args, **kwargs) -> "Future":
        """Runs a one-off task immediately in the thread pool."""
        return self.executor.submit(func, *args, **kwargs)

    def run_periodic(self, interval_seconds: float, func: Callable, *args, **kwargs) -> str:
        """Registers a task and wakes the scheduler to recalculate its sleep time."""

        import uuid

        task_id = str(uuid.uuid4())

        with self._condition:
            self._periodic_tasks[task_id] = {
                "interval": interval_seconds,
                "func": func,
                "args": args,
                "kwargs": kwargs,
                "next_run": time.time() + interval_seconds,
            }
            # Interrupt the scheduler's sleep so it knows about the new task
            self._condition.notify()

        return task_id

    def stop_periodic(self, task_id: str):
        """Removes a periodic task and updates the scheduler."""
        with self._condition:
            if self._periodic_tasks.pop(task_id, None):
                # Notify just in case we deleted the task the scheduler was waiting for
                self._condition.notify()

    def _scheduler_loop(self):
        """The precision clock thread that sleeps exactly as long as needed."""
        while True:
            with self._condition:
                if not self._running:
                    break

                now = time.time()
                next_wakeup = None

                # Dispatch due tasks and update their next run times
                for task in self._periodic_tasks.values():
                    if now >= task["next_run"]:
                        self.executor.submit(task["func"], *task["args"], **task["kwargs"])
                        task["next_run"] = now + task["interval"]

                # Find the earliest upcoming task
                if self._periodic_tasks:
                    next_wakeup = min(task["next_run"] for task in self._periodic_tasks.values())

                if not self._running:
                    break

                # Sleep until the next task is due, or until interrupted
                if next_wakeup is None:
                    # No tasks registered. Sleep indefinitely until .notify() is called
                    self._condition.wait()
                else:
                    sleep_time = next_wakeup - time.time()
                    if sleep_time > 0:
                        # Sleep exactly `sleep_time` seconds, unless interrupted
                        self._condition.wait(timeout=sleep_time)

    def shutdown(self, wait=True):
        """Kills the scheduler immediately and shuts down the worker pool."""
        with self._condition:
            self._running = False
            self._condition.notify()  # Instantly wake the scheduler so it can exit

        self.executor.shutdown(wait=wait)
