import logging
import os
import signal
import subprocess
import sys

logger = logging.getLogger(__name__)

_WORKER_COMMAND = (
    sys.executable,
    "-m",
    "agent.cli",
    "worker",
)


class WorkerPool:
    """Pre-fork process pool for Redis stream consumers.

    Spawns N independent child processes, each consuming from the same
    consumer group with a unique ID. The parent handles SIGINT/SIGTERM,
    terminates all children, and waits for graceful exit.

    Usage::

        WorkerPool(size=3).run()
    """

    def __init__(self, size: int):
        self._size = size
        self._processes: list[subprocess.Popen] = []

    def run(self) -> None:
        self.start()
        self.wait()

    def start(self) -> None:
        self._register_signal_handlers()

        for index in range(self._size):
            self._spawn_worker(index)

    def stop(self) -> None:
        logger.info("Stopping %d workers...", len(self._processes))

        for proc in self._processes:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass

    def wait(self) -> None:
        logger.info("Waiting for workers to exit...")

        for proc in self._processes:
            proc.wait()

        logger.info("All workers exited.")

    def _register_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, signum, frame) -> None:
        logger.info("Received signal %s, stopping workers...", signum)
        self.stop()

    def _spawn_worker(self, index: int) -> None:
        env = os.environ.copy()
        env["WORKER_INDEX"] = str(index)

        proc = subprocess.Popen(_WORKER_COMMAND, env=env)
        self._processes.append(proc)

        logger.info("Spawned worker-%s (pid=%s)", index, proc.pid)
