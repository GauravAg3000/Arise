import asyncio
import logging
import signal
import time

logger = logging.getLogger(__name__)


def register_shutdown() -> asyncio.Event:
    shutdown = asyncio.Event()

    def _on_signal() -> None:
        logger.info("shutdown signal received")
        shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_signal)

    return shutdown


def should_flush(
    buffer: list, last_flush: float, batch_size: int, batch_timeout: float
) -> bool:
    if not buffer:
        return False
    now = time.monotonic()
    return len(buffer) >= batch_size or now - last_flush >= batch_timeout
