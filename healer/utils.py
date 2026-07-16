import asyncio
import contextlib
import logging
import signal

logger = logging.getLogger(__name__)


def register_shutdown() -> asyncio.Event:
    shutdown = asyncio.Event()

    def _on_signal() -> None:
        logger.info("shutdown signal received")
        shutdown.set()

    loop = asyncio.get_running_loop()
    with contextlib.suppress(ValueError, OSError, AttributeError):
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _on_signal)

    return shutdown
