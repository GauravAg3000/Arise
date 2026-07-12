import logging
import os
import socket

from dotenv import load_dotenv

from shared.settings import WorkerSettings
from worker.consumer import consume
from worker.db_router import DatabaseRouter
from worker.redis_client import new_redis_client
from worker.utils import register_shutdown

load_dotenv()

settings = WorkerSettings()  # type: ignore

logger = logging.getLogger(__name__)

WORKER_INDEX = os.getenv("WORKER_INDEX", "0")
WORKER_ID = f"worker-{socket.gethostname()}-{WORKER_INDEX}"


async def run_worker() -> None:
    redis = new_redis_client(settings.redis_host, settings.redis_port)

    router = DatabaseRouter(settings)
    await router.connect(redis)

    shutdown_event = register_shutdown()

    try:
        await consume(redis, router, WORKER_ID, shutdown_event)
    finally:
        await router.close()
        await redis.aclose()
