import logging
import os
import socket

from dotenv import load_dotenv

from shared.settings import WorkerSettings
from worker.consumer import consume
from worker.redis_client import new_redis_client
from worker.repository import create_pool, init_db
from worker.utils import register_shutdown

load_dotenv()

settings = WorkerSettings() # type: ignore

logger = logging.getLogger(__name__)

WORKER_INDEX = os.getenv("WORKER_INDEX", "0")
WORKER_ID = f"worker-{socket.gethostname()}-{WORKER_INDEX}"


async def run_worker() -> None:
    redis = new_redis_client(settings.redis_host, settings.redis_port)
    pool = await create_pool(
        host=settings.pg_host,
        port=settings.pg_port,
        user=settings.pg_user,
        password=settings.pg_password,
        database=settings.pg_database,
    )

    await init_db(pool)

    shutdown_event = register_shutdown()

    try:
        await consume(redis, pool, WORKER_ID, shutdown_event)
    finally:
        await redis.aclose()
        await pool.close()
