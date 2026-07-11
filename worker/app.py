import logging
import socket
from dotenv import load_dotenv

from worker.consumer import consume
from worker.redis_client import new_redis_client
from worker.repository import create_pool, init_db
from worker.utils import register_shutdown

load_dotenv()

logger = logging.getLogger(__name__)

WORKER_ID = f"worker-{socket.gethostname()}"


async def run_worker() -> None:
    redis = new_redis_client()
    pool = await create_pool()

    await init_db(pool)

    shutdown_event = register_shutdown()

    try:
        await consume(redis, pool, WORKER_ID, shutdown_event)
    finally:
        await redis.aclose()
        await pool.close()
