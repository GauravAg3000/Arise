import asyncio
import contextlib
import logging
import time
from datetime import datetime

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from shared.constants import STREAM_KEY
from worker.db_router import DatabaseRouter
from worker.utils import should_flush

logger = logging.getLogger(__name__)

GROUP = "arise-workers"
BATCH_SIZE = 100
BATCH_TIMEOUT = 1.0
READ_COUNT = 50
BLOCK_MS = 1000


async def ensure_consumer_group(redis: Redis) -> None:
    # expect ResponseError and pass!
    with contextlib.suppress(ResponseError):
        await redis.xgroup_create(STREAM_KEY, GROUP, id="0", mkstream=True)


async def consume(
    redis: Redis,
    router: DatabaseRouter,
    worker_id: str,
    shutdown_event: asyncio.Event,
) -> None:
    await ensure_consumer_group(redis)

    logger.info("worker started | id=%s group=%s", worker_id, GROUP)

    buffer: list[tuple[str, dict]] = []
    last_flush = time.monotonic()

    while not shutdown_event.is_set():
        try:
            result = await redis.xreadgroup(
                GROUP,
                worker_id,
                {STREAM_KEY: ">"},
                count=READ_COUNT,
                block=BLOCK_MS,
            )
        except Exception:
            logger.exception("xreadgroup error")
            await asyncio.sleep(1)
            continue

        if result:
            for _, messages in result:
                for msg_id, fields in messages:
                    buffer.append((msg_id, fields))
                logger.info(
                    "consumed | count=%s first_id=%s",
                    len(messages),
                    messages[0][0] if messages else "?",
                )

        if should_flush(buffer, last_flush, BATCH_SIZE, BATCH_TIMEOUT):
            await _flush(redis, router, buffer)
            buffer.clear()
            last_flush = time.monotonic()

    if buffer:
        logger.info("flushing remaining %s events on shutdown", len(buffer))
        await _flush(redis, router, buffer)


async def _flush(
    redis: Redis,
    router: DatabaseRouter,
    buffer: list[tuple[str, dict]],
) -> None:
    events = []
    msg_ids = []
    for msg_id, fields in buffer:
        msg_ids.append(msg_id)
        events.append(
            {
                "event_id": fields.get("event_id"),
                "event_type": fields.get("event_type"),
                "payload": fields.get("payload", "{}"),
                "received_at": datetime.fromisoformat(str(fields.get("received_at"))),
                "request_id": fields.get("request_id"),
                "trace_id": fields.get("trace_id"),
            },
        )

    await router.insert(events)

    await redis.xack(STREAM_KEY, GROUP, *msg_ids)
    logger.info(
        "acknowledged | count=%s last_id=%s",
        len(msg_ids),
        msg_ids[-1] if msg_ids else "?",
    )
