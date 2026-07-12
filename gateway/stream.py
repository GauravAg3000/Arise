import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from shared.constants import STREAM_KEY
from shared.schemas import EventBatch

logger = logging.getLogger(__name__)


async def publish_batch(
    redis: Redis,
    batch: EventBatch,
    request_id: str,
    trace_id: str | None,
    received_at: str,
) -> None:
    async with redis.pipeline() as pipe:
        for event in batch.events:
            pipe.xadd(
                STREAM_KEY,
                {
                    "event_id": event.event_id,
                    "event_type": event.type,
                    "payload": json.dumps(event.payload, default=str),
                    "received_at": received_at,
                    "request_id": request_id,
                    "trace_id": trace_id or "",
                },
                maxlen=100000,
                approximate=True,
            )
        try:
            await pipe.execute()
        except RedisError:
            logger.exception(
                "redis unavailable | batch_id=%s events=%s",
                batch.batch_id,
                len(batch.events),
            )
            return

        logger.info(
            "enqueued | events=%s stream=%s batch_id=%s",
            len(batch.events),
            STREAM_KEY,
            batch.batch_id,
        )
