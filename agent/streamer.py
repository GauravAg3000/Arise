import asyncio
import logging
import secrets
import httpx
from uuid_extensions import uuid7
from shared.schemas import EventBatch
from agent.config import GATEWAY_ENDPOINT
from agent.batching import BatchBuffer
from agent.utils import parse_duration

logger = logging.getLogger(__name__)

POLL_INTERVAL = 1.0


class HTTPStreamer:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.client = httpx.AsyncClient(timeout=10.0)

    async def send(self, batch: EventBatch):
        if self.dry_run:
            logger.info("dry-run batch | size=%s", len(batch.events))
            return

        trace_id = str(uuid7()).replace("-", "")
        parent_id = secrets.token_hex(8)
        traceparent = f"00-{trace_id}-{parent_id}-01"

        payload = batch.model_dump_json()
        response = await self.client.post(
            f"{GATEWAY_ENDPOINT}/api/v1/events/batch",
            content=payload,
            headers={"Content-Type": "application/json", "traceparent": traceparent},
        )
        response.raise_for_status()

    async def close(self):
        await self.client.aclose()


async def stream_events(queue: asyncio.Queue, config):
    max_age_s = parse_duration(config.batch_timeout)
    buffer = BatchBuffer(config.batch_size, max_age_s)
    streamer = HTTPStreamer(config.dry_run)
    total = 0

    logger.info("streamer started | batch_size=%s timeout=%ss", config.batch_size, max_age_s)

    # Streamer logic
    while True:
        try:
            event = await asyncio.wait_for(queue.get(), timeout=POLL_INTERVAL)
            if event is None:
                break

            batch = buffer.add(event)
            if batch:
                # Batch full --> Sending batch
                await streamer.send(batch)
                total += len(batch.events)

        except asyncio.TimeoutError:
            # Batch timeout --> Sending batch (whatever events are accumulated in batch till now)
            batch = buffer.flush_if_expired()
            if batch:
                await streamer.send(batch)
                total += len(batch.events)

    remaining = buffer.flush()
    if remaining:
        await streamer.send(remaining)
        total += len(remaining.events)

    await streamer.close()
    logger.info("streamer complete | total=%s", total)
    return total
