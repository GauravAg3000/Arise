import asyncio
import logging
import secrets

import httpx
from uuid_extensions import uuid7

from agent.batching import BatchBuffer
from agent.config import GATEWAY_ENDPOINT
from agent.utils import parse_duration
from shared.schemas import EventBatch

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
        for attempt in range(10):
            response = await self.client.post(
                f"{GATEWAY_ENDPOINT}/api/v1/events/batch",
                content=payload,
                headers={"Content-Type": "application/json", "traceparent": traceparent},
            )
            if response.status_code in (429, 503):
                retry_after = float(response.headers.get("Retry-After", 5))
                reason = "rate limited" if response.status_code == 429 else "service unavailable"
                logger.warning(
                    "%s | batch_id=%s attempt=%s/10 retry_after=%ss",
                    reason,
                    batch.batch_id,
                    attempt + 1,
                    retry_after,
                )
                await asyncio.sleep(retry_after)
                continue
            response.raise_for_status()
            break
        else:
            logger.error(
                "rate limit retries exhausted, dropping batch | batch_id=%s events=%s",
                batch.batch_id,
                len(batch.events),
            )
            return
        body = response.json()
        logger.info(
            "batch sent | size=%s request_id=%s",
            len(batch.events),
            body["request_id"],
        )

    async def close(self):
        await self.client.aclose()


async def stream_events(queue: asyncio.Queue, config):
    max_age_s = parse_duration(config.batch_timeout)
    buffer = BatchBuffer(config.batch_size, max_age_s)
    streamer = HTTPStreamer(config.dry_run)
    total = 0

    logger.info(
        "streamer started | batch_size=%s timeout=%ss",
        config.batch_size,
        max_age_s,
    )

    async def _send_or_requeue(batch: EventBatch) -> int:
        try:
            await streamer.send(batch)
            return len(batch.events)
        except httpx.TransportError:
            logger.exception(
                "gateway unreachable, re-enqueueing %s events",
                len(batch.events),
            )
            for event in batch.events:
                await queue.put(event)
            return 0

    while True:
        try:
            event = await asyncio.wait_for(queue.get(), timeout=POLL_INTERVAL)
            if event is None:
                break

            batch = buffer.add(event)
            if batch:
                # Batch full --> Sending batch
                total += await _send_or_requeue(batch)

        except TimeoutError:
            # Batch timeout --> Sending batch (whatever events are accumulated in batch till now)
            batch = buffer.flush_if_expired()
            if batch:
                total += await _send_or_requeue(batch)

    remaining = buffer.flush()
    if remaining:
        try:
            await streamer.send(remaining)
            total += len(remaining.events)
        except httpx.TransportError:
            logger.error(
                "gateway unreachable on shutdown, dropping %s events",
                len(remaining.events),
            )

    await streamer.close()
    logger.info("streamer complete | total=%s", total)
    return total
