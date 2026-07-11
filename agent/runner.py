import asyncio
import logging

from agent.config import ProduceConfig
from agent.generator import generate_events
from agent.streamer import stream_events


async def run_producer(config: ProduceConfig):
    # very fast asyncio queue, lives in running python process
    queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

    # Producer -> Consumer Pattern
    # - Starting Producer task
    producer = asyncio.create_task(generate_events(queue, config))
    # - Starting Streamer task
    streamer = asyncio.create_task(stream_events(queue, config))
    # Both producer and streamer tasks are now running concurrently

    await producer

    # Sending stop signal (None) to Streamer
    await queue.put(None)
    total = await streamer

    logger = logging.getLogger(__name__)
    logger.info("done | total=%s", total)
