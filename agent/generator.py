import asyncio
import logging
from shared.schemas import Event
from agent.utils import parse_duration


logger = logging.getLogger(__name__)


async def generate_events(queue: asyncio.Queue, config):
    rate = config.rate
    duration_s = parse_duration(config.duration)
    interval = 1.0 / rate
    deadline = asyncio.get_running_loop().time() + duration_s
    count = 0

    logger.info("generating events | rate=%s duration=%ss", rate, duration_s)

    # Producer logic
    while asyncio.get_running_loop().time() < deadline:
        event = Event(type="page_view", payload={"path": "/home", "seq": count})
        await queue.put(event)
        count += 1
        await asyncio.sleep(interval)

    logger.info("generation complete | total=%s", count)
