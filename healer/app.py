import logging
import sys

from dotenv import load_dotenv

from healer.healer import Healer
from healer.utils import register_shutdown
from shared.settings import HealerSettings

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


async def run_healer() -> None:
    settings = HealerSettings()

    healer = Healer(settings)
    await healer.start()

    shutdown_event = register_shutdown()

    try:
        await healer.daemon_loop(shutdown_event)
    finally:
        await healer.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(run_healer())
