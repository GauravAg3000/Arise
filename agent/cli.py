import asyncio
import logging
import typer
from agent.config import ProduceConfig
from agent.generator import generate_events
from agent.streamer import stream_events


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)

app = typer.Typer()


@app.callback()
def main():
    pass


@app.command()
def produce(
    rate: int = typer.Option(100, "--rate", "-r", help="Events per second"),
    duration: str = typer.Option(
        "30s", "--duration", "-d", help="Duration (e.g., 30s, 5m, 1h)"
    ),
    batch_size: int = typer.Option(
        100, "--batch-size", "-b", help="Max events per batch"
    ),
    batch_timeout: str = typer.Option(
        "1s", "--batch-timeout", "-t", help="Max batch age (e.g., 500ms, 2s)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n", help="Print batches without sending"
    ),
):
    config = ProduceConfig(
        rate=rate,
        duration=duration,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        dry_run=dry_run,
    )
    asyncio.run(run_produce(config))


async def run_produce(config: ProduceConfig):
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


@app.command()
def gateway():
    """Start the FastAPI ingestion gateway."""
    import uvicorn

    uvicorn.run("gateway.app:app", host="127.0.0.1", port=8000, log_level="info")
