import asyncio
import logging

import typer

from agent.config import ProduceConfig
from agent.runner import run_producer

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
        "30s",
        "--duration",
        "-d",
        help="Duration (e.g., 30s, 5m, 1h)",
    ),
    batch_size: int = typer.Option(
        100,
        "--batch-size",
        "-b",
        help="Max events per batch",
    ),
    batch_timeout: str = typer.Option(
        "1s",
        "--batch-timeout",
        "-t",
        help="Max batch age (e.g., 500ms, 2s)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-n",
        help="Print batches without sending",
    ),
):
    config = ProduceConfig(
        rate=rate,
        duration=duration,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        dry_run=dry_run,
    )
    asyncio.run(run_producer(config))


@app.command()
def gateway():
    """Start the FastAPI ingestion gateway."""
    import uvicorn

    uvicorn.run("gateway.app:app", host="127.0.0.1", port=8000, log_level="info")


@app.command()
def worker(
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Number of worker processes",
    ),
):
    """Start one or more Redis stream consumer workers."""
    if workers > 1:
        from worker.pool import WorkerPool

        WorkerPool(size=workers).run()
    else:
        import asyncio

        from worker.app import run_worker

        asyncio.run(run_worker())


if __name__ == "__main__":
    app()
