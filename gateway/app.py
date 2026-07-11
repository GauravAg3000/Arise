import logging
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import Annotated

from fastapi import FastAPI, Header, Request, status
from redis.asyncio import Redis
from uuid_extensions import uuid7

from shared.schemas import EventBatch
from gateway.schemas import IngestedBatch
from gateway.stream import publish_batch

load_dotenv()

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    redis = Redis(
        host=os.getenv("ARISE_REDIS_HOST", "localhost"),
        port=int(os.getenv("ARISE_REDIS_PORT", "6379")),
        decode_responses=True,
    )
    app.state.redis = redis

    yield

    # Shutdown code
    await redis.aclose()


app = FastAPI(title="Arise Ingestion Gateway", version="0.1.0", lifespan=lifespan)


@app.post("/api/v1/events/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_events(
    batch: EventBatch,
    request: Request,
    traceparent: Annotated[str | None, Header()] = None,
) -> IngestedBatch:
    request_id = str(uuid7())
    trace_id = traceparent.split("-")[1] if traceparent else None
    received_at = datetime.now(timezone.utc)

    logger.info(
        "received batch | batch_id=%s events=%s trace_id=%s",
        batch.batch_id,
        len(batch.events),
        trace_id,
    )

    await publish_batch(
        request.app.state.redis,
        batch,
        request_id,
        trace_id,
        received_at.isoformat(),
    )

    # Every async ingestion API returns a receipt
    # - AWS returns requestId, Kafka REST Proxy returns offsets, Stripe returns event IDs.
    # - Arise gateway returning 202 + body.
    return IngestedBatch(
        batch_id=batch.batch_id,
        request_id=request_id,
        trace_id=trace_id,
        received_at=received_at,
        event_count=len(batch.events),
    )


@app.get("/health")
async def health():
    return {"status": "ok"}
