import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timezone
from typing import Annotated

from dotenv import load_dotenv
from fastapi import FastAPI, Header, Request, status
from redis.asyncio import Redis
from uuid_extensions import uuid7

from gateway.schemas import IngestedBatch
from gateway.stream import publish_batch
from shared.schemas import EventBatch
from shared.settings import GatewaySettings

load_dotenv()

settings = GatewaySettings()

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        decode_responses=True,
    )
    app.state.redis = redis

    yield

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
    received_at = datetime.now(UTC)

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
