import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import FastAPI, Header
from uuid_extensions import uuid7

from shared.schemas import EventBatch
from gateway.schemas import IngestedBatch

logger = logging.getLogger(__name__)

app = FastAPI(title="Arise Ingestion Gateway", version="0.1.0")


@app.post("/api/v1/events/batch")
async def ingest_events(
    batch: EventBatch,
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
