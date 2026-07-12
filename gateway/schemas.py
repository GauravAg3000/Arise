from datetime import datetime

from pydantic import BaseModel


class IngestedBatch(BaseModel):
    batch_id: str
    request_id: str
    trace_id: str | None
    received_at: datetime
    event_count: int
