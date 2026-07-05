from pydantic import BaseModel
from datetime import datetime


class IngestedBatch(BaseModel):
    batch_id: str
    request_id: str
    trace_id: str | None
    received_at: datetime
    event_count: int
