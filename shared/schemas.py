from pydantic import BaseModel, Field
from datetime import datetime, timezone
from uuid_extensions import uuid7


class EventMetadata(BaseModel):
    source: str = "cli"
    version: str = "1.0"
    trace_id: str | None = None


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid7()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    type: str
    payload: dict = Field(default_factory=dict)
    metadata: EventMetadata = Field(default_factory=EventMetadata)


class EventBatch(BaseModel):
    batch_id: str = Field(default_factory=lambda: str(uuid7()))
    events: list[Event]
    sent_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
