import logging
import time

from shared.schemas import Event, EventBatch

logger = logging.getLogger(__name__)


class BatchBuffer:
    def __init__(self, max_size: int, max_age_s: float):
        self.max_size = max_size
        self.max_age_s = max_age_s
        self.batch: list[Event] = []
        self.flushed_at = time.monotonic()

    def add(self, event: Event) -> EventBatch | None:
        self.batch.append(event)
        if len(self.batch) >= self.max_size:
            return self._flush()
        return None

    def flush_if_expired(self) -> EventBatch | None:
        if not self.batch:
            return None
        if time.monotonic() - self.flushed_at >= self.max_age_s:
            return self._flush()
        return None

    def flush(self) -> EventBatch | None:
        if not self.batch:
            return None
        return self._flush()

    def _flush(self) -> EventBatch:
        batch = EventBatch(events=self.batch)
        self.batch = []
        self.flushed_at = time.monotonic()
        return batch
