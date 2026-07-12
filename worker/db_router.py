import logging

from redis.asyncio import Redis

from shared.constants import DLQ_STREAM
from worker.circuit_breaker import CircuitBreaker, CircuitState
from worker.errors import DatabaseConnectionError, InvalidDataError
from worker.pg_repository import create_pool, init_db, insert_events

logger = logging.getLogger(__name__)


class DatabaseRouter:
    """Routes event inserts to PostgreSQL or DLQ based on circuit breaker state.

    The worker calls ``insert(events)`` and never touches a database directly.
    The router decides:

    - PostgreSQL healthy  → insert into PG
    - PG connection fails  → circuit breaker opens → DLQ
    - Invalid event data   → DLQ (circuit breaker stays CLOSED)
    """

    def __init__(self, settings) -> None:
        self._settings = settings
        self._pg_pool = None
        self._redis: Redis | None = None
        self.cb = CircuitBreaker()

    async def connect(self, redis: Redis) -> None:
        """Connect to PostgreSQL and Redis. Creates schema if needed."""
        self._redis = redis
        self._pg_pool = await create_pool(
            host=self._settings.pg_host,
            port=self._settings.pg_port,
            user=self._settings.pg_user,
            password=self._settings.pg_password,
            database=self._settings.pg_database,
        )
        await init_db(self._pg_pool)
        logger.info("database router connected | pg_pool=%s", id(self._pg_pool))

    async def close(self) -> None:
        """Release PostgreSQL pool."""
        if self._pg_pool is not None:
            await self._pg_pool.close()
            logger.info("database router closed")

    async def insert(self, events: list[dict]) -> None:
        """Insert events into PostgreSQL or route to DLQ on failure.

        Three outcomes:
        1. PG succeeds             → XACK (caller's responsibility)
        2. DatabaseConnectionError  → CB opens → DLQ
        3. InvalidDataError        → DLQ immediately, CB stays CLOSED
        """
        if self.cb.can_try_primary():
            try:
                await insert_events(self._pg_pool, events)
                self.cb.record_success()
                return
            except DatabaseConnectionError:
                self.cb.record_failure()
            except InvalidDataError:
                await self._route_to_dlq(events, "invalid_data")
                return

        await self._route_to_dlq(events, self._fallback_reason())

    def _fallback_reason(self) -> str:
        if self.cb.state is CircuitState.OPEN:
            return "circuit_open"
        return "primary_unavailable"

    async def _route_to_dlq(self, events: list[dict], reason: str) -> None:
        """Write failed events to the DLQ Redis stream."""
        if self._redis is None:
            logger.error("router missing redis — cannot write to DLQ, events may be lost")
            return

        try:
            for event in events:
                await self._redis.xadd(
                    DLQ_STREAM,
                    {
                        "event_id": str(event.get("event_id", "")),
                        "event_type": str(event.get("event_type", "")),
                        "payload": str(event.get("payload", "{}")),
                        "received_at": str(event.get("received_at", "")),
                        "request_id": str(event.get("request_id", "")),
                        "trace_id": str(event.get("trace_id", "")),
                        "dlq_reason": reason,
                    },
                    maxlen=10000,
                )
            logger.warning(
                "dlq routed | count=%s reason=%s",
                len(events),
                reason,
            )
        except Exception:
            logger.exception("dlq write failed — events may be lost")
