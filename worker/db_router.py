import logging

from redis.asyncio import Redis

from shared.constants import DLQ_STREAM
from worker.circuit_breaker import CircuitBreaker
from worker.errors import DatabaseConnectionError, InvalidDataError
from worker.mongo_repository import MongoStore
from worker.pg_repository import create_pool, init_db, insert_events

logger = logging.getLogger(__name__)


class DatabaseRouter:
    """Routes event inserts to PostgreSQL, MongoDB, or DLQ.

    The worker calls ``insert(events)`` and never touches a database directly.
    The router decides:

    - PostgreSQL healthy        → insert into PG
    - PG connection fails       → circuit breaker opens → MongoDB (lazy connect)
    - Invalid event data        → DLQ (circuit breaker stays CLOSED)
    - CB open + Mongo down      → DLQ (last resort)
    """

    def __init__(self, settings) -> None:
        self._pg_pool = None
        self._redis: Redis | None = None
        self.cb = CircuitBreaker()
        self._mongo = MongoStore(settings.mongo_uri)
        self._pg_config = settings

    async def connect(self, redis: Redis) -> None:
        self._redis = redis

        self._pg_pool = await create_pool(
            host=self._pg_config.pg_host,
            port=self._pg_config.pg_port,
            user=self._pg_config.pg_user,
            password=self._pg_config.pg_password,
            database=self._pg_config.pg_database,
        )
        await init_db(self._pg_pool)

        logger.info("database router connected")

    async def close(self) -> None:
        if self._pg_pool is not None:
            await self._pg_pool.close()
        await self._mongo.close()
        logger.info("database router closed")

    async def insert(self, events: list[dict]) -> None:
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

        await self._fallback(events)

    async def _fallback(self, events: list[dict]) -> None:
        try:
            await self._mongo.insert(events)
            logger.warning(
                "mongo fallback | count=%s cb_state=%s",
                len(events),
                self.cb.state.value,
            )
            return
        except Exception:
            logger.exception("mongo fallback failed")

        await self._route_to_dlq(events, "fallback_exhausted")

    async def _route_to_dlq(self, events: list[dict], reason: str) -> None:
        if self._redis is None:
            logger.error("router missing redis — events may be lost")
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
