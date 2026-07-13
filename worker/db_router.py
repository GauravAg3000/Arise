import logging

from redis.asyncio import Redis

from shared.constants import DLQ_STREAM
from shared.errors import DatabaseConnectionError, InvalidDataError
from shared.mongo_repository import MongoStore
from shared.pg_repository import create_pool, init_db, insert_events
from worker.circuit_breaker import CircuitBreaker

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
        self._circuit_breaker = CircuitBreaker()
        self._mongo = MongoStore(settings.mongo_uri)
        self._pg_config = settings

    async def connect(self, redis: Redis) -> None:
        self._redis = redis

        try:
            self._pg_pool = await create_pool(
                host=self._pg_config.pg_host,
                port=self._pg_config.pg_port,
                user=self._pg_config.pg_user,
                password=self._pg_config.pg_password,
                database=self._pg_config.pg_database,
            )
            await init_db(self._pg_pool)
            logger.info("database router connected")
        except Exception:
            logger.warning("postgres unavailable at startup — using fallback")
            self._circuit_breaker.force_open()

    async def close(self) -> None:
        if self._pg_pool is not None:
            await self._pg_pool.close()
        await self._mongo.close()
        logger.info("database router closed")

    async def insert(self, events: list[dict]) -> None:
        if await self._try_postgres(events):
            return

        if await self._try_mongo(events):
            return

        await self._route_to_dlq(events, "fallback_exhausted")

    async def _try_postgres(self, events: list[dict]) -> bool:
        if self._pg_pool is None:
            return False

        if not self._circuit_breaker.can_try_primary():
            return False

        try:
            await insert_events(self._pg_pool, events)
            self._circuit_breaker.record_success()
            logger.info("pg insert | count=%s", len(events))
            return True
        except DatabaseConnectionError:
            self._circuit_breaker.record_failure()
            return False
        except InvalidDataError:
            await self._route_to_dlq(events, "invalid_data")
            return True

    async def _try_mongo(self, events: list[dict]) -> bool:
        try:
            await self._mongo.insert(events)
            logger.warning("mongo fallback | count=%s", len(events))
            return True
        except Exception:
            logger.exception("mongo fallback failed")
            return False

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
