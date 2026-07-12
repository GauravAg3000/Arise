import logging

import asyncpg

from worker.errors import DatabaseConnectionError, InvalidDataError

logger = logging.getLogger(__name__)


async def create_pool(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> asyncpg.Pool:
    try:
        return await asyncpg.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            min_size=1,
            max_size=2,
        )
    except (asyncpg.PostgresConnectionError, OSError) as exc:
        raise DatabaseConnectionError(f"PostgreSQL unreachable: {exc}") from exc


async def init_db(pool: asyncpg.Pool) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id BIGSERIAL PRIMARY KEY,
                    event_id UUID NOT NULL UNIQUE,
                    event_type VARCHAR(64) NOT NULL,
                    payload JSONB,
                    received_at TIMESTAMPTZ NOT NULL,
                    request_id UUID,
                    trace_id VARCHAR(64),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
    except (asyncpg.PostgresConnectionError, OSError) as exc:
        raise DatabaseConnectionError(f"PostgreSQL unreachable: {exc}") from exc
    logger.info("table 'events' created")


async def insert_events(pool: asyncpg.Pool, events: list[dict]) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                "events",
                records=[
                    (
                        e["event_id"],
                        e["event_type"],
                        e["payload"],
                        e["received_at"],
                        e["request_id"],
                        e["trace_id"],
                    )
                    for e in events
                ],
                columns=[
                    "event_id",
                    "event_type",
                    "payload",
                    "received_at",
                    "request_id",
                    "trace_id",
                ],
            )
    except (asyncpg.PostgresConnectionError, OSError) as exc:
        raise DatabaseConnectionError(f"PostgreSQL unreachable: {exc}") from exc
    except asyncpg.PostgresError as exc:
        raise InvalidDataError(f"PostgreSQL constraint violation: {exc}") from exc
