import logging

import asyncpg

logger = logging.getLogger(__name__)


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(min_size=1, max_size=2)


async def init_db(pool: asyncpg.Pool) -> None:
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
    logger.info("table 'events' created")


async def insert_events(pool: asyncpg.Pool, events: list[dict]) -> None:
    async with pool.acquire() as conn:
        # leveraging PostgreSQL's COPY protocol
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
