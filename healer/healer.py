import asyncio
import contextlib
import logging

from shared.errors import DatabaseConnectionError, InvalidDataError
from shared.mongo_repository import MongoStore
from shared.pg_repository import create_pool, insert_events

logger = logging.getLogger(__name__)


# fmt: off
class Healer:
    """Daemon that replays pending events from MongoDB into PostgreSQL.

    Checks PG health before each batch, bulk-inserts via COPY, and falls
    back to per-event insertion when a bad row poisons the batch.
    """

    def __init__(self, settings) -> None:
        self._batch_size = settings.healer_batch_size
        self._poll_interval = settings.healer_poll_interval
        self._pg_pool = None
        self._mongo = MongoStore(settings.mongo_uri)
        self._pg_config = settings


    async def start(self) -> None:
        self._pg_pool = await create_pool(
            host=self._pg_config.pg_host,
            port=self._pg_config.pg_port,
            user=self._pg_config.pg_user,
            password=self._pg_config.pg_password,
            database=self._pg_config.pg_database,
        )
        logger.info("healer started")


    async def close(self) -> None:
        if self._pg_pool is not None:
            await self._pg_pool.close()
        await self._mongo.close()
        logger.info("healer stopped")


    async def daemon_loop(self, shutdown_event: asyncio.Event) -> None:
        while not shutdown_event.is_set():
            if not await self._pg_healthy():
                logger.warning("pg unhealthy — skipping replay cycle")
                await self._sleep(shutdown_event)
                continue

            pending = await self._mongo.find_pending(self._batch_size)
            if not pending:
                logger.info("no pending events — sleeping")
                await self._sleep(shutdown_event)
                continue

            logger.info("replay batch | count=%s", len(pending))
            await self._replay_batch(pending)


    async def _replay_batch(self, events: list[dict]) -> None:
        event_ids = [e["event_id"] for e in events]

        try:
            await insert_events(self._pg_pool, events)
            await self._mongo.mark_replayed(event_ids)
            logger.info("replayed | count=%s", len(events))
            return
        except InvalidDataError as exc:
            logger.warning("bulk insert poisoned — isolating bad rows: %s", exc)

        good: list[dict] = []
        bad_ids: list[str] = []
        for event in events:
            try:
                await insert_events(self._pg_pool, [event])
                good.append(event)
            except InvalidDataError as exc:
                bad_ids.append(event["event_id"])
                logger.warning("poison row | event_id=%s error=%s", event["event_id"], exc)

        if good:
            await self._mongo.mark_replayed([e["event_id"] for e in good])
            logger.info("replayed partial | count=%s", len(good))
        if bad_ids:
            await self._mongo.mark_failed(bad_ids, "poison_row")
            logger.warning("failed poison rows | count=%s", len(bad_ids))


    async def _pg_healthy(self) -> bool:
        try:
            async with self._pg_pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False


    # Wait until shutdown signal or _poll_interval expires, whichever happens first.
    async def _sleep(self, shutdown_event: asyncio.Event) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown_event.wait(), timeout=self._poll_interval)
# fmt: on
