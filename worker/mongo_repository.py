import logging
from datetime import UTC, datetime

from pymongo import AsyncMongoClient

logger = logging.getLogger(__name__)

DATABASE = "arise"
COLLECTION = "events"


class MongoStore:
    """Lazy-connected MongoDB fallback store.

    Connects on first ``insert()`` call. Idempotent ``close()`` for shutdown.
    Safe in single-consumer asyncio — no locks needed.
    """

    def __init__(self, uri: str) -> None:
        self._uri = uri
        self._client: AsyncMongoClient | None = None

    async def insert(self, events: list[dict]) -> None:
        if not events:
            return
        client = await self._ensure_connected()
        if client is None:
            raise ConnectionError("MongoDB not available")

        now = datetime.now(UTC)
        records = [
            {
                "event_id": e.get("event_id"),
                "event_type": e.get("event_type"),
                "payload": e.get("payload", {}),
                "received_at": e.get("received_at"),
                "request_id": e.get("request_id"),
                "trace_id": e.get("trace_id"),
                "stored_at": now,
                "replayed": False,
            }
            for e in events
        ]

        db = client[DATABASE]
        await db[COLLECTION].insert_many(records, ordered=False)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def _ensure_connected(self) -> AsyncMongoClient | None:
        if self._client is not None:
            return self._client
        
        try:
            client = AsyncMongoClient(self._uri)
            db = client[DATABASE]
            
            existing = await db.list_collection_names()
            if COLLECTION not in existing:
                await db.create_collection(COLLECTION)
                
            self._client = client
            logger.info("mongo store connected (lazy)")
            return client
        except Exception:
            logger.exception("mongo store connection failed")
            return None
