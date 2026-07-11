import os

from redis.asyncio import Redis


def new_redis_client() -> Redis:
    return Redis(
        host=os.getenv("ARISE_REDIS_HOST", "localhost"),
        port=int(os.getenv("ARISE_REDIS_PORT", "6379")),
        decode_responses=True,
    )
