from redis.asyncio import Redis


def new_redis_client(host: str, port: int) -> Redis:
    return Redis(
        host=host,
        port=port,
        decode_responses=True,
    )
