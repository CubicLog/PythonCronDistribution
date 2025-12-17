import redis.asyncio as redis
from redis.exceptions import RedisError
import os

redis_conn: redis.Redis | None = None

async def init_redis():
    global redis_conn
    redis_conn = redis.from_url(
        os.getenv("REDIS_URL", "redis://redis:6379/0"),
        encoding="utf-8",
        decode_responses=True,
    )

async def close_redis():
    global redis_conn
    if redis_conn:
        await redis_conn.close()
        redis_conn = None

def get_redis():
    if redis_conn is None:
        raise RuntimeError("Redis not initialized")
    return redis_conn
