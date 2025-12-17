import time, logging, asyncio
from redis.exceptions import RedisError

class RedisHealth:
    def __init__(self) -> None:
        self.ok = True
        self.down_until = 0.0

    async def check(self, r, *, cooldown_s: float = 5.0) -> bool:
        now = time.time()

        if not self.ok and now < self.down_until:
            return False

        try:
            await r.ping()
            if not self.ok:
                logging.getLogger(__name__).warning("Redis recovered")
            self.ok = True
            return True
        except RedisError:
            if self.ok:
                logging.getLogger(__name__).error("Redis unreachable — pausing ownership + scheduling")
            self.ok = False
            self.down_until = now + cooldown_s
            return False

class MongoHealth:
    def __init__(self) -> None:
        self.ok = True
        self.down_until = 0.0

    async def check(self, db, *, cooldown_s: float = 5.0, timeout_s: float = 1.0) -> bool:
        now = time.time()

        if not self.ok and now < self.down_until:
            return False

        try:
            await asyncio.wait_for(db.db.command("ping"), timeout=timeout_s)
            if not self.ok:
                logging.getLogger(__name__).warning("MongoDB recovered")
            self.ok = True
            return True
        except Exception as e:
            if self.ok:
                logging.getLogger(__name__).error("MongoDB unreachable — skipping tasks that require it")
            self.ok = False
            self.down_until = now + cooldown_s
            return False
