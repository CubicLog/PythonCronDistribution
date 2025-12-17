from __future__ import annotations

from typing import Optional


_RENEW_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
"""

_RELEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
"""


class Leases:
    """
    Task ownership via a per-task lease key.

    Keys:
      - {ns}:lease:{task_id} (STRING): value=node_id, TTL=lease_ms
    """

    def __init__(self, r, *, namespace: str = "lem", lease_ms: int = 30_000) -> None:
        self.r = r
        self.ns = namespace
        self.lease_ms = int(lease_ms)

        # Register Lua scripts once per Redis connection
        self._renew_script = self.r.register_script(_RENEW_LUA)
        self._release_script = self.r.register_script(_RELEASE_LUA)

    def key(self, task_id: str) -> str:
        return f"{self.ns}:lease:{task_id}"

    async def try_acquire(self, task_id: str, node_id: str) -> bool:
        """
        SET key value NX PX lease_ms
        """
        k = self.key(task_id)
        res = await self.r.set(k, node_id, nx=True, px=self.lease_ms)
        return bool(res)

    async def renew(self, task_id: str, node_id: str, *, lease_ms: Optional[int] = None) -> bool:
        """
        Extends TTL only if still owned by node_id.
        """
        k = self.key(task_id)
        ttl = int(self.lease_ms if lease_ms is None else lease_ms)
        # returns PEXPIRE result (1) or 0
        out = await self._renew_script(keys=[k], args=[node_id, ttl])
        return int(out) == 1

    async def release(self, task_id: str, node_id: str) -> bool:
        """
        Deletes key only if owned by node_id.
        """
        k = self.key(task_id)
        out = await self._release_script(keys=[k], args=[node_id])
        return int(out) == 1

    async def current_owner(self, task_id: str) -> Optional[str]:
        return await self.r.get(self.key(task_id))
