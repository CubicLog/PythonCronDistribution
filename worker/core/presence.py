from __future__ import annotations

import time
from typing import Iterable, List


class Presence:
    """
    Node membership via heartbeat stored in a ZSET.

    Keys:
      - {ns}:nodes (ZSET): member=node_id, score=unix_ts
    """

    def __init__(
        self,
        r,
        *,
        namespace: str = "lem",
        heartbeat_s: int = 5,
        stale_s: int = 15,
    ) -> None:
        self.r = r
        self.ns = namespace
        self.heartbeat_s = heartbeat_s
        self.stale_s = stale_s
        self.key_nodes = f"{self.ns}:nodes"

    def now(self) -> int:
        return int(time.time())
    
    def _meta_key(self, node_id: str) -> str:
        return f"{self.ns}:node_meta:{node_id}"

    async def heartbeat(self, node_id: str) -> None:
        now = self.now()
        pipe = self.r.pipeline(transaction=False)
        pipe.zadd(self.key_nodes, {node_id: now})
        # Only set first_seen if it doesn't exist yet
        pipe.hsetnx(self._meta_key(node_id), "first_seen", now)
        await pipe.execute()

    async def prune_stale(self) -> int:
        """
        Removes nodes that haven't heartbeated in stale_s seconds.
        Also deletes their meta hash so a returning node must "warm up" again.
        Returns number removed.
        """
        cutoff = self.now() - self.stale_s

        # Find stale nodes so we can delete their meta
        stale_nodes = await self.r.zrangebyscore(self.key_nodes, "-inf", cutoff)
        if stale_nodes:
            pipe = self.r.pipeline(transaction=False)
            pipe.zremrangebyscore(self.key_nodes, "-inf", cutoff)
            for nid in stale_nodes:
                pipe.delete(self._meta_key(nid))
            await pipe.execute()
            return len(stale_nodes)

        # Nothing stale
        return 0

    async def live_nodes(self) -> List[str]:
        """
        Returns nodes seen within stale_s seconds.
        """
        cutoff = self.now() - self.stale_s
        # ZRANGEBYSCORE key min max
        return await self.r.zrangebyscore(self.key_nodes, cutoff, "+inf")
    
    async def eligible_nodes(self, *, min_uptime_s: int = 120) -> List[str]:
        """
        Returns nodes that are live AND have been continuously present for at least min_uptime_s.
        This prevents flapping/rejoining nodes from immediately taking tasks.
        """
        min_uptime_s = max(0, int(min_uptime_s))
        live = await self.live_nodes()
        if not live or min_uptime_s == 0:
            return live

        now = self.now()

        pipe = self.r.pipeline(transaction=False)
        for nid in live:
            pipe.hget(self._meta_key(nid), "first_seen")
        first_seen_vals = await pipe.execute()

        eligible: List[str] = []
        for nid, fs in zip(live, first_seen_vals):
            try:
                if fs is None:
                    # If missing (shouldn't happen often), treat as not eligible yet
                    continue
                if (now - int(fs)) >= min_uptime_s:
                    eligible.append(nid)
            except (TypeError, ValueError):
                continue

        return eligible
