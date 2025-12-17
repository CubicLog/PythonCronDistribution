from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RunResult:
    ok: bool
    started_at: float
    finished_at: float
    lag_ms: int
    error: Optional[str] = None

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at) * 1000)


class TaskState:
    """
    Stores task run state + basic metrics in Redis.

    Keys:
      - {ns}:task_state:{task_id} (HASH)
        fields: last_start, last_finish, last_ok, last_error, last_error_at,
                last_duration_ms, ok_count, err_count, updated_at, owner
    """

    def __init__(self, r, *, namespace: str = "lem") -> None:
        self.r = r
        self.ns = namespace

    def key(self, task_id: str) -> str:
        return f"{self.ns}:task_state:{task_id}"

    async def mark_owner(self, task_id: str, node_id: str) -> None:
        now = int(time.time())
        await self.r.hset(self.key(task_id), mapping={
            "owner": node_id,
            "updated_at": now,
        })

    async def record_run(self, task_id: str, node_id: str, result: RunResult) -> None:
        now = int(time.time())
        k = self.key(task_id)

        mapping = {
            "owner": node_id,
            "last_start": int(result.started_at),
            "last_finish": int(result.finished_at),
            "last_duration_ms": result.duration_ms,
            "lag_ms": result.lag_ms,
            "updated_at": now,
        }

        if result.ok:
            mapping["last_ok"] = int(result.finished_at)
            # increment ok_count
            pipe = self.r.pipeline(transaction=False)
            pipe.hset(k, mapping=mapping)
            pipe.hincrby(k, "ok_count", 1)
            await pipe.execute()
        else:
            mapping["last_error"] = result.error or "unknown_error"
            mapping["last_error_at"] = int(result.finished_at)
            pipe = self.r.pipeline(transaction=False)
            pipe.hset(k, mapping=mapping)
            pipe.hincrby(k, "err_count", 1)
            await pipe.execute()
