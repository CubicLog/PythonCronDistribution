from __future__ import annotations

from dataclasses import asdict
from typing import Iterable
import time
import hashlib

from modules import Task


def _task_id(t: Task) -> str:
    # Stable identifier across nodes
    return f"{t.coro_factory.__module__}:{t.name}"


def _task_fingerprint(t: Task) -> str:
    """
    A lightweight 'version' string so you can detect metadata changes.
    (Not required, but useful for debugging and future migrations.)
    """
    payload = "|".join([
        _task_id(t),
        str(t.interval_s),
        str(getattr(t.priority, "name", t.priority)),
        str(t.cost),
        str(getattr(t.overlap, "name", t.overlap)),
        str(getattr(t.backlog, "name", t.backlog)),
    ]).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


async def register_tasks(
    r,
    tasks: Iterable[Task],
    *,
    namespace: str = "lem",
    prune: bool = False,
    node_id: str | None = None,
) -> list[str]:
    """
    Upserts task metadata into Redis.

    - namespace: prefix for keys
    - prune: if True, removes tasks from Redis that are not in this codebase
             (only enable once you're confident you won't run mixed versions)
    """
    tasks = list(tasks)
    task_ids = [_task_id(t) for t in tasks]

    key_tasks = f"{namespace}:tasks"

    now = int(time.time())

    pipe = r.pipeline(transaction=False)

    # 1) Add to global set
    if task_ids:
        pipe.sadd(key_tasks, *task_ids)

    # 2) Upsert per-task metadata
    for t in tasks:
        tid = _task_id(t)
        key = f"{namespace}:task:{tid}"

        meta = {
            "name": t.name,
            "module": t.coro_factory.__module__,
            "interval_s": int(t.interval_s),
            "priority": getattr(t.priority, "name", str(t.priority)),
            "cost": float(t.cost),
            "overlap": getattr(t.overlap, "name", str(t.overlap)),
            "backlog": getattr(t.backlog, "name", str(t.backlog)),
            "fingerprint": _task_fingerprint(t),
            "updated_at": now,
        }
        pipe.hset(key, mapping=meta)

    # Optional: mark registry heartbeat
    if node_id:
        pipe.set(f"{namespace}:registry_epoch:{node_id}", now, ex=3600)

    # 3) Optional prune (danger in mixed deploys)
    if prune:
        # Fetch currently known tasks then remove ones not present in this code
        pipe.smembers(key_tasks)

    res = await pipe.execute()

    if prune:
        existing = res[-1] or set()
        stale = set(existing) - set(task_ids)
        if stale:
            pipe = r.pipeline(transaction=False)
            pipe.srem(key_tasks, *stale)
            for tid in stale:
                pipe.delete(f"{namespace}:task:{tid}")
            await pipe.execute()

    return task_ids
