from __future__ import annotations

import asyncio, time, inspect
from dataclasses import dataclass
from typing import Dict, Optional
from redis.exceptions import RedisError

from modules import Task, Priority, Overlap, Backlog
from .state import TaskState, RunResult
from .task_ctx import load_task_ctx, save_task_ctx
from util.database import AnyTimeDB

from core.logging import current_task_id
import logging

log = logging.getLogger(__name__)

REDIS_COOLDOWN_S = 5.0

def _prio_value(p: Priority) -> int:
    # higher priority should run first
    return int(p.value)

@dataclass
class TaskRuntime:
    task: Task
    task_id: str
    next_due: float
    running_task: Optional[asyncio.Task] = None
    wants_ctx: bool = False
    wants_mongo: bool = False

class TaskRunner:
    def __init__(
        self,
        *,
        tasks_by_id: Dict[str, Task],
        node_id: str,
        state: TaskState,
        namespace: str = "lem",
        global_concurrency: int = 8,
        default_timeout_s: int = 60,
        tick_s: float = 0.5,
        redis,
        health,
        db,
        db_health,
    ) -> None:
        self.tasks_by_id = tasks_by_id
        self.node_id = node_id
        self.state = state
        self.ns = namespace

        self.sem = asyncio.Semaphore(global_concurrency)
        self.default_timeout_s = default_timeout_s
        self.tick_s = tick_s

        self._rt: Dict[str, TaskRuntime] = {}
        self._stop = asyncio.Event()

        self.redis = redis
        self.health = health
        
        self.db: AnyTimeDB = db
        self.db_health = db_health

    def stop(self) -> None:
        self._stop.set()

    def ensure_task(self, task_id: str) -> None:
        """
        Ensure runtime entry exists for a task id (if present locally).
        """
        t = self.tasks_by_id.get(task_id)
        if not t:
            return
        if task_id not in self._rt:
            self._rt[task_id] = TaskRuntime(
                task=t,
                task_id=task_id,
                next_due=time.time() + float(t.interval_s),
                wants_ctx=self._wants_ctx(t.coro_factory),
                wants_mongo=t.requires_mongo,
            )

    def drop_task(self, task_id: str) -> None:
        """
        Remove runtime entry; does not cancel running unless overlap=replace triggers.
        """
        self._rt.pop(task_id, None)
    
    def _wants_ctx(self, factory) -> bool:
        """
        Return True if the task factory expects a ctx argument.

        True if:
        - it has a parameter named 'ctx' (positional or keyword)
        - OR it accepts **kwargs

        False if:
        - it has no parameters
        - OR it has parameters, but none named 'ctx'
        """
        sig = inspect.signature(factory)

        for p in sig.parameters.values():
            # Explicit ctx parameter
            if p.name == "ctx":
                return True

            # Accepts **kwargs → safe to pass ctx=
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                return True

        return False

    async def _run_once(self, rt: TaskRuntime) -> bool:
        t = rt.task
        task_id = rt.task_id

        if t.requires_mongo and not self.db_health.ok:
            # Mongo down → defer task a bit so we don't busy-loop
            rt.next_due = max(rt.next_due, time.time() + 2.0)
            return False # we handled scheduling ourselves

        # overlap handling
        if rt.running_task and not rt.running_task.done():
            if t.overlap == Overlap.forbid:
                return False
            if t.overlap == Overlap.replace:
                rt.running_task.cancel()
                # allow it to be replaced
            # Overlap.allow: just proceed (concurrency guard still applies)

        due_at = rt.next_due

        async def _exec():
            token = current_task_id.set(task_id)
            started = time.time()
            lag_ms = int(max(0.0, (started - due_at)) * 1000)
            
            if lag_ms > max(2000, int(rt.task.interval_s * 1000 * 0.50)):
                log.error(f"task {task_id} severely delayed: {lag_ms}ms")
            elif lag_ms > max(500, t.interval_s * 1000 * 0.10):
                log.warning(f"task {task_id} started late: {lag_ms}ms")

            try:
                kwargs = {}
                try:
                    await self.state.mark_owner(task_id, self.node_id)

                    # call task with or without ctx depending on signature
                    if rt.wants_ctx:
                        ctx = await load_task_ctx(self.redis, namespace=self.ns, task_id=task_id)
                        kwargs["ctx"] = ctx
                except Exception as e:
                    # redis/control-plane failure
                    log.warning(f"Skipping task {task_id}: Redis unavailable ({e!r})")
                    return
                
                if rt.wants_mongo:
                    # implement check db health
                    kwargs["db"] = self.db
                
                coro = t.coro_factory(**kwargs)

                # Run under global concurrency + timeout
                async with self.sem:
                    await asyncio.wait_for(coro, timeout=self.default_timeout_s)

                finished = time.time()

                # persist mutated ctx
                if rt.wants_ctx:
                    try:
                        await save_task_ctx(self.redis, namespace=self.ns, task_id=task_id, ctx=ctx)
                    except Exception as e:
                        log.warning(f"Failed to persist ctx for {task_id}: {e!r}")

                await self.state.record_run(
                    task_id,
                    self.node_id,
                    RunResult(ok=True, started_at=started, finished_at=finished, lag_ms=lag_ms),
                )
            except asyncio.CancelledError:
                # If we cancel due to replace, treat as neutral (don’t record error)
                raise
            except Exception as e:
                finished = time.time()
                await self.state.record_run(
                    task_id,
                    self.node_id,
                    RunResult(ok=False, started_at=started, finished_at=finished, error=repr(e), lag_ms=lag_ms),
                )
                log.error(f"Task {task_id} failed: {e!r}")
            finally:
                current_task_id.reset(token)

        rt.running_task = asyncio.create_task(_exec())
        return True

    def _compute_next_due(self, rt: TaskRuntime, now: float) -> float:
        """
        Backlog policy decides how to schedule next run.
        """
        interval = float(max(1, rt.task.interval_s))

        if rt.task.backlog == Backlog.catch_up:
            # schedule strictly by fixed interval timeline
            return rt.next_due + interval

        if rt.task.backlog == Backlog.coalesce:
            # run once "now", then schedule next from now
            return now + interval

        # Backlog.skip (default): jump to the next future tick aligned from previous due
        # Ensure next_due is in the future even if we fell behind
        nd = rt.next_due + interval
        if nd <= now:
            # jump forward by enough intervals
            missed = int((now - rt.next_due) // interval) + 1
            nd = rt.next_due + missed * interval
        return nd

    async def run_loop(self, owned_ids_provider) -> None:
        """
        owned_ids_provider: callable returning a set[str] (e.g. owner.owned)
        """
        while not self._stop.is_set():
            # If Redis is unhealthy, don't schedule new tasks
            if not await self.health.check(self.redis):
                await asyncio.sleep(self.tick_s)
                continue

            now = time.time()

            owned = set(owned_ids_provider())

            # ensure runtimes exist for owned tasks
            for tid in owned:
                self.ensure_task(tid)

            # drop runtimes for tasks no longer owned (don’t cancel in-flight)
            for tid in list(self._rt.keys()):
                if tid not in owned:
                    self.drop_task(tid)

            # pick due tasks, run in priority order
            due = [rt for rt in self._rt.values() if rt.next_due <= now]

            # sort: higher priority first, then earliest due
            due.sort(key=lambda rt: (-_prio_value(rt.task.priority), rt.next_due))

            for rt in due:
                # run
                did_run = await self._run_once(rt)
                # schedule next due based on backlog policy
                if did_run:
                    rt.next_due = self._compute_next_due(rt, now)

            await asyncio.sleep(self.tick_s)
