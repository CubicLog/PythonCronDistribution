from __future__ import annotations
from functools import wraps
from enum import Enum
from dataclasses import dataclass
from typing import Awaitable, Callable, Any, List, Optional

"""
Default Combo For Most Tasks:
Priority.medium
Overlap.forbid
Backlog.coalesce

High-frequency "latest data" tasks:
Priority.high
Overlap.replace
Backlog.coalesce

Strict timeline tasks:
Priority.high
Overlap.forbid
Backlog.catch_up

For regular cadence tasks
(i.e 5 sec flight data snapshots that can take longer than a few seconds to complete):
Priority.high
Overlap.forbid
Backlog.skip
"""

class Priority(Enum):
    """
    Determines the execution priority of a task *within a single node*.

    Priority ONLY affects local scheduling order when multiple tasks are due
    at the same time. It does NOT affect task ownership or distribution
    across nodes (that is handled separately via weights and rendezvous hashing).

    Higher values run first.
    """
    low = 0        # Best-effort / background tasks. Can be delayed without issue.
    medium = 1     # Default priority for most periodic collectors.
    high = 2       # Important tasks that should run before medium/low ones.
    critical = 3   # Time-sensitive tasks that should preempt all others.


class Overlap(Enum):
    """
    Determines how the scheduler behaves when a task is due to run
    but a previous run of the same task is still executing. In most cases, we want .forbid
    """
    forbid = 0     # Do NOT start a new run if the previous run is still active.
                   # The scheduled run is skipped until the task becomes free.

    allow = 1      # Allow overlapping executions of the task.
                   # Use ONLY if the task is re-entrant and concurrency-safe.

    replace = 2    # Cancel the currently running task and start a new run.
                   # Useful for "latest state" tasks where only the newest run matters.


class Backlog(Enum):
    """
    Determines how the scheduler handles missed executions when a task
    falls behind schedule (e.g. due to overload or long runtimes).
    """
    skip = 0       # Skip missed runs and jump forward to the next future interval.
                   # Prevents backlog buildup. Best for most periodic collectors.

    catch_up = 1   # Execute every missed interval until fully caught up.
                   # Use sparingly — can cause bursts if the task lags.

    coalesce = 2   # Collapse all missed runs into a single execution.
                   # Run once "now", then schedule the next run normally.
                   # Ideal for "latest snapshot" or idempotent tasks.

class CostClass(float, Enum):
    """
    Relative execution cost of a task.

    Cost is an approximate measure of how expensive a single execution is,
    used ONLY for ownership/load balancing across nodes.
    """
    trivial = 0.25     # tiny, fast, minimal I/O
    light = 0.5        # small API call or quick DB write
    standard = 1.0     # typical periodic collector
    heavy = 3.0        # large API response + parsing + bulk DB writes
    very_heavy = 6.0   # global datasets, large fan-outs
    extreme = 10.0     # rare, very expensive jobs


_REGISTRY: List["Task"] = []

@dataclass
class Task:
    name: str
    interval_s: int
    coro_factory: Callable[[Any], Awaitable[None]]
    priority: Priority = Priority.medium
    cost: float | int = 2.0
    overlap: Overlap = Overlap.forbid
    backlog: Backlog = Backlog.skip
    requires_mongo: bool = False

def task(
    name: str | None = None, *,
    interval_s: int = 5,
    priority: Priority = Priority.medium,
    cost: CostClass | float = CostClass.standard,
    overlap: Overlap = Overlap.forbid,
    backlog: Backlog = Backlog.skip,
    requires_mongo: bool = False,
):
    def wrapper(factory: Callable[[Any], Awaitable[None]]):
       _REGISTRY.append(Task(
           name=name or factory.__name__,
           interval_s=interval_s,
           coro_factory=factory,
           priority=priority,
           cost=float(cost),
           overlap=overlap,
           backlog=backlog,
           requires_mongo=requires_mongo,
       ))
       return factory
    return wrapper

def get_registry() -> List[Task]: 
    return list(_REGISTRY)
