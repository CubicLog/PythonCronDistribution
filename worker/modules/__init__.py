from __future__ import annotations
from functools import wraps
from enum import Enum
from dataclasses import dataclass
from typing import Awaitable, Callable, Any, List, Optional

class Priority(Enum):
    low = 0
    medium = 1
    high = 2
    critical = 3

class Overlap(Enum):
    forbid = 0
    allow = 1
    replace = 2

class Backlog(Enum):
    skip = 0
    catch_up = 1
    coalesce = 2

_REGISTRY: List["Task"] = []

@dataclass
class Task:
    name: str
    interval_s: int
    coro_factory: Callable[[Any], Awaitable[None]]
    priority: Priority = Priority.medium,
    cost: float | int = 2.0,
    overlap: Overlap = Overlap.forbid,
    backlog: Backlog = Backlog.skip,

def task(
    name: str | None = None, *,
    interval_s: int = 5,
    priority: Priority = Priority.medium,
    cost: float | int = 2.0,
    overlap: Overlap = Overlap.forbid,
    backlog: Backlog = Backlog.skip,
):
    def wrapper(factory: Callable[[Any], Awaitable[None]]):
       _REGISTRY.append(Task(
           name=name or factory.__name__,
           interval_s=interval_s,
           coro_factory=factory,
           priority=priority,
           cost=cost,
           overlap=overlap,
           backlog=backlog
       ))
       return factory
    return wrapper

def get_registry() -> List[Task]:
    return list(_REGISTRY)
