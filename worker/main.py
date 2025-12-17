from __future__ import annotations
import sys, os, asyncio, time
from pathlib import Path
import importlib.util
import logging
from redis.exceptions import RedisError
from modules import get_registry
from core.registry import register_tasks
from core.presence import Presence
from core.leases import Leases
from core.ownership import OwnershipManager
from core.runner import TaskRunner
from core.state import TaskState
from core.health import monitor_event_loop_lag
from core.logging import setup_logging
from core.redis import init_redis, close_redis, get_redis
from core.control import RedisHealth, MongoHealth
from util.database import AnyTimeDB

setup_logging()

def import_modules_from_dir(root: Path, prefix: str = "modules") -> None:
    root = root.resolve()

    for py in root.rglob("*.py"):
        if py.name == "__init__.py":
            continue

        rel = py.relative_to(root).with_suffix("")  # e.g. nested/foo
        modname = prefix + "." + ".".join(rel.parts)  # modules.nested.foo

        spec = importlib.util.spec_from_file_location(modname, py)
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[modname] = module  # important so imports/reference work
        spec.loader.exec_module(module)

async def ownership_loop(owner, presence, node_id, r, health: RedisHealth):
    while True:
        if not await health.check(r):
            # Important: drop ownership locally to avoid split brain assumptions
            owner.owned.clear()
            await asyncio.sleep(1.0)
            continue

        try:
            await presence.heartbeat(node_id)
            await presence.prune_stale()
            res = await owner.reconcile()
        except Exception as e:
            # Most likely RedisError, but keep it broad here to prevent loop death
            logging.getLogger(__name__).warning("ownership loop error: %r", e)

        await asyncio.sleep(5)

async def wait_for_redis(r, *, health: RedisHealth):
    while not await health.check(r):
        await asyncio.sleep(1.0)

async def monitor_mongo_health(health: MongoHealth, db: AnyTimeDB, interval_s: float = 2.0):
    while True:
        await health.check(db)
        await asyncio.sleep(interval_s)

async def main():
    try:
        await init_redis()
        r = get_redis()
        health = RedisHealth()

        mongodb_uri = os.getenv("MONGO_URI")
        if not mongodb_uri:
            raise RuntimeError("MONGO_URI not set")
        db = AnyTimeDB(mongodb_uri, "OSINT")
        db_health = MongoHealth()

        import_modules_from_dir(Path(__file__).parent / "modules")
        
        tasks_meta = get_registry()
        if not tasks_meta:
            raise RuntimeError("No scheduled tasks registered in modules/")
        
        node_id = os.getenv("NODE_ID") or os.getenv("HOSTNAME") or "unknown"

        # waitt for Redis to be available before registering tasks
        await wait_for_redis(r, health=health)
        
        await register_tasks(
            r,
            tasks_meta,
            namespace="lem",
            prune=False,
            node_id=node_id
        )

        presence = Presence(r, namespace="lem", heartbeat_s=5, stale_s=15)
        leases = Leases(r, namespace="lem", lease_ms=30_000)
        owner = OwnershipManager(
            r,
            namespace="lem",
            presence=presence,
            leases=leases,
            node_id=node_id,
            top_k=2,
            min_uptime_s=120
        )

        # map task_id -> Task locally
        tasks_by_id = {f"{t.coro_factory.__module__}:{t.name}": t for t in tasks_meta}

        state = TaskState(r, namespace="lem")
        runner = TaskRunner(
            tasks_by_id=tasks_by_id,
            node_id=node_id,
            state=state,
            global_concurrency=8,
            redis=r,
            health=health,
            db=db,
            db_health=db_health,
        )

        await asyncio.gather(
            ownership_loop(owner, presence, node_id, r, health),
            runner.run_loop(lambda: owner.owned),
            monitor_event_loop_lag(interval_s=5.0, warn_ms=200),
            monitor_mongo_health(db_health, db, interval_s=5.0),
        )
    finally:
        await close_redis()
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())