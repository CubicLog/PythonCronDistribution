import logging
from modules import task, Priority, Overlap, Backlog, CostClass
from util.database import AnyTimeDB

import time, asyncio

log = logging.getLogger(__name__)

@task(cost=0.5)
async def example(ctx):
    #log.info("Hello example")
    if "counter" not in ctx:
        ctx["counter"] = 0
    ctx["counter"] += 1
    log.info(f"counter: {ctx["counter"]}")

@task(interval_s=30)
async def example_1():
    #log.info("Hello example_1")
    await asyncio.sleep(5)

@task(interval_s=30, cost=3)
async def example_2():
    #log.info("Hello example_2")
    await asyncio.sleep(5)

@task(priority=Priority.critical, interval_s=5, cost=CostClass.heavy)
async def critical_example():
    #log.info("Hello critical_example")
    await asyncio.sleep(3)

@task(interval_s=10, requires_mongo=True)
async def mongo_example(ctx, db: AnyTimeDB): # argument order doesn't matter
    await db.db.test_collection.insert_one({"timestamp": time.time()})
    log.info(f"inserted new document")
