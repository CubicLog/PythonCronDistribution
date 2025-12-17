import logging
from modules import task, Priority, Overlap, Backlog

log = logging.getLogger(__name__)

@task()
async def another_example():
    #log.info("Hello another_example")
    pass
