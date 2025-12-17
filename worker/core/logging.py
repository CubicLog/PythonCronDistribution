from __future__ import annotations

import logging
from contextvars import ContextVar

current_task_id: ContextVar[str | None] = ContextVar("task_id", default=None)

class TaskLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.task_id = current_task_id.get() or "-"
        return True

def setup_logging(level: int = logging.INFO) -> None:
    # Create root handler if not configured yet
    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        h = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] [task=%(task_id)s]: %(message)s"
        )
        h.setFormatter(fmt)
        root.addHandler(h)

    # IMPORTANT: add filter to every handler (not just the logger)
    flt = TaskLogFilter()
    for h in root.handlers:
        h.addFilter(flt)
