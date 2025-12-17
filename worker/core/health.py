from __future__ import annotations
import asyncio
import time
import logging

log = logging.getLogger(__name__)

async def monitor_event_loop_lag(*, interval_s: float = 1.0, warn_ms: int = 200):
    """
    Event loop lag (a.k.a. loop latency / scheduler lag).

    If the loop is blocked, this will spike. Great for detecting accidental
    time.sleep(), heavy CPU parsing, massive sync work, etc.
    """
    next_t = time.perf_counter() + interval_s
    while True:
        await asyncio.sleep(interval_s)
        now = time.perf_counter()
        lag_s = now - next_t
        next_t = now + interval_s

        lag_ms = max(0, int(lag_s * 1000))
        if lag_ms >= warn_ms:
            log.warning("event loop lag detected: %dms", lag_ms)
        else:
            log.debug("event loop lag: %dms", lag_ms)
