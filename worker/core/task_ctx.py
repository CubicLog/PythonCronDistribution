from __future__ import annotations
import json
from typing import Any

DEFAULT_CTX: dict[str, Any] = {}

async def load_task_ctx(r, *, namespace: str, task_id: str) -> dict[str, Any]:
    key = f"{namespace}:taskctx:{task_id}"
    raw = await r.get(key)
    if not raw:
        return dict(DEFAULT_CTX)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # safest fallback
        return dict(DEFAULT_CTX)

async def save_task_ctx(r, *, namespace: str, task_id: str, ctx: dict[str, Any]) -> None:
    key = f"{namespace}:taskctx:{task_id}"
    await r.set(key, json.dumps(ctx, separators=(",", ":"), ensure_ascii=False))
