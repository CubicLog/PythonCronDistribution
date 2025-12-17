from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple


from .presence import Presence
from .leases import Leases


def hrw_score(task_id: str, node_id: str) -> int:
    """
    Rendezvous (HRW) score: higher wins.
    Stable 64-bit hash.
    """
    h = hashlib.blake2b(
        f"{task_id}|{node_id}".encode("utf-8"),
        digest_size=8,
    ).digest()
    return int.from_bytes(h, "big", signed=False)


def hrw_topk(task_id: str, nodes: List[str], k: int) -> List[str]:
    """
    Return top-k nodes by HRW score (descending). Deterministic.
    """
    scored = [(hrw_score(task_id, n), n) for n in nodes]
    scored.sort(reverse=True, key=lambda x: x[0])
    return [n for _, n in scored[: max(1, k)]]


def compute_weight(cost: float, interval_s: int) -> float:
    """
    Weight ~ expected work per second.
    Guardrails prevent div-by-zero and keep tiny tasks non-zero.
    """
    interval = max(1, int(interval_s))
    c = float(cost)
    w = c / interval
    return max(0.001, w)


@dataclass
class OwnershipResult:
    owned: Set[str]
    acquired: Set[str]
    renewed: Set[str]
    released: Set[str]
    skipped: Set[str]
    no_winner: Set[str]


class OwnershipManager:
    def __init__(
        self,
        r,
        *,
        namespace: str = "lem",
        presence: Presence,
        leases: Leases,
        node_id: str,
        top_k: int = 2,
        min_uptime_s: int = 120,
    ) -> None:
        self.r = r
        self.ns = namespace
        self.node_id = node_id
        self.presence = presence
        self.leases = leases
        self.top_k = max(1, int(top_k))
        self.min_uptime_s = max(0, int(min_uptime_s))

        self.key_tasks = f"{self.ns}:tasks"
        self._owned: Set[str] = set()

    @property
    def owned(self) -> Set[str]:
        return set(self._owned)

    async def all_tasks(self) -> List[str]:
        tasks = await self.r.smembers(self.key_tasks)
        return sorted(tasks) if tasks else []

    async def _fetch_task_weights(self, task_ids: List[str]) -> Dict[str, float]:
        """
        Fetch interval_s and cost from Redis task hashes, compute weights.
        """
        if not task_ids:
            return {}

        pipe = self.r.pipeline(transaction=False)
        keys = [f"{self.ns}:task:{tid}" for tid in task_ids]

        # HMGET each hash for needed fields
        for k in keys:
            pipe.hmget(k, "interval_s", "cost")

        rows = await pipe.execute()

        out: Dict[str, float] = {}
        for tid, row in zip(task_ids, rows):
            try:
                interval_s = int(row[0]) if row and row[0] is not None else 5
                cost = float(row[1]) if row and row[1] is not None else 2.0
            except (ValueError, TypeError):
                interval_s = 5
                cost = 2.0
            out[tid] = compute_weight(cost, interval_s)

        return out

    def _assign_weighted_topk(
        self,
        task_ids: List[str],
        nodes: List[str],
        weights: Dict[str, float],
    ) -> Dict[str, str]:
        """
        Deterministic weighted assignment:
          - tasks processed in sorted order
          - for each task, consider top-k HRW candidate nodes
          - choose candidate with minimal current load (total assigned weight)
          - tie-break by HRW score (stable)
        Returns: task_id -> assigned_node_id
        """
        loads: Dict[str, float] = {n: 0.0 for n in nodes}
        assignment: Dict[str, str] = {}

        for tid in task_ids:
            w = weights.get(tid, 0.001)
            candidates = hrw_topk(tid, nodes, self.top_k)

            # Pick candidate with smallest load; tie-break with HRW score
            best = None
            best_tuple = None  # (load, -score) because we want higher score if same load
            for n in candidates:
                tup = (loads[n], -hrw_score(tid, n))
                if best_tuple is None or tup < best_tuple:
                    best_tuple = tup
                    best = n

            assert best is not None
            assignment[tid] = best
            loads[best] += w

        return assignment

    async def reconcile(self) -> OwnershipResult:
        live = sorted(await self.presence.eligible_nodes(min_uptime_s=self.min_uptime_s))
        tasks = await self.all_tasks()

        no_winner: Set[str] = set()
        if not live:
            no_winner = set(tasks)
            desired: Set[str] = set()
        else:
            weights = await self._fetch_task_weights(tasks)
            assignment = self._assign_weighted_topk(tasks, live, weights)
            desired = {tid for tid, n in assignment.items() if n == self.node_id}

        acquired: Set[str] = set()
        renewed: Set[str] = set()
        released: Set[str] = set()
        skipped: Set[str] = set()

        # Release tasks we no longer desire
        to_release = self._owned - desired
        for tid in to_release:
            ok = await self.leases.release(tid, self.node_id)
            if ok:
                released.add(tid)

        # Acquire/renew desired
        for tid in desired:
            if tid in self._owned:
                ok = await self.leases.renew(tid, self.node_id)
                if ok:
                    renewed.add(tid)
                else:
                    self._owned.discard(tid)
                    skipped.add(tid)
            else:
                ok = await self.leases.try_acquire(tid, self.node_id)
                if ok:
                    acquired.add(tid)
                else:
                    skipped.add(tid)

        self._owned -= released
        self._owned |= acquired

        return OwnershipResult(
            owned=set(self._owned),
            acquired=acquired,
            renewed=renewed,
            released=released,
            skipped=skipped,
            no_winner=no_winner,
        )
