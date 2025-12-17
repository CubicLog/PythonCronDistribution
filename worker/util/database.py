import json, hashlib, pymongo
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, List, Dict, Set
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo import ReplaceOne
from bson import encode as bson_encode

def utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        # Assume it's local time and convert it properly to UTC
        return ts.astimezone().astimezone(timezone.utc)
    # If it's already aware, just convert to UTC
    return ts.astimezone(timezone.utc)

def canonical_ids(ids: dict) -> dict:
    # sort keys to make the key stable regardless of dict order
    return {k: ids[k] for k in sorted(ids)}

def make_key(source: str, ids: dict, ts: datetime, hash_key: bool = True) -> str:
    ts = utc(ts).isoformat().replace("+00:00", "Z")
    ids_sorted = canonical_ids(ids)
    key_str = json.dumps({"s": source, "i": ids_sorted, "t": ts}, separators=(",", ":"), sort_keys=True)
    if hash_key:
        return hashlib.sha1(key_str.encode("utf-8")).hexdigest()  # short, safe for any size
    # readable (but longer) alternative:
    parts = [source] + [f"{k}={ids_sorted[k]}" for k in ids_sorted] + [ts]
    return "|".join(parts)

def make_series_key(source: str, ids: dict, hash_key: bool = True) -> str:
    ids_sorted = canonical_ids(ids)
    key_str = json.dumps({"s": source, "i": ids_sorted}, separators=(",", ":"), sort_keys=True)
    if hash_key:
        return hashlib.sha1(key_str.encode("utf-8")).hexdigest()  # short, safe for any size
    # readable (but longer) alternative:
    parts = [source] + [f"{k}={ids_sorted[k]}" for k in ids_sorted]
    return "|".join(parts)

class AnyTimeDB:
    def __init__(self, uri: str, db_name: str):
        self.client = AsyncIOMotorClient(
            uri,
            tz_aware=True,
            tzinfo=timezone.utc,
            serverSelectionTimeoutMS=1000,
            connectTimeoutMS=1000,
            socketTimeoutMS=2000
        )
        self.db: AsyncIOMotorDatabase = self.client[db_name]
        self._gatekeeper_ready = False

    async def _ensure_gatekeeper(self):
        """Create the ts_keys collection and unique (c, k) index once."""
        if self._gatekeeper_ready:
            return
        # create (or no-op if exists)
        try:
            await self.db.create_collection("ts_keys")
        except pymongo.errors.CollectionInvalid:
            pass
        # unique key on (collection, key)
        await self.db.ts_keys.create_index(
            [("c", 1), ("k", 1)],
            unique=True,
            name="uniq_collection_key"
        )
        self._gatekeeper_ready = True

    async def ensure_ts_collection(self, collection: str):
        """Create a time-series collection and some helpful indexes."""
        # ---- main time-series collection ----
        try:
            await self.db.create_collection(
                collection,
                timeseries={
                    "timeField": "observed_at",
                    "metaField": "series",
                    "granularity": "minutes",
                },
            )
        except pymongo.errors.CollectionInvalid:
            # already exists
            pass

        # helpful general-purpose indexes for main TS collection
        await self.db[collection].create_index(
            [("series.source", 1), ("observed_at", -1)]
        )
        await self.db[collection].create_index(
            [("series_key", 1), ("observed_at", -1)]
        )

        # ensure gatekeeper exists
        await self._ensure_gatekeeper()

        # ---- _latest collection: NORMAL collection (not time-series) ----
        latest_name = collection + "_latest"

        try:
            # create as a regular collection (no timeseries=...)
            await self.db.create_collection(latest_name)
        except pymongo.errors.CollectionInvalid:
            # already exists
            pass

        # indexes for _latest
        # one doc per series_key:
        await self.db[latest_name].create_index(
            [("series_key", 1)], unique=True
        )
        # useful if you ever sort/filter by recency:
        await self.db[latest_name].create_index(
            [("observed_at", -1)]
        )
        # optional: if you filter by source a lot:
        await self.db[latest_name].create_index(
            [("series.source", 1), ("observed_at", -1)]
        )

    # -------------------- single-point write (insert-only, dedup via gatekeeper) --------------------
    async def write_point(
        self,
        collection: str,
        source: str,
        ids: dict[str, Any],
        observed_at: datetime,
        data: dict[str, Any],
    ):
        await self._ensure_gatekeeper()

        observed_at = utc(observed_at)
        ids_norm = canonical_ids(ids)
        key = make_key(source, ids_norm, observed_at)
        series_key = make_series_key(source, ids_norm)

        # 1) Gatekeeper insert
        try:
            await self.db.ts_keys.insert_one({"c": collection, "k": key})
        except DuplicateKeyError:
            # already ingested this (source+ids+timestamp) for this collection
            return

        # 2) Actual TS insert (insert-only)
        doc = {
            "_id": key,  # readable, but NOT unique-enforced in TS
            "observed_at": observed_at,
            "series": {"source": source, **ids_norm},
            "series_key": series_key,
            **(data or {}),
        }
        try:
            await self.db[collection].insert_one(doc)
        except DuplicateKeyError:
            # Extremely rare race: another writer got in between gatekeeper and TS insert
            # It's safe to ignore
            pass
        
        # 3) replace/upsert into <collection>_latest for fast retrieval
        existing = await self.db[collection + "_latest"].find_one(
            {"series_key": series_key},
            projection={"observed_at": 1}
        )
        if existing is None or existing["observed_at"] < observed_at:
            # This doc is newer → replace or insert
            doc = {k: v for k, v in doc.items() if k != "_id"}
            await self.db[collection + "_latest"].replace_one(
                {"series_key": series_key},
                doc,
                upsert=True
            )

        # -------------------- batched write (insert-only, dedup via gatekeeper bulk) --------------------
    
    async def write_many(self, collection: str, items: Iterable[dict]):
        """
        items: list of dicts with keys:
          - source: str
          - ids: dict
          - observed_at: datetime
          - data: dict (optional)
        """

        await self._ensure_gatekeeper()

        latest_collection_name = f"{collection}_latest"
        latest_coll = self.db[latest_collection_name]

        # ---- helpers ----
        async def insert_many_sized(coll, docs: List[Dict[str, Any]],
                                    max_batch_bytes: int = 8_000_000,
                                    max_batch_count: int = 20_000,
                                    ordered: bool = False):
            """
            Insert docs in batches that stay under max_batch_bytes and max_batch_count.
            Returns nothing; raises on non-dup errors.
            """
            batch: List[Dict[str, Any]] = []
            batch_bytes = 5  # small overhead buffer

            async def flush():
                nonlocal batch, batch_bytes
                if not batch:
                    return
                await coll.insert_many(batch, ordered=ordered, bypass_document_validation=True)
                batch = []
                batch_bytes = 5

            for d in docs:
                b = bson_encode(d)
                blen = len(b)
                if blen >= 16_000_000:
                    raise ValueError(f"Single document exceeds 16MB ({blen} bytes)")
                if batch and (batch_bytes + blen > max_batch_bytes or len(batch) >= max_batch_count):
                    await flush()
                batch.append(d)
                batch_bytes += blen
            await flush()

        async def insert_gatekeeper_and_get_ok_indices(
            gate_docs: List[Dict[str, Any]],
            start_index: int,
            max_batch_bytes: int = 8_000_000,
            max_batch_count: int = 20_000,
        ) -> Set[int]:
            """
            Insert gatekeeper docs in size-aware batches and return the set of global indices
            that were successfully inserted (i.e., NOT duplicates).
            'start_index' is the global index offset for this gate_docs block.
            """
            ok: Set[int] = set()
            batch: List[Dict[str, Any]] = []
            batch_bytes = 5
            batch_first_global_idx = start_index

            async def flush():
                nonlocal batch, batch_bytes, batch_first_global_idx, ok
                if not batch:
                    return
                try:
                    # unordered for speed; will raise BulkWriteError for dups
                    await self.db.ts_keys.insert_many(batch, ordered=False)
                    # success: all docs in this batch are new
                    ok.update(range(batch_first_global_idx,
                                    batch_first_global_idx + len(batch)))
                except BulkWriteError as bwe:
                    # mark non-dup errors fatal; dup errors just remove those indices
                    write_errors = bwe.details.get("writeErrors", []) if bwe.details else []
                    dup_local_idxs = {e["index"] for e in write_errors if e.get("code") == 11000}
                    non_dup = [e for e in write_errors if e.get("code") != 11000]
                    if non_dup:
                        # surface first non-dup error
                        raise
                    # all others succeeded
                    for i_local in range(len(batch)):
                        if i_local not in dup_local_idxs:
                            ok.add(batch_first_global_idx + i_local)
                finally:
                    batch.clear()
                    batch_bytes = 5
                    batch_first_global_idx = None  # will be set when we start a new batch

            for i, d in enumerate(gate_docs):
                b = bson_encode(d)
                blen = len(b)
                if blen >= 16_000_000:
                    # Extremely unlikely (gate docs are tiny), but guard anyway
                    # Try inserting alone to classify dup/non-dup deterministically
                    try:
                        await self.db.ts_keys.insert_one(d)
                        ok.add(start_index + i)
                    except BulkWriteError as bwe:
                        errs = bwe.details.get("writeErrors", []) if bwe.details else []
                        if not errs or errs[0].get("code") != 11000:
                            raise
                    continue

                # start a new batch if needed
                if batch and (batch_bytes + blen > max_batch_bytes or len(batch) >= max_batch_count):
                    await flush()
                if not batch:
                    batch_first_global_idx = start_index + i
                batch.append(d)
                batch_bytes += blen

            await flush()
            return ok

        # ---- 0) Build docs + gatekeeper docs (matching order) ----
        def utc(ts):
            return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)

        docs: List[Dict[str, Any]] = []
        gate_docs: List[Dict[str, Any]] = []

        for it in items:
            ts = utc(it["observed_at"])
            ids_norm = canonical_ids(it["ids"])
            key = make_key(it["source"], ids_norm, ts)
            series_key = make_series_key(it["source"], ids_norm)
            docs.append({
                "_id": key,
                "observed_at": ts,
                "series": {"source": it["source"], **ids_norm},
                "series_key": series_key,
                **(it.get("data") or {}),
            })
            gate_docs.append({"c": collection, "k": key})

        if not docs:
            return

        # ---- 1) Gatekeeper insert in size-aware batches; collect OK indices ----
        OK: Set[int] = set()
        WINDOW = 200_000  # process in windows to limit overhead (tune as needed)
        for start in range(0, len(gate_docs), WINDOW):
            end = min(start + WINDOW, len(gate_docs))
            sub_ok = await insert_gatekeeper_and_get_ok_indices(
                gate_docs[start:end],
                start_index=start,
                max_batch_bytes=8_000_000,
                max_batch_count=20_000,
            )
            OK.update(sub_ok)

        if not OK:
            return

        # ---- 2) Insert only NEW docs into the time-series collection (size-aware batches) ----
        to_insert = [docs[i] for i in sorted(OK)]
        try:
            await insert_many_sized(
                self.db[collection],
                to_insert,
                max_batch_bytes=8_000_000,
                max_batch_count=20_000,
                ordered=False,
            )
        except BulkWriteError as bwe2:
            # If any non-dup errors occur, bubble up; dup errors can happen due to races.
            write_errors = bwe2.details.get("writeErrors", []) if bwe2.details else []
            for err in write_errors:
                if err.get("code") != 11000:
                    raise

        # ---- 3) Update <collection>_latest for affected series_keys ----
        if not to_insert:
            return

        # (a) Within this batch, pick the latest doc per series_key
        latest_candidates: Dict[str, Dict[str, Any]] = {}
        for d in to_insert:
            sk = d["series_key"]
            current = latest_candidates.get(sk)
            if current is None or d["observed_at"] > current["observed_at"]:
                latest_candidates[sk] = d

        if not latest_candidates:
            return

        # (b) Fetch existing latest docs for those series_keys in **batched** $in queries
        existing_latest: Dict[str, datetime] = {}
        all_keys = list(latest_candidates.keys())
        KEY_WINDOW = 50_000  # tune as needed; keeps the $in doc well under 16MB

        for start in range(0, len(all_keys), KEY_WINDOW):
            sub_keys = all_keys[start:start + KEY_WINDOW]
            cursor = latest_coll.find(
                {"series_key": {"$in": sub_keys}},
                projection={"series_key": 1, "observed_at": 1, "_id": 0},
            )
            async for doc in cursor:
                existing_latest[doc["series_key"]] = doc["observed_at"]

        # (c) Build and flush ReplaceOne ops in size-aware batches
        MAX_BATCH_BYTES = 8_000_000
        MAX_BATCH_COUNT = 20_000
        ops_batch: List[ReplaceOne] = []
        batch_bytes = 5  # small overhead buffer

        async def flush_latest_batch():
            nonlocal ops_batch, batch_bytes
            if not ops_batch:
                return
            try:
                await latest_coll.bulk_write(ops_batch, ordered=False)
            except BulkWriteError as bwe3:
                # Ignore pure dup-key races; surface anything else.
                write_errors = bwe3.details.get("writeErrors", []) if bwe3.details else []
                non_dup = [e for e in write_errors if e.get("code") != 11000]
                if non_dup:
                    raise
            finally:
                ops_batch = []
                batch_bytes = 5

        for sk, cand in latest_candidates.items():
            prev_ts = existing_latest.get(sk)
            # UTC-aware on both sides, so direct comparison is fine
            if prev_ts is not None and prev_ts >= cand["observed_at"]:
                continue

            # Strip _id so we don't hit immutable _id errors in _latest
            latest_doc = {k: v for k, v in cand.items() if k != "_id"}

            # Estimate size based on BSON of the replacement doc
            encoded = bson_encode(latest_doc)
            blen = len(encoded)
            if blen >= 16_000_000:
                raise ValueError(f"Single _latest document exceeds 16MB ({blen} bytes)")

            # If adding this op would exceed our batch limits, flush first
            if ops_batch and (
                batch_bytes + blen > MAX_BATCH_BYTES
                or len(ops_batch) >= MAX_BATCH_COUNT
            ):
                await flush_latest_batch()

            ops_batch.append(
                ReplaceOne(
                    {"series_key": sk},
                    latest_doc,
                    upsert=True,
                )
            )
            batch_bytes += blen

        # Flush any remaining ops
        if ops_batch:
            await flush_latest_batch()

    # -------------------- read helpers --------------------
    async def get_at(self, collection: str, source: str, ids: dict, observed_at: datetime) -> Optional[dict]:
        key = make_key(source, canonical_ids(ids), utc(observed_at))
        return await self.db[collection].find_one({"_id": key}, {"_id": 0})

    async def close(self):
        self.client.close()
