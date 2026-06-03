"""
Archiving policy (Requirement #4)

Policy:
- HOT store: ecommerce.fact_cart_events (real-time stream for KPIs)
- Threshold: 300 MB (decimal)
- When threshold exceeded:
    renameCollection (atomic) to:
      ecommerce_archive.fact_cart_events_<timestamp>
  then ingestion continues into a fresh ecommerce.fact_cart_events automatically.
- Metadata stored in:
    ecommerce_archive.archive_metadata
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")

SOURCE_DB = os.getenv("SOURCE_DB", "ecommerce")
SOURCE_COLL = os.getenv("SOURCE_COLL", "fact_cart_events")

ARCHIVE_DB = os.getenv("ARCHIVE_DB", "ecommerce_archive")
META_COLL = os.getenv("META_COLL", "archive_metadata")

# 300 MB (decimal) to match requirement wording
MAX_BYTES = int(os.getenv("ARCHIVE_MAX_BYTES", "300000000"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    client = MongoClient(MONGO_URI)
    src_db = client[SOURCE_DB]
    src_coll = src_db[SOURCE_COLL]

    # If collection doesn't exist yet, skip.
    if SOURCE_COLL not in src_db.list_collection_names():
        print(f"ℹ️ {SOURCE_DB}.{SOURCE_COLL} not found — nothing to archive")
        return

    stats = src_db.command("collStats", SOURCE_COLL)
    size_bytes = int(stats.get("size", 0))
    count = int(stats.get("count", 0))

    if size_bytes <= MAX_BYTES:
        print(f"ℹ️ {size_bytes/1e6:.2f} MB <= 300 MB — skip archive")
        return

    # Timestamp batch id
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_coll_name = f"{SOURCE_COLL}_{batch_id}"

    # Optional min/max event_time for metadata (ISO strings sort fine)
    min_event_time = None
    max_event_time = None
    try:
        agg = list(
            src_coll.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "min_event_time": {"$min": "$event_time"},
                            "max_event_time": {"$max": "$event_time"},
                        }
                    }
                ]
            )
        )
        if agg:
            min_event_time = agg[0].get("min_event_time")
            max_event_time = agg[0].get("max_event_time")
    except Exception as e:
        print(f"⚠️ Could not compute min/max event_time: {e}")

    # Atomic move: ecommerce.fact_cart_events -> ecommerce_archive.fact_cart_events_<batch>
    src_ns = f"{SOURCE_DB}.{SOURCE_COLL}"
    dst_ns = f"{ARCHIVE_DB}.{archive_coll_name}"

    client.admin.command(
        "renameCollection",
        src_ns,
        to=dst_ns,
        dropTarget=False,
    )

    # Metadata record
    meta = {
        "batch_id": batch_id,
        "archived_at": utc_now_iso(),
        "source_namespace": src_ns,
        "archive_namespace": dst_ns,
        "trigger_size_bytes": size_bytes,
        "trigger_size_mb": round(size_bytes / 1e6, 2),
        "doc_count": count,
        "min_event_time": min_event_time,
        "max_event_time": max_event_time,
        "metadata_format": "JSON document in MongoDB collection ecommerce_archive.archive_metadata",
        "storage_policy": {
            "hot_store": {
                "db": SOURCE_DB,
                "collection": SOURCE_COLL,
                "cap_bytes": MAX_BYTES,
                "purpose": "fast real-time KPIs/joins (Spark + dashboard)",
            },
            "archive_store": {
                "db": ARCHIVE_DB,
                "collection_naming": f"{SOURCE_COLL}_YYYYMMDD_HHMMSS",
                "format": "MongoDB collection (same schema as source)",
                "write_mode": "append-only (new archive collections)",
                "retention": "keep for project duration for historical analysis/audits/model training",
            },
        },
    }

    client[ARCHIVE_DB][META_COLL].insert_one(meta)

    print(f"✅ Archived {src_ns} ({size_bytes/1e6:.2f} MB, {count} docs) -> {dst_ns} | batch={batch_id}")


if __name__ == "__main__":
    main()
