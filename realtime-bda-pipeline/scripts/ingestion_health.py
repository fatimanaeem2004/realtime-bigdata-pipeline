from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.objectid import ObjectId

MONGO_URI = "mongodb://mongo:27017"
DB = "ecommerce"
COLL = "fact_cart_events"

client = MongoClient(MONGO_URI)
col = client[DB][COLL]

now = datetime.utcnow()
since = now - timedelta(seconds=120)

# Use ObjectId timestamp (works even if _ingested_at is stored as string)
since_oid = ObjectId.from_datetime(since)

recent = col.count_documents({"_id": {"$gte": since_oid}})
total = col.estimated_document_count()

print(f"[INGESTION CHECK] total={total}, last_120s={recent}")

if recent == 0:
    raise SystemExit("❌ No new events inserted in the last 120 seconds (by _id time).")

print("✅ Ingestion OK")
