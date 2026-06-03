import json
from datetime import datetime

from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BOOTSTRAP = "kafka:9092"
GROUP_ID = "mongo-ingest-FRESH"
MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "ecommerce"
TOPICS = ["cart_events"]

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("[OK] Consumer started")

for msg in consumer:
    doc = msg.value
    doc["_ingested_at"] = datetime.utcnow().isoformat()
    db.fact_cart_events.insert_one(doc)
    print("[INSERTED]", doc.get("cart_id", "no_cart_id"), doc.get("event_type", "no_event_type"))
