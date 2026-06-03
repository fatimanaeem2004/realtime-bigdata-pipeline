import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime, timezone

# -----------------------------
# Config
# -----------------------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "cart_events"
MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "ecommerce"
COLLECTION = "fact_cart_events"

# -----------------------------
# Mongo connection
# -----------------------------
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DB_NAME]
mongo_collection = mongo_db[COLLECTION]

# -----------------------------
# Kafka consumer
# -----------------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="cart-events-consumer",
)

print("Kafka consumer started. Waiting for events...")

# -----------------------------
# Consume messages
# -----------------------------
for message in consumer:
    event = message.value

    # Convert event_time string -> datetime (Mongo Date)
    t = event.get("event_time")
if isinstance(t, str):
    try:
        s = t.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"   # make it ISO-friendly for fromisoformat
        dt = datetime.fromisoformat(s)

        # if no timezone info, force UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        event["event_time"] = dt
    except Exception:
        # If parsing fails, keep original value (string) so you can detect it later
        pass

    # add ingestion time (optional but helpful)
    event["_ingested_at"] = datetime.utcnow()

    mongo_collection.insert_one(event)
    print(" Inserted event:", event)
