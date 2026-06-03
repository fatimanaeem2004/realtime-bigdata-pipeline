import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import numpy as np

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_PROBS = ["view"] * 70 + ["add_to_cart"] * 20 + ["purchase"] * 10

def generate_event():
    event_type = random.choice(EVENT_PROBS)
    order_total = 0

    if event_type == "purchase":
        order_total = round(float(np.random.lognormal(mean=4, sigma=0.5)), 2)

    return {
        "event_time": datetime.utcnow().isoformat(),
        "cart_id": f"c{random.randint(1, 10000)}",
        "customer_id": random.randint(1, 50),
        "device_id": random.randint(1, 5),
        "campaign_id": random.randint(1, 5),
        "event_type": event_type,
        "order_total": order_total
    }
    print("Kafka Producer started")

while True:
    event = generate_event()
    producer.send("cart_events", value=event)
    print("Produced:", event)
    time.sleep(np.random.poisson(1) + 1)
