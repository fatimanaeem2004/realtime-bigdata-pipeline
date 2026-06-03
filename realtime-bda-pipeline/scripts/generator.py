import json, time, random, math
from datetime import datetime, timezone
from kafka import KafkaProducer

# --- Kafka setup ---
BOOTSTRAP = "localhost:29092"
TOPIC_EVENTS = "cart_events"
TOPIC_ORDERS = "orders"
TOPIC_ORDER_ITEMS = "order_items"

# --- Statistical knobs ---
LAMBDA_SESSIONS_PER_MIN = 12
PEAK_HOURS = set(range(18, 23))
ABANDON_AFTER_MIN = 30

P_VIEW = 1.0
P_ADD = 0.35
P_CHECKOUT = 0.25
P_PURCHASE = 0.12

LOGNORM_MU = 8.6
LOGNORM_SIGMA = 0.55

CUSTOMERS = ["U1", "U2", "U3", "U4"]
PRODUCTS = ["P1", "P2", "P3", "P4"]
DEVICES = ["D1", "D2", "D3", "D4"]
CAMPAIGNS = ["C1", "C2", "C3", None]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def poisson(lmbd):
    L = math.exp(-lmbd)
    k, p = 0, 1.0
    while p > L:
        k += 1
        p *= random.random()
    return k - 1


def lognormal_value():
    val = random.lognormvariate(LOGNORM_MU, LOGNORM_SIGMA)
    return round(min(max(val, 500), 50000), 2)


# --- Kafka producer ---
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
)

cart_seq = 1000
event_seq = 5000
order_seq = 2000

print("Generator started → producing to Kafka topics:", [TOPIC_EVENTS, TOPIC_ORDERS, TOPIC_ORDER_ITEMS])

while True:
    hour = datetime.now().hour
    multiplier = 1.8 if hour in PEAK_HOURS else 1.0
    sessions = poisson(LAMBDA_SESSIONS_PER_MIN * multiplier)

    for _ in range(sessions):
        cart_seq += 1
        cart_id = f"CART{cart_seq}"
        customer_id = random.choice(CUSTOMERS)
        device_id = random.choice(DEVICES)
        campaign_id = random.choice(CAMPAIGNS)

        # --- View cart ---
        event_seq += 1
        producer.send(TOPIC_EVENTS, {
            "event_id": f"E{event_seq}",
            "cart_id": cart_id,
            "customer_id": customer_id,
            "event_time": now_iso(),
            "event_type": "view_cart",
            "device_id": device_id,
            "campaign_id": campaign_id
        })

        # --- Add items ---
        items = []
        if random.random() < P_ADD:
            num_items = random.randint(1, 3)
            for _i in range(num_items):
                pid = random.choice(PRODUCTS)
                qty = random.randint(1, 2)
                price = random.uniform(500, 5000)
                items.append((pid, qty, price))

                event_seq += 1
                producer.send(TOPIC_EVENTS, {
                    "event_id": f"E{event_seq}",
                    "cart_id": cart_id,
                    "customer_id": customer_id,
                    "event_time": now_iso(),
                    "event_type": "add_to_cart",
                    "product_id": pid,
                    "quantity": qty,
                    "unit_price": price,
                    "device_id": device_id,
                    "campaign_id": campaign_id
                })

        # --- Checkout start ---
        if random.random() < P_CHECKOUT:
            event_seq += 1
            producer.send(TOPIC_EVENTS, {
                "event_id": f"E{event_seq}",
                "cart_id": cart_id,
                "customer_id": customer_id,
                "event_time": now_iso(),
                "event_type": "checkout_start",
                "device_id": device_id,
                "campaign_id": campaign_id
            })

        # --- Purchase vs abandon ---
        if random.random() < P_PURCHASE and items:
            order_seq += 1
            order_id = f"O{order_seq}"
            order_total = sum(q * p for _, q, p in items)

            # Purchase event
            event_seq += 1
            producer.send(TOPIC_EVENTS, {
                "event_id": f"E{event_seq}",
                "cart_id": cart_id,
                "customer_id": customer_id,
                "event_time": now_iso(),
                "event_type": "purchase",
                "order_id": order_id,
                "order_total": order_total,
                "payment_status": "PAID",
                "payment_method": random.choice(["card", "cod", "wallet"]),
                "device_id": device_id,
                "campaign_id": campaign_id
            })

            # Order summary
            producer.send(TOPIC_ORDERS, {
                "order_id": order_id,
                "cart_id": cart_id,
                "customer_id": customer_id,
                "order_time": now_iso(),
                "order_total": order_total,
                "payment_status": "PAID",
                "payment_method": random.choice(["card", "cod", "wallet"]),
                "device_id": device_id,
                "campaign_id": campaign_id,
                "recovered_flag": 0
            })

            # Order items
            for pid, qty, price in items:
                producer.send(TOPIC_ORDER_ITEMS, {
                    "order_item_id": f"{order_id}_{pid}",
                    "order_id": order_id,
                    "product_id": pid,
                    "quantity": qty,
                    "unit_price": price,
                    "line_total": round(qty * price, 2)
                })

        else:
            # Abandon event
            event_seq += 1
            producer.send(TOPIC_EVENTS, {
                "event_id": f"E{event_seq}",
                "cart_id": cart_id,
                "customer_id": customer_id,
                "event_time": now_iso(),
                "event_type": "abandon",
                "device_id": device_id,
                "campaign_id": campaign_id,
                "abandon_after_minutes": ABANDON_AFTER_MIN
            })

    producer.flush()
    time.sleep(60)
