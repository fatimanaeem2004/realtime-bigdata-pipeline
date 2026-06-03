from pymongo import MongoClient
import json, os

MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "ecommerce"
SEED_DIR = "/opt/airflow/data/seeds"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

for f in os.listdir(SEED_DIR):
    if f.endswith(".json"):
        path = os.path.join(SEED_DIR, f)
        col = f.replace(".json", "")
        docs = json.load(open(path))
        if isinstance(docs, list):
            db[col].delete_many({})
            db[col].insert_many(docs)
            print(f"✅ Seeded {len(docs)} docs into {col}")
