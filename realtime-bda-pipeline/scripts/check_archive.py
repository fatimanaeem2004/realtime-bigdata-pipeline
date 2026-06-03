from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = os.getenv("DB_NAME", "ecommerce")

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

count = db.archive_metadata.count_documents({})
print(f"[CHECK] archive_metadata entries = {count}")

# Fail task if no metadata exists (meaning no archive ever happened)
# (During demo you can lower threshold to trigger at least one archive)
if count == 0:
    raise SystemExit("No archives recorded yet (archive_metadata is empty).")

print("[CHECK] Archive metadata exists ✅")
