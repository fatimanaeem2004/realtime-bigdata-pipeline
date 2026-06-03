FROM python:3.10-slim

WORKDIR /app

# Install deps
RUN pip install --no-cache-dir kafka-python pymongo

# Copy only what we need
COPY kafka_to_mongo_consumer.py /app/kafka_to_mongo_consumer.py

CMD ["python", "/app/kafka_to_mongo_consumer.py"]
