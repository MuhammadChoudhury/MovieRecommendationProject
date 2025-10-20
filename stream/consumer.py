# stream/consumer.py
import json
import os
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd
import s3fs
from confluent_kafka import Consumer
from pydantic import BaseModel, ValidationError

# --- Pydantic Schemas for Data Validation ---
class WatchEvent(BaseModel):
    ts: int
    user_id: int
    movie_id: int
    minute: int

class RatingEvent(BaseModel):
    ts: int
    user_id: int
    movie_id: int
    rating: int

# --- CHANGE HERE: Match your topic names ---
SCHEMA_MAP = {
    "byteflix.watch": WatchEvent,
    "byteflix.rate": RatingEvent
}

# --- CHANGE HERE: Simplified Kafka config for local Docker ---
# This version doesn't require environment variables and connects directly
# to your local Kafka instance.
KAFKA_CONFIG = {
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ['KAFKA_API_KEY'],
    'sasl.password': os.environ['KAFKA_API_SECRET'],
    'group.id': 'stream-ingestor-group-cloud-1', # New group ID for the new cluster
    'auto.offset.reset': 'earliest'
}

S3_BUCKET = os.environ['S3_BUCKET_NAME']
TOPICS = ["byteflix.watch", "byteflix.rate"]

# Batching parameters: write to S3 every 1000 messages or every 60 seconds
BATCH_SIZE = 1000
BATCH_TIMEOUT_SECONDS = 60

s3 = s3fs.S3FileSystem()


def write_batch_to_s3(buffer):
    """Writes the contents of the buffer to S3, partitioned by event type and date."""
    for event_type, events in buffer.items():
        if not events:
            continue

        print(f"Writing batch of {len(events)} for event type '{event_type}'...")
        df = pd.DataFrame([e.dict() for e in events])
        
        now = datetime.utcnow()
        filename = f"data_{int(now.timestamp())}.parquet"
        
        path = f"s3://{S3_BUCKET}/snapshots/{event_type}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{filename}"

        try:
            with s3.open(path, 'wb') as f:
                df.to_parquet(f, index=False)
            print(f"Successfully wrote to {path}")
        except Exception as e:
            print(f"Failed to write to S3: {e}")


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)

    event_buffer = defaultdict(list)
    last_write_time = time.time()
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if time.time() - last_write_time > BATCH_TIMEOUT_SECONDS and any(event_buffer.values()):
                    print("Batch timeout reached, writing buffer to S3...")
                    write_batch_to_s3(event_buffer)
                    event_buffer.clear()
                    last_write_time = time.time()
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            SchemaModel = SCHEMA_MAP.get(topic)
            if not SchemaModel:
                print(f"No schema found for topic {topic}, skipping.")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                validated_event = SchemaModel(**data)
                event_buffer[topic].append(validated_event)
                
            except (json.JSONDecodeError, ValidationError) as e:
                print(f"Schema validation failed for message on topic {topic}: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

            if sum(len(v) for v in event_buffer.values()) >= BATCH_SIZE:
                print("Batch size reached, writing buffer to S3...")
                write_batch_to_s3(event_buffer)
                consumer.commit(asynchronous=False)
                event_buffer.clear()
                last_write_time = time.time()

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        print("Final flush of buffer before shutting down...")
        write_batch_to_s3(event_buffer)
        consumer.close()


if __name__ == "__main__":
    if 'S3_BUCKET_NAME' not in os.environ:
        print("Error: S3_BUCKET_NAME environment variable not set.")
    else:
        main()