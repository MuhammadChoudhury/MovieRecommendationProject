import os
import json
import random
import time
import requests
from confluent_kafka import Producer

API_URL = os.environ['API_URL']
TEAM_NAME = "byteflix"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
}

if 'KAFKA_API_KEY' in os.environ:
    KAFKA_CONFIG.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ['KAFKA_API_KEY'],
        'sasl.password': os.environ['KAFKA_API_SECRET'],
    })

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")

def main():
    print("--- Probe Script Initializing ---")
    print(f"API URL: {API_URL}")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    try:
        producer = Producer(KAFKA_CONFIG)
        print("Kafka producer created successfully.")
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return

    print("Probe script started...")

    max_iters = int(os.getenv("PROBE_ITERATIONS", 100))

    for i in range(max_iters):
        user_id = random.randint(1, 6040)
        start_time = time.time()
        print(f"[{i+1}/{max_iters}] Probing user {user_id}...")

        try:
            req_payload = json.dumps({"ts": int(start_time), "user_id": user_id}).encode("utf-8")
            producer.produce(f"{TEAM_NAME}.reco_requests", key=str(user_id), value=req_payload, callback=acked)

            response = requests.get(f"{API_URL}/recommend/{user_id}", params={"k": 20}, timeout=30)
            response.raise_for_status()
            latency_ms = int((time.time() - start_time) * 1000)
            data = response.json()

            res_payload = json.dumps({
                "ts": int(time.time()),
                "user_id": user_id,
                "status": response.status_code,
                "latency_ms": latency_ms,
                "k": data.get("k", 0),
                "model": data.get("model", "unknown"),
                "movie_ids": data.get("movie_ids", [])
            }).encode("utf-8")
            producer.produce(f"{TEAM_NAME}.reco_responses", key=str(user_id), value=res_payload, callback=acked)

            print(f"  ✓ Probe complete (latency={latency_ms} ms)")
        except requests.Timeout:
            print(f"  ✗ Timeout after 30 s for user {user_id}")
        except requests.RequestException as e:
            print(f"  ✗ Request failed: {e}")

        producer.poll(0)
        time.sleep(10)

    print("Probe finished successfully.")


if __name__ == "__main__":
    if 'API_URL' not in os.environ or 'KAFKA_BOOTSTRAP_SERVERS' not in os.environ:
        print("Error: Required environment variables not set.")
        print("Please set API_URL and KAFKA_BOOTSTRAP_SERVERS.")
    else:
        main()