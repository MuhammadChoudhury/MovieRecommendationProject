import os
import json
import random
import time
import requests
from confluent_kafka import Producer

# --- Configuration ---
# Load from environment variables for flexibility
API_URL = os.environ['API_URL']
TEAM_NAME = "byteflix"

# Use your local Kafka for testing, but this will need to change for GitHub Actions
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')

KAFKA_CONFIG = {
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ['KAFKA_API_KEY'],
    'sasl.password': os.environ['KAFKA_API_SECRET'],
}

def acked(err, msg):
    """Callback function for Kafka producer results."""
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        # Uncomment the line below for verbose logging
        # print(f"Message produced to {msg.topic()} [{msg.partition()}]")
        pass

def main():
    producer = Producer(KAFKA_CONFIG)
    print("🚀 Probe script started...")
    print(f"API URL: {API_URL}")
    print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")

    while True:
        # Pick a random user from the MovieLens 1M dataset range
        user_id = random.randint(1, 6040)
        start_time = time.time()
        
        try:
            # 1. Send a 'reco_requests' event
            req_payload = json.dumps({"ts": int(start_time), "user_id": user_id}).encode('utf-8')
            producer.produce(f'{TEAM_NAME}.reco_requests', key=str(user_id), value=req_payload, callback=acked)
            
            # 2. Call the live API
            response = requests.get(f"{API_URL}/recommend/{user_id}", params={"k": 20, "model": "popularity"})
            response.raise_for_status() # Raises an exception for 4xx or 5xx status codes
            
            latency_ms = int((time.time() - start_time) * 1000)
            data = response.json()
            
            # 3. Send a 'reco_responses' event
            res_payload = json.dumps({
                "ts": int(time.time()),
                "user_id": user_id,
                "status": response.status_code,
                "latency_ms": latency_ms,
                "k": data.get("k", 0),
                "model": data.get("model", "unknown"),
                "movie_ids": data.get("movie_ids", [])
            }).encode('utf-8')
            producer.produce(f'{TEAM_NAME}.reco_responses', key=str(user_id), value=res_payload, callback=acked)
            
            print(f"Probed user {user_id}: status={response.status_code}, latency={latency_ms}ms")
            
        except requests.RequestException as e:
            print(f"API request failed for user {user_id}: {e}")
        
        producer.poll(0) # Serve delivery callbacks
        time.sleep(5) # Wait 5 seconds before the next probe

if __name__ == "__main__":
    if 'API_URL' not in os.environ:
        print("Error: API_URL environment variable not set.")
        print("Example: export API_URL='https://recommender-api-....run.app'")
    else:
        main()