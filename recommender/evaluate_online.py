import json
import os
from collections import deque
from confluent_kafka import Consumer

KAFKA_CONFIG = {
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ['KAFKA_API_KEY'],
    'sasl.password': os.environ['KAFKA_API_SECRET'],
    'group.id': 'online-kpi-group-1',
    'auto.offset.reset': 'earliest'
}
TOPICS = ["byteflix.reco_responses", "byteflix.watch"]
JOIN_WINDOW_MINUTES = 10

def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)


    reco_requests = {} 
    watch_events = {}

    successful_recos = 0
    total_recos = 0

    print("🚀 Starting Online KPI Consumer...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))
            user_id = data.get('user_id')
            ts = data.get('ts')

            if topic == "byteflix.reco_responses":
                total_recos += 1
                movie_ids = data.get('movie_ids', [])
                
                if user_id not in reco_requests:
                    reco_requests[user_id] = deque()
                reco_requests[user_id].append((ts, movie_ids))
                
                if user_id in watch_events:
                    for watch_ts, watched_movie in list(watch_events[user_id]):
                        if ts < watch_ts < ts + (JOIN_WINDOW_MINUTES * 60) and watched_movie in movie_ids:
                            successful_recos += 1
                            watch_events[user_id].remove((watch_ts, watched_movie)) 
                            break 

            elif topic == "byteflix.watch":
                movie_id = data.get('movie_id')
                if user_id not in watch_events:
                    watch_events[user_id] = deque()
                watch_events[user_id].append((ts, movie_id))

                if user_id in reco_requests:
                    for reco_ts, reco_movies in list(reco_requests[user_id]):
                        if reco_ts < ts < reco_ts + (JOIN_WINDOW_MINUTES * 60) and movie_id in reco_movies:
                            successful_recos += 1
                            reco_requests[user_id].remove((reco_ts, reco_movies))
                            break

            if total_recos > 0:
                ictr = (successful_recos / total_recos) * 100
                print(f"\riCTR: {ictr:.2f}% ({successful_recos}/{total_recos} successful recommendations)", end="")

    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()