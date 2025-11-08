import json
# import os
from collections import deque, defaultdict
from confluent_kafka import Consumer
import statsmodels.api as sm
# import time
import numpy as np  

KAFKA_CONFIG = { ... } 
TOPICS = ["byteflix.reco_responses", "byteflix.watch"]
JOIN_WINDOW_MINUTES = 10

def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)

    reco_events = defaultdict(dict)
  #  watch_events = defaultdict(deque)

    conversions = defaultdict(int)
    total_trials = defaultdict(int)

    print(" Starting A/B Test Consumer...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            user_id = data.get('user_id')
            ts = data.get('ts')

            if msg.topic() == "byteflix.reco_responses":
                model = data.get('model_used', 'unknown')
                req_id = data.get('request_id')
                movie_ids = data.get('movie_ids', [])
                
                total_trials[model] += 1
                reco_events[user_id][req_id] = (ts, movie_ids, model)

            elif msg.topic() == "byteflix.watch":
                watch_movie = data.get('movie_id')
                if user_id in reco_events:
                    for req_id, (reco_ts, reco_movies, model) in list(reco_events[user_id].items()):
                        if reco_ts < ts < reco_ts + (JOIN_WINDOW_MINUTES * 60) and watch_movie in reco_movies:
                            conversions[model] += 1
                            del reco_events[user_id][req_id] 
                            break

            print("\r--- A/B Test Live Results ---")
            c_pop = conversions.get('popularity', 0)
            t_pop = total_trials.get('popularity', 1)
            ictr_pop = (c_pop / t_pop) * 100
            print(f"Control (Popularity): {ictr_pop:.4f}% ({c_pop} / {t_pop})")
            
            c_cf = conversions.get('item_cf', 0)
            t_cf = total_trials.get('item_cf', 1)
            ictr_cf = (c_cf / t_cf) * 100
            print(f"Treatment (Item_CF):  {ictr_cf:.4f}% ({c_cf} / {t_cf})")

            if t_pop > 50 and t_cf > 50: 
                count = np.array([c_pop, c_cf])
                nobs = np.array([t_pop, t_cf])
                z_stat, p_value = sm.stats.proportions_ztest(count, nobs, alternative='two-sided')
                print(f"Z-statistic: {z_stat:.3f}, P-value: {p_value:.5f}")
                if p_value < 0.05:
                    print("Decision: Result is statistically significant! (p < 0.05)")
                else:
                    print("Decision: Result is NOT significant. (p >= 0.05)")

    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()