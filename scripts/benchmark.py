import requests
import time
import numpy as np

API_URL = "http://127.0.0.1:8000"
USER_ID = 123  # 
NUM_REQUESTS = 100

def benchmark_model(model_name: str):
    """Sends requests to the API for a specific model and measures latency."""
    latencies = []
    print(f"\nBenchmarking '{model_name}' model...")

    for i in range(NUM_REQUESTS):
        start_time = time.time()
        try:
            requests.get(f"{API_URL}/recommend/{USER_ID}", params={"model": model_name})
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            continue
        
        latency = (time.time() - start_time) * 1000  #
        latencies.append(latency)
        
        print(".", end="", flush=True)

    print("\n--- Results ---")
    print(f"Average Latency: {np.mean(latencies):.2f} ms")
    print(f"p95 Latency: {np.percentile(latencies, 95):.2f} ms") 

if __name__ == "__main__":
    
    try:
        health = requests.get(f"{API_URL}/healthz")
        if health.status_code == 200:
            print("API is healthy. Starting benchmark...")
            benchmark_model("popularity")
            benchmark_model("item_cf")
        else:
            print(f"API health check failed with status {health.status_code}")
    except requests.ConnectionError:
        print("\nError: Could not connect to the API.")
        print("Please ensure the API is running locally with the command:")
        print("uvicorn service.app:app")