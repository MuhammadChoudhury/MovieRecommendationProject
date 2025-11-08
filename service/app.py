import os
import joblib
import s3fs
import uuid
import json
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from confluent_kafka import Producer
from config import settings

s3 = s3fs.S3FileSystem()
producer = Producer({
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ['KAFKA_API_KEY'],
    'sasl.password': os.environ['KAFKA_API_SECRET'],
})

GIT_SHA = os.environ.get("GIT_SHA", "unknown")
IMAGE_DIGEST = os.environ.get("IMAGE_DIGEST", "unknown")

def load_model_from_s3(model_path: str):
    """Downloads and loads a model artifact from S3."""
    print(f"Loading model from S3 path: {model_path}")
    with s3.open(model_path, 'rb') as f:
        return joblib.load(f)

print("--- API Starting Up: Loading Models ---")
latest_version_path = f"{settings.S3_BUCKET_NAME}/model-registry/latest.txt"
with s3.open(latest_version_path, 'r') as f:
    LATEST_VERSION = f.read().strip()
print(f"Latest model version found: {LATEST_VERSION}")

POP_PATH = f"{settings.S3_BUCKET_NAME}/model-registry/popularity/{LATEST_VERSION}/model.joblib"
CF_PATH = f"{settings.S3_BUCKET_NAME}/model-registry/item_cf/{LATEST_VERSION}/model.joblib"

MODELS = {
    "popularity": load_model_from_s3(POP_PATH),
    "item_cf": load_model_from_s3(CF_PATH),
}
print(f"Models for version {LATEST_VERSION} loaded successfully.")

app = FastAPI(title="Movie Recommender API", version=LATEST_VERSION)
Instrumentator().instrument(app).expose(app)


@app.get("/healthz", tags=["Status"])
def healthz():
    return {"status": "ok", "version": LATEST_VERSION}

@app.get("/recommend/{user_id}", tags=["Recommendations"])
def recommend(user_id: int, k: int = 20):
    """
    Get movie recommendations for a given user.
    This endpoint runs an A/B test:
    - 50% get 'popularity' (Control)
    - 50% get 'item_cf' (Treatment)
    """
    request_id = str(uuid.uuid4())
    model_to_use = "popularity" 
    
    if user_id % 2 == 0:
        model_to_use = "item_cf"
    
    if model_to_use == "popularity":
        recs = MODELS["popularity"][:k]
    else:
        recs = MODELS["item_cf"]["movie_ids"][:k] 

    log_payload = {
        "request_id": request_id,
        "ts": int(time.time()),
        "user_id": user_id,
        "model_version": LATEST_VERSION,
        "model_used": model_to_use,
        "k": len(recs),
        "movie_ids": recs,
        "git_sha": GIT_SHA,
        "image_digest": IMAGE_DIGEST,
    }
    
    try:
        producer.produce("byteflix.reco_responses", value=json.dumps(log_payload))
        producer.poll(0)
    except Exception as e:
        print(f"Failed to produce to Kafka: {e}")

    return {
        "request_id": request_id,
        "user_id": user_id,
        "model_version": LATEST_VERSION,
        "model_used": model_to_use,
        "movie_ids": recs
    }