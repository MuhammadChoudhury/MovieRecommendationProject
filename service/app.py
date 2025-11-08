# service/app.py
import os
import joblib
import s3fs
import uuid
import json
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from confluent_kafka import Producer
from config import settings
from contextlib import asynccontextmanager

# --- 1. Provenance Environment Variables ---
GIT_SHA = os.environ.get("GIT_SHA", "unknown")
IMAGE_DIGEST = os.environ.get("IMAGE_DIGEST", "unknown")

# --- 2. Lifespan Function ---
# This function runs when the API starts up
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events.
    Connects to Kafka and loads models on startup.
    """
    print("--- API Starting Up: Loading Models & Connecting to Kafka ---")
    
    # --- Connect to Kafka ---
    try:
        app.state.kafka_producer = Producer({
            'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ['KAFKA_API_KEY'],
            'sasl.password': os.environ['KAFKA_API_SECRET'],
        })
        print("Kafka producer connected.")
    except KeyError:
        print("WARNING: Kafka credentials not found. Producer not started.")
        app.state.kafka_producer = None

    # --- Load Models from S3 ---
    s3 = s3fs.S3FileSystem()
    try:
        latest_version_path = f"{settings.S3_BUCKET_NAME}/model-registry/latest.txt"
        with s3.open(latest_version_path, 'r') as f:
            LATEST_VERSION = f.read().strip()
        print(f"Latest model version found: {LATEST_VERSION}")

        POP_PATH = f"{settings.S3_BUCKET_NAME}/model-registry/popularity/{LATEST_VERSION}/model.joblib"
        CF_PATH = f"{settings.S3_BUCKET_NAME}/model-registry/item_cf/{LATEST_VERSION}/model.joblib"
        
        app.state.models = {
            "popularity": joblib.load(s3.open(POP_PATH, 'rb')),
            "item_cf": joblib.load(s3.open(CF_PATH, 'rb')),
        }
        app.state.model_version = LATEST_VERSION
        print(f"Models for version {LATEST_VERSION} loaded successfully.")
    
    except FileNotFoundError:
        print("WARNING: Model files not found on S3. API will not serve recommendations.")
        app.state.models = None
        app.state.model_version = "error-no-models"

    yield # The application runs here
    
    # --- Shutdown ---
    print("--- API Shutting Down ---")
    if app.state.kafka_producer:
        app.state.kafka_producer.flush()

# --- 3. API Definition ---
# Pass the lifespan function to the FastAPI app
app = FastAPI(title="Movie Recommender API", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

# --- 4. API Endpoints ---
@app.get("/healthz", tags=["Status"])
def healthz():
    if app.state.models is None:
        raise HTTPException(status_code=503, detail="Models are not loaded.")
    return {"status": "ok", "version": app.state.model_version}

@app.get("/recommend/{user_id}", tags=["Recommendations"])
def recommend(user_id: int, k: int = 20):
    """
    Get movie recommendations for a given user.
    """
    if app.state.models is None:
        raise HTTPException(status_code=503, detail="Service unavailable: Models are not loaded.")

    request_id = str(uuid.uuid4())
    model_to_use = "popularity" # Control
    if user_id % 2 == 0:
        model_to_use = "item_cf" # Treatment
    
    if model_to_use == "popularity":
        recs = app.state.models["popularity"][:k]
    else:
        recs = app.state.models["item_cf"]["movie_ids"][:k] 

    # --- Provenance & Online Logging ---
    log_payload = {
        "request_id": request_id,
        "ts": int(time.time()),
        "user_id": user_id,
        "model_version": app.state.model_version,
        "model_used": model_to_use,
        "k": len(recs),
        "movie_ids": recs,
        "git_sha": GIT_SHA,
        "image_digest": IMAGE_DIGEST,
    }
    
    if app.state.kafka_producer:
        try:
            app.state.kafka_producer.produce("byteflix.reco_responses", value=json.dumps(log_payload))
            app.state.kafka_producer.poll(0)
        except Exception as e:
            print(f"Failed to produce to Kafka: {e}")

    return {
        "request_id": request_id,
        "user_id": user_id,
        "model_version": app.state.model_version,
        "model_used": model_to_use,
        "movie_ids": recs
    }