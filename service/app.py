import os
import joblib
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from config import settings 

MODEL_REGISTRY_PATH = "model_registry"

try:
    POP_PATH = os.path.join(settings.MODEL_REGISTRY_PATH, settings.POPULARITY_MODEL_DIR, "model.joblib")
    popularity_model = joblib.load(POP_PATH)
    
    CF_PATH = os.path.join(settings.MODEL_REGISTRY_PATH, settings.ITEM_CF_MODEL_DIR, "model.joblib")
    item_cf_model = joblib.load(CF_PATH)
    
    print("Models loaded successfully from config paths.")
except FileNotFoundError:
    print("Warning: Model files not found. API will run with limited functionality.")
    popularity_model = []
    item_cf_model = None

MODELS = {
    "popularity": popularity_model,
    "item_cf": item_cf_model,
}

app = FastAPI(title="Movie Recommender API", version="1.0")

Instrumentator().instrument(app).expose(app)

@app.get("/healthz", tags=["Status"])
def healthz():
    """Health check endpoint to confirm the API is running."""
    return {"status": "ok", "version": "1.0"}

@app.get("/recommend/{user_id}", tags=["Recommendations"])
def recommend(user_id: int, k: int = 20, model: str = "popularity"):
    """
    Get movie recommendations for a given user.
    - user_id: The ID of the user to get recommendations for.
    - k: The number of recommendations to return.
    - model: The name of the model to use (e.g., 'popularity', 'item_cf').
    """
    if model not in MODELS:
        raise HTTPException(status_code=400, detail=f"Model '{model}' not found.")
        
    print(f"Request for user {user_id} with model '{model}' and k={k}")

    if model == "popularity":
        recs = MODELS[model][:k]
        return {
            "user_id": user_id,
            "movie_ids": recs,
            "model": "popularity",
            "k": len(recs)
        }
    
    if model == "item_cf":
        if item_cf_model is None:
            raise HTTPException(status_code=500, detail="Item-CF model not loaded.")
        
        recs = popularity_model[:k]
        return {
            "user_id": user_id,
            "movie_ids": recs,
            "model": "item_cf (fallback to popularity)",
            "k": len(recs)
        }

    return {} 