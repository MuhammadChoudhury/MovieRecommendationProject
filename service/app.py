import os
import joblib
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from config import settings

DEFAULT_TOP = list(range(1, 101))  

def _load_list_model(path: str):
    try:
        return joblib.load(path)
    except Exception:
        return None

POP_PATH = os.path.join(
    settings.MODEL_REGISTRY_PATH,
    settings.POPULARITY_MODEL_DIR,
    "model.joblib",
)
CF_PATH = os.path.join(
    settings.MODEL_REGISTRY_PATH,
    settings.ITEM_CF_MODEL_DIR,
    "model.joblib",
)

popularity_model = _load_list_model(POP_PATH)
item_cf_model = _load_list_model(CF_PATH)  

MODELS = {"popularity": "popularity", "item_cf": "item_cf"}

app = FastAPI(title="Movie Recommender API", version="1.0")
Instrumentator().instrument(app).expose(app)

@app.get("/healthz", tags=["Status"])
def healthz():
    return {"status": "ok", "version": "1.0"}

@app.get("/recommend/{user_id}", tags=["Recommendations"])
def recommend(user_id: int, k: int = 20, model: str = "popularity"):
    if model not in MODELS:
        raise HTTPException(status_code=400, detail=f"Model '{model}' not found.")

    k = max(0, int(k))

    if model == "popularity":
        src = popularity_model if isinstance(popularity_model, list) and popularity_model else DEFAULT_TOP
        recs = src[:k]
        return {"user_id": user_id, "movie_ids": recs, "model": "popularity", "k": k}

    if model == "item_cf":
        src = popularity_model if isinstance(popularity_model, list) and popularity_model else DEFAULT_TOP
        recs = src[:k]
        return {"user_id": user_id, "movie_ids": recs, "model": "item_cf (fallback to popularity)", "k": k}

    raise HTTPException(status_code=400, detail=f"Model '{model}' not found.")
