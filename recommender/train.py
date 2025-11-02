import os
import joblib
import pandas as pd
from config import settings

def _train_popularity(ratings_df: pd.DataFrame):
    popularity_counts = ratings_df.groupby("movie_id").size().sort_values(ascending=False)
    model_artifact = popularity_counts.index.tolist()
    path = os.path.join(settings.MODEL_REGISTRY_PATH, settings.POPULARITY_MODEL_DIR)
    os.makedirs(path, exist_ok=True)
    joblib.dump(model_artifact, os.path.join(path, "model.joblib"))
    print(f"[train] Popularity model saved to {path}")

def _train_item_cf_stub(movie_ids):
    model_artifact = {"similarity_matrix": None, "movie_ids": list(movie_ids)}
    path = os.path.join(settings.MODEL_REGISTRY_PATH, settings.ITEM_CF_MODEL_DIR)
    os.makedirs(path, exist_ok=True)
    joblib.dump(model_artifact, os.path.join(path, "model.joblib"))
    print(f"[train] Item-CF (stub) saved to {path}")

def main():
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 2, 3, 4, 5, 5, 6, 7, 8],
            "movie_id": [10, 11, 10, 12, 10, 11, 13, 12, 14, 10],
            "timestamp": range(10),
        }
    )
    _train_popularity(df)
    _train_item_cf_stub(sorted(df["movie_id"].unique()))

if __name__ == "__main__":
    main()
