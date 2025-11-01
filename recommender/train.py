import os
import joblib
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from config import settings
from recommender import transform

def train_and_serialize_all(ratings_df: pd.DataFrame, user_item_matrix: pd.DataFrame):
    _train_popularity(ratings_df)
    _train_item_cf(user_item_matrix)

def _train_popularity(ratings_df: pd.DataFrame):
    print("Training Popularity model...")
    popularity_counts = ratings_df.groupby('movie_id').size().sort_values(ascending=False)
    model_artifact = popularity_counts.index.tolist()
    
    # Serialize
    path = os.path.join(settings.MODEL_REGISTRY_PATH, settings.POPULARITY_MODEL_DIR)
    os.makedirs(path, exist_ok=True)
    joblib.dump(model_artifact, os.path.join(path, "model.joblib"))
    print(f"Popularity model saved to {path}")

def _train_item_cf(user_item_matrix: pd.DataFrame):
    print("Training Item-Item CF model...")
    user_item_sparse = transform.to_sparse_matrix(user_item_matrix)
    item_similarity_matrix = cosine_similarity(user_item_sparse.T)
    
    model_artifact = {
        'similarity_matrix': item_similarity_matrix,
        'movie_ids': user_item_matrix.columns.tolist()
    }
    
    path = os.path.join(settings.MODEL_REGISTRY_PATH, settings.ITEM_CF_MODEL_DIR)
    os.makedirs(path, exist_ok=True)
    joblib.dump(model_artifact, os.path.join(path, "model.joblib"))
    print(f"Item-CF model saved to {path}")