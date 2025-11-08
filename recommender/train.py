# recommender/train.py
# import os
import time
import joblib
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from config import settings  
from recommender import transform
import s3fs 

s3 = s3fs.S3FileSystem()



def train_and_serialize_all(ratings_df: pd.DataFrame, user_item_matrix: pd.DataFrame) -> str:

    version_id = f"v_{int(time.time())}"
    print(f"--- Training and serializing new model version: {version_id} ---")
    
    _train_popularity(ratings_df, version_id)
    _train_item_cf(user_item_matrix, version_id)
    
    latest_path = f"s3://{settings.S3_BUCKET_NAME}/model-registry/latest.txt"
    print(f"Updating latest version pointer to {version_id} at {latest_path}")
    with s3.open(latest_path, 'w') as f:
        f.write(version_id)
        
    return version_id 
    
def _train_popularity(ratings_df: pd.DataFrame, version: str):
    """Trains and serializes the popularity model to S3."""
    print("Training Popularity model...")
    popularity_counts = ratings_df.groupby('movie_id').size().sort_values(ascending=False)
    model_artifact = popularity_counts.index.tolist()
    
    # Serialize to S3
    path = f"{settings.S3_BUCKET_NAME}/model-registry/popularity/{version}/model.joblib"
    print(f"Saving Popularity model to {path}")
    with s3.open(path, 'wb') as f:
        joblib.dump(model_artifact, f)

def _train_item_cf(user_item_matrix: pd.DataFrame, version: str):
    """Trains and serializes the Item-Item CF model to S3."""
    print("Training Item-Item CF model...")
    user_item_sparse = transform.to_sparse_matrix(user_item_matrix)
    item_similarity_matrix = cosine_similarity(user_item_sparse.T)
    model_artifact = {
        'similarity_matrix': item_similarity_matrix,
        'movie_ids': user_item_matrix.columns.tolist()
    }
    
    # Serialize to S3
    path = f"{settings.S3_BUCKET_NAME}/model-registry/item_cf/{version}/model.joblib"
    print(f"Saving Item-CF model to {path}")
    with s3.open(path, 'wb') as f:
        joblib.dump(model_artifact, f)