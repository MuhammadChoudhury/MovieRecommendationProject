# recommender/train.py

import os
import time
import pandas as pd
import yaml
import joblib
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
import urllib.request
import zipfile
import io

# --- Configuration ---
# CHANGED URL to point to the ZIP file
DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
MODEL_REGISTRY_PATH = "model_registry"

# --- NEW FUNCTION to handle downloading and extracting the dataset ---
def load_movielens_data():
    """
    Downloads the MovieLens 1M dataset ZIP file, extracts 'ratings.dat'
    in memory, and loads it into a pandas DataFrame.
    """
    print(f"Downloading and extracting MovieLens 1M dataset from {DATASET_URL}...")
    
    # Download the file from the URL
    with urllib.request.urlopen(DATASET_URL) as response:
        # Read the content into a memory buffer
        zip_content = response.read()

    # Use ZipFile to read the archive from the memory buffer
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        # Find and open the ratings.dat file inside the zip
        with z.open('ml-1m/ratings.dat') as f:
            ratings_df = pd.read_csv(
                f,
                sep='::',
                engine='python',
                header=None,
                names=['user_id', 'movie_id', 'rating', 'timestamp']
            )
    print("Dataset loaded successfully.")
    return ratings_df

# --- Model 1: Popularity Model (No changes needed) ---
def train_popularity_model(data):
    """
    Trains a simple popularity model that ranks movies by the number of ratings.
    """
    print("Training Popularity model...")
    start_time = time.time()
    
    popularity_counts = data.groupby('movie_id').size().sort_values(ascending=False)
    model_artifact = popularity_counts.index.tolist()
    
    training_time = time.time() - start_time
    
    version_path = os.path.join(MODEL_REGISTRY_PATH, "popularity", "v1.0")
    os.makedirs(version_path, exist_ok=True)
    
    model_path = os.path.join(version_path, "model.joblib")
    joblib.dump(model_artifact, model_path)
    
    meta = {
        'model_name': 'popularity',
        'version': '1.0',
        'training_time_sec': round(training_time, 4),
        'model_size_kb': round(os.path.getsize(model_path) / 1024, 2),
    }
    with open(os.path.join(version_path, "meta.yaml"), 'w') as f:
        yaml.dump(meta, f)
        
    print(f"Popularity model saved. Metadata: {meta}")
    return meta

# --- Model 2: Item-Item Collaborative Filtering (No changes needed) ---
def train_item_item_cf_model(data):
    """
    Trains an Item-Item Collaborative Filtering model.
    """
    print("\nTraining Item-Item CF model...")
    start_time = time.time()
    
    user_item_matrix = data.pivot_table(index='user_id', columns='movie_id', values='rating').fillna(0)
    user_item_sparse = csr_matrix(user_item_matrix.values)
    item_similarity_matrix = cosine_similarity(user_item_sparse.T)
    
    model_artifact = {
        'similarity_matrix': item_similarity_matrix,
        'movie_ids': user_item_matrix.columns.tolist()
    }
    
    training_time = time.time() - start_time
    
    version_path = os.path.join(MODEL_REGISTRY_PATH, "item_cf", "v1.0")
    os.makedirs(version_path, exist_ok=True)
    
    model_path = os.path.join(version_path, "model.joblib")
    joblib.dump(model_artifact, model_path)
    
    meta = {
        'model_name': 'item_cf',
        'version': '1.0',
        'training_time_sec': round(training_time, 4),
        'model_size_kb': round(os.path.getsize(model_path) / 1024, 2),
    }
    with open(os.path.join(version_path, "meta.yaml"), 'w') as f:
        yaml.dump(meta, f)

    print(f"Item-Item CF model saved. Metadata: {meta}")
    return meta


if __name__ == "__main__":
    # UPDATED to call the new function
    ratings_df = load_movielens_data()
    
    # Train both models
    train_popularity_model(ratings_df)
    train_item_item_cf_model(ratings_df)
    
    print("\n✅ Training complete for both models.")