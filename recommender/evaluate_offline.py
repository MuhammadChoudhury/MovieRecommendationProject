import pandas as pd
import numpy as np
import joblib
from collections import defaultdict

from config import settings
from recommender import ingest, transform, train  
from recommender.schemas import RatingsSchema 

def evaluate_models():
    print("--- Starting Offline Evaluation ---")
    
    ratings_df = ingest.load_ratings_from_s3() 
    
    RatingsSchema.validate(ratings_df)
    
    ratings_df = ratings_df.sort_values(by='timestamp')
    split_point = int(len(ratings_df) * 0.8)
    train_data = ratings_df.iloc[:split_point]
    test_data = ratings_df.iloc[split_point:]

    print(f"Training data size: {len(train_data)}")
    print(f"Test data size: {len(test_data)}")

    train_user_item_matrix = transform.create_user_item_matrix(train_data)
    train.train_and_serialize_all(train_data, train_user_item_matrix)
    print("Models trained on training split successfully.")

    test_user_movies = test_data.groupby('user_id')['movie_id'].apply(list).to_dict()
    train_user_movies = train_data.groupby('user_id')['movie_id'].apply(list).to_dict()

    pop_path = f"s3://{settings.S3_BUCKET_NAME}/model-registry/popularity/{train.version_id}/model.joblib"
    print(f"Loading test model from {pop_path}")
    pop_model_recs = joblib.load(train.s3.open(pop_path, 'rb'))
    pop_hits = 0
    total_users = 0
    for user_id, actual_movies in test_user_movies.items():
        recommendations = pop_model_recs[:10]
        if any(movie in actual_movies for movie in recommendations):
            pop_hits += 1
        total_users += 1
    
    pop_hr10 = pop_hits / total_users if total_users > 0 else 0
    print(f"\nPopularity Model HR@10: {pop_hr10:.4f}")

    cf_path = f"s3://{settings.S3_BUCKET_NAME}/model-registry/item_cf/{train.version_id}/model.joblib"
    print(f"Loading test model from {cf_path}")
    cf_model_artifact = joblib.load(train.s3.open(cf_path, 'rb'))
    similarity_matrix = cf_model_artifact['similarity_matrix']
    movie_ids = cf_model_artifact['movie_ids']
    movie_id_to_idx = {movie_id: i for i, movie_id in enumerate(movie_ids)}

    cf_hits = 0
    total_users_cf = 0
    for user_id, actual_movies in test_user_movies.items():
        if user_id not in train_user_movies:
            continue
        
        user_train_movies = train_user_movies[user_id][-5:]
        user_train_indices = [movie_id_to_idx[m] for m in user_train_movies if m in movie_id_to_idx]

        if not user_train_indices:
            continue

        similar_scores = similarity_matrix[user_train_indices].mean(axis=0)
        top_indices = np.argsort(similar_scores)[-15:][::-1]
        
        recommendations = []
        for idx in top_indices:
            if movie_ids[idx] not in train_user_movies.get(user_id, []):
                recommendations.append(movie_ids[idx])
            if len(recommendations) >= 10:
                break
        
        if any(movie in actual_movies for movie in recommendations):
            cf_hits += 1
        total_users_cf += 1
        
    cf_hr10 = cf_hits / total_users_cf if total_users_cf > 0 else 0
    print(f"Item-Item CF Model HR@10: {cf_hr10:.4f}")

    print("\n--- Offline Evaluation Complete ---")
    return {"popularity_hr10": pop_hr10, "item_cf_hr10": cf_hr10}

if __name__ == "__main__":
    evaluate_models()