# import pandas as pd
import numpy as np
# from collections import defaultdict
import joblib  

from recommender.train import train_popularity_model, train_item_item_cf_model, load_movielens_data

def evaluate_models():

    print("--- Starting Offline Evaluation ---")
    
    ratings_df = load_movielens_data()
    ratings_df = ratings_df.sort_values(by='timestamp')
    split_point = int(len(ratings_df) * 0.8)
    train_data = ratings_df.iloc[:split_point]
    test_data = ratings_df.iloc[split_point:]

    print(f"Training data size: {len(train_data)}")
    print(f"Test data size: {len(test_data)}")

    train_popularity_model(train_data)
    train_item_item_cf_model(train_data)

    test_user_movies = test_data.groupby('user_id')['movie_id'].apply(list).to_dict()
    train_user_movies = train_data.groupby('user_id')['movie_id'].apply(list).to_dict()

    pop_model_recs = joblib.load("model_registry/popularity/v1.0/model.joblib")
    pop_hits = 0
    total_users = 0
    for user_id, actual_movies in test_user_movies.items():
        recommendations = pop_model_recs[:10]
        if any(movie in actual_movies for movie in recommendations):
            pop_hits += 1
        total_users += 1
    
    pop_hr10 = pop_hits / total_users if total_users > 0 else 0
    print(f"\nPopularity Model HR@10: {pop_hr10:.4f}")

    cf_model_artifact = joblib.load("model_registry/item_cf/v1.0/model.joblib")
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