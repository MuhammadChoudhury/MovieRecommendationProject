from recommender import ingest, transform, train, evaluate_offline

def main():
    print("--- Starting Batch Pipeline ---")

    print("\n[1/4] Ingesting data...")
    ratings_df = ingest.load_ratings_from_s3()

    print("\n[2/4] Transforming data...")
    user_item_matrix = transform.create_user_item_matrix(ratings_df)

    print("\n[3/4] Training models...")
    train.train_and_serialize_all(ratings_df, user_item_matrix)
    
    print("\n[4/4] Evaluating models...")
    evaluate_offline.evaluate_models()

    print("\n--- Batch Pipeline Complete ---")

if __name__ == "__main__":
    main()