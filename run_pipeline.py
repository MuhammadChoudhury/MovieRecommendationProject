import os
from recommender import ingest, transform, train, evaluate_offline
from recommender.schemas import RatingsSchema 
from config import settings
import s3fs

def main():
    print("--- Starting Batch Pipeline ---")
    
    s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    # 1. Ingest: Load data from the data lake
    print("\n[1/5] Ingesting data...")
    ratings_df = ingest.load_ratings_from_s3()

    # 2. Validate
    print("\n[2/5] Validating data schema...")
    RatingsSchema.validate(ratings_df)
    print("Schema validation passed.")

    # 3. Transform: Prepare data for modeling
    print("\n[3/5] Transforming data...")
    user_item_matrix = transform.create_user_item_matrix(ratings_df)

    # 4. Train & Serialize: Train models and save them
    print("\n[4/5] Training models...")
    train.train_and_serialize_all(ratings_df, user_item_matrix)
    
    # 5. Evaluate: Run offline evaluation
    print("\n[5/5] Evaluating models...")
    evaluate_offline.evaluate_models(ratings_df) # Pass the data in

    print("\n--- Batch Pipeline Complete ---")

if __name__ == "__main__":
    main()