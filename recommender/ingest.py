import pandas as pd
from config import settings 

def load_ratings_from_s3() -> pd.DataFrame:
    print(f"Loading ratings from {settings.RATINGS_SNAPSHOT_PATH}")
    ratings_df = pd.read_parquet(settings.RATINGS_SNAPSHOT_PATH)
    return ratings_df