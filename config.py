from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    S3_BUCKET_NAME: str = "byteflix-datalake"
    
    MODEL_REGISTRY_PATH: str = "model_registry"
    POPULARITY_MODEL_DIR: str = "popularity/v1.0"
    ITEM_CF_MODEL_DIR: str = "item_cf/v1.0"

    RATINGS_SNAPSHOT_PATH: str = f"s3://{S3_BUCKET_NAME}/snapshots/byteflix.rate/"

settings = Settings()