# fraud_api/config.py

import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Defines the application's configuration settings.
    
    Pydantic's BaseSettings will automatically attempt to override the default
    values here with any matching environment variables. For example, setting
    an environment variable `LOG_LEVEL=DEBUG` will override the default "INFO".
    """

    API_TITLE: str = "Fraud Detection API"
    API_DESCRIPTION: str = "An API to predict fraudulent transactions in real-time using a machine learning model."
    API_VERSION: str = "1.0.0"

    
    MODEL_PATH: Path = Path("pipeline_model.joblib")

    LOG_LEVEL: str = "INFO"


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding='utf-8',
        case_sensitive=False
    )


settings = Settings()