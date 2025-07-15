import os
from fastapi import FastAPI, Request, HTTPException
from contextlib import asynccontextmanager
from typing import List

from schemas import PredictionInput, BatchPredictionOutput
from predictor import load_model, make_predictions
from config import settings

MODELS = {}

AIP_HEALTH_ROUTE = os.environ.get('AIP_HEALTH_ROUTE', '/health')
AIP_PREDICT_ROUTE = os.environ.get('AIP_PREDICT_ROUTE', '/predict')
model_path = os.environ.get("MODEL_PATH")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application startup...")
    MODELS["xgboost"] = load_model(model_path=model_path)
    print(f"Model {model_path} loaded successfully.")
    MODELS["loaded"] = True
    yield
    # This code runs on shutdown
    print("Application shutdown...")
    
    MODELS.clear()

app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    lifespan=lifespan
)

@app.get(AIP_HEALTH_ROUTE, tags=["Monitoring"])
async def health_check():
    """
    Health check endpoint to verify service and model status.
    """
    model_loaded = MODELS['loaded']
    status = "healthy" if model_loaded else "unhealthy"
    return dict(status=status)

@app.post(AIP_PREDICT_ROUTE, response_model=BatchPredictionOutput, tags=["Prediction"])
async def predict_fraud(payload: PredictionInput):
    """
    Receives a single transaction or a batch of transactions and returns fraud predictions.
    """
    if not MODELS["loaded"]:
        raise HTTPException(status_code=503, detail="Model is not available. Please check service health.")
    try:
        predictions = make_predictions(model_pipeline=MODELS["xgboost"], input_data=payload)
        return BatchPredictionOutput(predictions=predictions)
    except Exception as e:
        print(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail="An error occurred during prediction.")