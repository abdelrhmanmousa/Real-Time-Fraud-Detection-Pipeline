import pandas as pd
import joblib
from typing import Any, List

from schemas import PredictionInput

def load_model(model_path: str) -> Any:
    return joblib.load(model_path)


def make_predictions(input_data: PredictionInput, model_pipeline: Any) -> List[float]:
    """
    Prepares the input data and makes predictions using the loaded scikit-learn pipeline.

    The pipeline is expected to handle all necessary preprocessing, including:
    - Column selection
    - Scaling of numerical features
    - One-hot encoding of categorical features

    Args:
        input_data: The batch of transactions from the API, conforming to PredictionInput schema.
        model_pipeline: The loaded scikit-learn pipeline object.

    Returns:
        A list of fraud scores (floats).
    """
    
    # The pipeline's ColumnTransformer will select the columns it needs by name.
    input_dicts = [tx.model_dump() for tx in input_data.transactions]
    df = pd.DataFrame(input_dicts)

    try:
        predictions = model_pipeline.predict_proba(df)[:, 1]
    except Exception as e:
        raise RuntimeError(f"An error occurred during prediction: {e}")

    preds = predictions.tolist()
    outputs = [{"transaction_id": tx.transaction_id, "fraud_score": score} for tx, score in zip(input_data.transactions, preds)]
    return outputs
