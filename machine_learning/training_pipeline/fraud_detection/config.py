import os

# Environment variables with fallbacks
DATA_PATH = os.getenv("DATA_PATH", "/teamspace/studios/this_studio/data_generation/final_project/synthetic_fraud_data_v3.csv")
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "/teamspace/studios/this_studio/final_project/fraud_detection/training_pipeline/artifacts")

print(DATA_PATH)

# Configuration parameters from environment variables with defaults
TEST_SIZE_RATIO = float(os.getenv("TEST_SIZE_RATIO", "0.2"))

TARGET = 'is_flagged_fraud'

COLS_TO_DROP = [
    'timestamp', 'user_id', 'card_id', 'merchant_id',
    'ip_address', 'device_id', 'transaction_id', 'fraud_scenario'
]

# For Optuna Hyperparameter Optimization
STUDY_NAME = "xgboost-fraud-detection"
N_TRIALS = int(os.getenv("N_TRIALS", "3")) # Number of HPO trials to run
N_SPLITS_CV = 3 # Number of folds for cross-validation in HPO

# Artifacts
EXPORT_TYPE = "joblib"
MODEL_OUTPUT_PATH_ONNX = f"{ARTIFACTS_DIR}/model.onnx"
MODEL_OUTPUT_PATH_JOBLIB = f"{ARTIFACTS_DIR}/model.joblib"
CONFUSION_MATRIX_PATH = f"{ARTIFACTS_DIR}/confusion_matrix.png"

# Weights & Biases Configuration
WANDB_PROJECT = "fraud-detection"
WANDB_MODEL_NAME = "fraud-detection-model"