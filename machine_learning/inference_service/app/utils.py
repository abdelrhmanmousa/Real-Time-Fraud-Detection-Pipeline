import os
import joblib
from google.cloud import storage
from urllib.parse import urlparse

def load_model(model_path: str):
    """
    Loads a joblib model from either a local file path or a GCS URI.

    Args:
        model_path (str): The local path or GCS URI (e.g., 'gs://bucket-name/model.joblib')
                          of the model to load.

    Returns:
        The loaded model object.
    """
    print(f"Attempting to load model from: {model_path}")

    try:
        
        if model_path.startswith('gs://'):
            print("GCS path detected. Initializing GCS client...")
            
            # Parse the GCS URI to get bucket and blob name
            parsed_url = urlparse(model_path)
            bucket_name = parsed_url.netloc
            blob_name = parsed_url.path.lstrip('/')

            # Initialize the GCS client
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            # Create a temporary local file to download the model to
            # This is a robust way to handle loading, especially for large models
            _, temp_local_filename = os.path.split(blob_name)
            temp_local_path = os.path.join("/tmp", temp_local_filename)

            print(f"Downloading model from gs://{bucket_name}/{blob_name} to {temp_local_path}...")
            blob.download_to_filename(temp_local_path)
            print("Download complete.")

            # Load the model from the temporary local file
            model = joblib.load(temp_local_path)
            print("Model loaded successfully from GCS.")

            # Clean up the temporary file
            os.remove(temp_local_path)
            print(f"Temporary file {temp_local_path} removed.")

        else:
            # If it's not a GCS path, assume it's a local path
            print("Local path detected.")
            if not os.path.exists(model_path):
                raise FileNotFoundError(f"Local model file not found at: {model_path}")
            
            model = joblib.load(model_path)
            print("Model loaded successfully from local path.")

        return model

    except Exception as e:
        print(f"An error occurred while loading the model: {e}")
        return None
