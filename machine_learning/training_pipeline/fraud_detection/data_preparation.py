from pathlib import Path
import pandas as pd
import numpy as np
from config import DATA_PATH, TARGET, COLS_TO_DROP, TEST_SIZE_RATIO
from utils import is_gcs_path # Import GCS utility
import gcsfs # Import gcsfs

def read_data():
    """Reads data from single CSV file or multiple CSV files in a directory,
    supporting both local and GCS paths."""
    if is_gcs_path(DATA_PATH):
        fs = gcsfs.GCSFileSystem()
        # Check if DATA_PATH is a direct file path or a directory
        if DATA_PATH.endswith(".csv") and fs.isfile(DATA_PATH): # Heuristic for single file
            print(f"Reading single GCS CSV file: {DATA_PATH}")
            return pd.read_csv(DATA_PATH)
        else:
            # Assume DATA_PATH is a directory; list all *.csv files
            # Ensure DATA_PATH for glob ends with a slash if it's a directory
            gcs_dir_path = DATA_PATH if DATA_PATH.endswith('/') else DATA_PATH + '/'
            print(f"Reading GCS CSV files from directory: {gcs_dir_path}")
            
            # fs.glob might return full paths including bucket, ensure they are gs:// prefixed for pandas
            file_paths = ["gs://" + path for path in fs.glob(gcs_dir_path + "*.csv")]
            if not file_paths:
                raise FileNotFoundError(f"No CSV files found in GCS directory: {gcs_dir_path}")

            df_list = []
            for file_path in file_paths:
                print(f"Reading GCS file: {file_path}")
                with fs.open(file_path, 'r') as f: # Open with gcsfs to pass file object to pandas
                    df_list.append(pd.read_csv(f))
            if not df_list: # Should be caught by file_paths check, but as a safeguard
                 raise FileNotFoundError(f"No dataframes created from GCS CSV files in: {gcs_dir_path}")
            return pd.concat(df_list, ignore_index=True)
    else:
        # Local path logic using pathlib
        data_path_obj = Path(DATA_PATH)
        if data_path_obj.is_file():
            print(f"Reading single local CSV file: {data_path_obj}")
            return pd.read_csv(data_path_obj)
        elif data_path_obj.is_dir():
            print(f"Reading local CSV files from directory: {data_path_obj}")
            df_list = []
            for file in data_path_obj.glob("*.csv"):
                print(f"Reading local file: {file}")
                df_list.append(pd.read_csv(file))
            if not df_list:
                 raise FileNotFoundError(f"No CSV files found in local directory: {data_path_obj}")
            return pd.concat(df_list, ignore_index=True)
        else:
            raise FileNotFoundError(f"Local path is not a file or directory: {DATA_PATH}")


def get_numerical_categorical_cols(data):
    # gets the numerical and categorical columns to be used in the preprocessor
    numerical_features = data.select_dtypes(include=np.number).columns.tolist()
    categorical_features = data.select_dtypes(exclude=np.number).columns.tolist()
    return {"num_cols":numerical_features, "cat_cols":categorical_features}

# def add_hour_of_day_column(df):
#     df['timestamp'] = pd.to_datetime(df['timestamp'])
#     df['hour_of_day'] = df['timestamp'].dt.hour
#     return df

def split_dataset_based_on_time(df):
    df_sorted = df.sort_values('timestamp').reset_index(drop=True)
    
    features_df = df_sorted.drop(columns=[TARGET] + COLS_TO_DROP)
    target = df_sorted[TARGET]

    split_index = int(len(df_sorted) * (1 - TEST_SIZE_RATIO))
    X_train = features_df.iloc[:split_index]
    X_test = features_df.iloc[split_index:]
    y_train = target.iloc[:split_index]
    y_test = target.iloc[split_index:]

    return X_train, X_test, y_train, y_test

def prepare_data():
    """
    Loads, cleans, engineers features, and splits the data.
    Returns:
        X_train, X_test, y_train, y_test, numerical_features, categorical_features
    """
    print("Preparing data...")
    
    df = read_data()
    
    # Adds the hour of day feature from the timestamp
    # df = add_hour_of_day_column(df)
    
    # Convertin boolean features to integers
    for col in df.select_dtypes(include='bool').columns:
        df[col] = df[col].astype(int)

    # Time-based Split
    X_train, X_test, y_train, y_test = split_dataset_based_on_time(df)

    features_names_dict = get_numerical_categorical_cols(X_train)

    print("Data preparation complete.")
    print(f"Training set shape: {X_train.shape}, Test set shape: {X_test.shape}")
    
    return X_train, X_test, y_train, y_test, features_names_dict