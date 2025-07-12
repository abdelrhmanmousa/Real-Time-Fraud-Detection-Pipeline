import os
import gcsfs

# Initialize GCS filesystem globally or on-demand
# For on-demand, instantiate GCSFileSystem() inside functions where needed.
# For global, ensure it's handled correctly if project/credentials might change per call.
# Starting with on-demand instantiation within open_file for simplicity.

def is_gcs_path(path: str) -> bool:
    """Checks if a path string starts with gs://."""
    return path.startswith("gs://")

def ensure_path_exists(path: str):
    """
    Ensures that the directory for the given path exists if it's a local path.
    If it's a GCS path, does nothing as GCS directories are implicit.
    Ensures that the given directory path exists if it's a local path.
    If it's a GCS path, does nothing as GCS directories are implicit.
    """
    if not is_gcs_path(path):
        if path: # Ensure path is not empty
            os.makedirs(path, exist_ok=True)
            print(f"Ensured local directory exists: {path}")
        else:
            print(f"Path is empty. No directory created by ensure_path_exists.")
    else:
        print(f"Path '{path}' is a GCS path. Directory creation is implicit on GCS.")

def open_file(path: str, mode: str, gcs_project: str = None):
    """
    Opens a file, supporting both local and GCS paths.
    For GCS, uses gcsfs.
    'gcs_project' can be specified if the GCS path requires a specific project context
    that isn't covered by default credentials.
    """
    if is_gcs_path(path):
        # TODO: Consider how to handle gcs_project if needed.
        # For now, assuming default project from environment is sufficient.
        fs = gcsfs.GCSFileSystem()
        return fs.open(path, mode)
    else:
        # Ensure parent directory exists for local file writing
        if 'w' in mode or 'a' in mode: # Writing or appending
            parent_dir = os.path.dirname(path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
        return open(path, mode)

# Example of how ensure_path_exists might be used:
# ensure_path_exists("/tmp/local_dir/some_file.txt") # Ensures /tmp/local_dir exists
# ensure_path_exists("/tmp/another_local_dir/") # Ensures /tmp/another_local_dir exists
# ensure_path_exists("gs://my-bucket/my_folder/data.csv") # Does nothing, prints message
# ensure_path_exists("local_file_in_current_dir.txt") # Ensures current dir ('.') conceptually, or does nothing if path is just filename.
# ensure_path_exists(".") # Ensures current dir.
# ensure_path_exists("new_dir_in_current") # Creates ./new_dir_in_current
