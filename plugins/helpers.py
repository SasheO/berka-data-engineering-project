import os

def list_all_files_within_path(path: str, with_path_prefix: str = ''):
    if not os.path.exists(path):
        raise FileNotFoundError(f"The directory {path} does not exist.")
    if with_path_prefix:
        return [with_path_prefix + "/" + f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    else:
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
