import os

def get_env_str(key: str, default: str = "") -> str:
    return os.environ.get(key, default)

def get_env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))