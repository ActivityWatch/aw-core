import os
from functools import wraps

import appdirs


def ensure_returned_path_exists(f):
    @wraps(f)
    def wrapper(*args, **kwargs) -> str:
        path = f(*args, **kwargs)
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    return wrapper


@ensure_returned_path_exists
def get_data_dir(module_name: str) -> str:
    user_data_dir = appdirs.user_data_dir("activitywatch")
    return os.path.join(user_data_dir, module_name)


@ensure_returned_path_exists
def get_cache_dir(module_name: str) -> str:
    user_cache_dir = appdirs.user_cache_dir("activitywatch")
    return os.path.join(user_cache_dir, module_name)


@ensure_returned_path_exists
def get_config_dir(module_name: str) -> str:
    user_config_dir = appdirs.user_config_dir("activitywatch")
    return os.path.join(user_config_dir, module_name)


@ensure_returned_path_exists
def get_log_dir(module_name: str) -> str:  # pragma: no cover
    user_log_dir = appdirs.user_log_dir("activitywatch")
    return os.path.join(user_log_dir, module_name)
