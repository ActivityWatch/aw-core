import os
from functools import wraps

import appdirs


def ensure_returned_path_exists(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        path = f(*args, **kwargs)
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    return wrapper


@ensure_returned_path_exists
def get_log_dir():
    return os.path.join(_get_global_data_dir(), "logs")


@ensure_returned_path_exists
def get_data_dir(module_name: str) -> str:
    return os.path.join(_get_global_data_dir(), module_name)


@ensure_returned_path_exists
def get_config_dir(module_name: str) -> str:
    return os.path.join(_get_global_config_dir(), module_name)


def _get_global_data_dir() -> str:
    return appdirs.user_data_dir("activitywatch")


def _get_global_config_dir() -> str:
    return appdirs.user_config_dir("activitywatch")
