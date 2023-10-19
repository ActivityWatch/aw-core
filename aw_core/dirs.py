import os
import sys
from functools import wraps
from typing import Callable, Optional

import platformdirs

GetDirFunc = Callable[[Optional[str]], str]


def ensure_path_exists(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def _ensure_returned_path_exists(f: GetDirFunc) -> GetDirFunc:
    @wraps(f)
    def wrapper(subpath: Optional[str] = None) -> str:
        path = f(subpath)
        ensure_path_exists(path)
        return path

    return wrapper


@_ensure_returned_path_exists
def get_data_dir(module_name: Optional[str] = None) -> str:
    data_dir = platformdirs.user_data_dir("activitywatch")
    return os.path.join(data_dir, module_name) if module_name else data_dir


@_ensure_returned_path_exists
def get_cache_dir(module_name: Optional[str] = None) -> str:
    cache_dir = platformdirs.user_cache_dir("activitywatch")
    return os.path.join(cache_dir, module_name) if module_name else cache_dir


@_ensure_returned_path_exists
def get_config_dir(module_name: Optional[str] = None) -> str:
    config_dir = platformdirs.user_config_dir("activitywatch")
    return os.path.join(config_dir, module_name) if module_name else config_dir


@_ensure_returned_path_exists
def get_log_dir(module_name: Optional[str] = None) -> str:  # pragma: no cover
    # on Linux/Unix, platformdirs changed to using XDG_STATE_HOME instead of XDG_DATA_HOME for log_dir in v2.6
    # we want to keep using XDG_DATA_HOME for backwards compatibility
    # https://github.com/ActivityWatch/aw-core/pull/122#issuecomment-1768020335
    if sys.platform.startswith("linux"):
        log_dir = platformdirs.user_cache_path("activitywatch") / "log"
    else:
        log_dir = platformdirs.user_log_dir("activitywatch")
    return os.path.join(log_dir, module_name) if module_name else log_dir
