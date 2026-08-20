import os
import sys
from functools import wraps
from typing import Callable, Optional

import platformdirs

GetDirFunc = Callable[[Optional[str]], str]


def _get_appname() -> str:
    """Return the platformdirs appname, optionally suffixed by the active profile.

    If the ``AW_PROFILE`` environment variable is set to a non-empty string the
    appname becomes ``activitywatch-<profile>`` so that *all* platform
    directories (data, config, cache, log) are completely separate from the
    default profile.  An unset or empty ``AW_PROFILE`` returns the bare
    ``"activitywatch"`` name, which is identical to the pre-profile behaviour.

    This is the single authoritative place where profile isolation is applied.
    Every module that uses :func:`get_data_dir`, :func:`get_config_dir`,
    :func:`get_cache_dir` or :func:`get_log_dir` automatically inherits the
    correct root for the running profile; no ``profile=`` parameter needs to be
    threaded through the call chain.
    """
    profile = os.environ.get("AW_PROFILE", "")
    return f"activitywatch-{profile}" if profile else "activitywatch"


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
    data_dir = platformdirs.user_data_dir(_get_appname())
    return os.path.join(data_dir, module_name) if module_name else data_dir


@_ensure_returned_path_exists
def get_cache_dir(module_name: Optional[str] = None) -> str:
    cache_dir = platformdirs.user_cache_dir(_get_appname())
    return os.path.join(cache_dir, module_name) if module_name else cache_dir


@_ensure_returned_path_exists
def get_config_dir(module_name: Optional[str] = None) -> str:
    config_dir = platformdirs.user_config_dir(_get_appname())
    return os.path.join(config_dir, module_name) if module_name else config_dir


@_ensure_returned_path_exists
def get_log_dir(module_name: Optional[str] = None) -> str:  # pragma: no cover
    # on Linux/Unix, platformdirs changed to using XDG_STATE_HOME instead of XDG_DATA_HOME for log_dir in v2.6
    # we want to keep using XDG_DATA_HOME for backwards compatibility
    # https://github.com/ActivityWatch/aw-core/pull/122#issuecomment-1768020335
    if sys.platform.startswith("linux"):
        log_dir = platformdirs.user_cache_path(_get_appname()) / "log"
    else:
        log_dir = platformdirs.user_log_dir(_get_appname())
    return os.path.join(log_dir, module_name) if module_name else log_dir
