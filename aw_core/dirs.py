import os
import sys
from functools import wraps
from typing import Callable, Optional

import platformdirs

GetDirFunc = Callable[[Optional[str]], str]

_DEFAULT_APPNAME = "activitywatch"
_TESTING_PROFILE = "testing"
_TESTING_APPNAME = f"{_DEFAULT_APPNAME}-{_TESTING_PROFILE}"

# Filenames that mark a machine as still using the pre-profile shared-root
# testing layout (ActivityWatch/activitywatch#1399). Keep this list specific:
# a false positive would pin a fresh install to the legacy layout forever.
_LEGACY_TESTING_FILENAME_MARKERS = (
    "peewee-sqlite-testing",
    "sqlite-testing",
    "settings-testing",
    "config-testing",
    "-testing.db",
    "-testing.toml",
    "-testing.json",
    "_testing_",
)


def get_profile() -> str:
    """Return the active ``AW_PROFILE`` value, or ``""`` for the default profile."""
    return os.environ.get("AW_PROFILE", "") or ""


def _new_testing_root_exists() -> bool:
    """True if any platform dir for ``activitywatch-testing`` already exists.

    Must not create directories: existence is the signal that a previous run
    already adopted the isolated testing root.
    """
    for getter in (
        platformdirs.user_data_dir,
        platformdirs.user_config_dir,
        platformdirs.user_cache_dir,
    ):
        if os.path.isdir(getter(_TESTING_APPNAME)):
            return True
    return False


def _is_legacy_testing_filename(name: str) -> bool:
    lower = name.lower()
    return any(marker in lower for marker in _LEGACY_TESTING_FILENAME_MARKERS)


def _legacy_testing_artifacts_exist() -> bool:
    """True if testing data still lives under the shared ``activitywatch`` root."""
    roots = (
        platformdirs.user_data_dir(_DEFAULT_APPNAME),
        platformdirs.user_config_dir(_DEFAULT_APPNAME),
        platformdirs.user_cache_dir(_DEFAULT_APPNAME),
    )
    for root in roots:
        if not os.path.isdir(root):
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            for filename in filenames:
                if _is_legacy_testing_filename(filename):
                    return True
            # Descend one level (activitywatch/aw-server/...) not further.
            if os.path.relpath(dirpath, root) != ".":
                dirnames.clear()
    return False


def using_legacy_testing_root() -> bool:
    """Whether ``AW_PROFILE=testing`` should stay on the shared ``activitywatch`` root.

    Resolution rule (ActivityWatch/activitywatch#1399), identical on python and rust:

    1. If ``activitywatch-testing/`` already exists: use it (new layout).
    2. Else if legacy testing artifacts exist in the bare ``activitywatch/``
       root: stay in legacy mode (old paths, old filenames).
    3. Else (fresh setup): create and use ``activitywatch-testing/``.
    """
    if get_profile() != _TESTING_PROFILE:
        return False
    if _new_testing_root_exists():
        return False
    return _legacy_testing_artifacts_exist()


def legacy_testing_suffix(testing: bool) -> str:
    """Return ``"-testing"`` only when testing data shares the default root.

    Isolated profile roots (including new-style ``activitywatch-testing/``) use
    bare filenames: the directory already isolates. Suffixed names remain only
    in legacy mode so existing ``peewee-sqlite-testing.v2.db`` files keep working.
    """
    if not testing:
        return ""
    profile = get_profile()
    if profile == _TESTING_PROFILE and not using_legacy_testing_root():
        return ""
    if profile and profile != _TESTING_PROFILE:
        # Named isolated profile — directory already isolates.
        return ""
    return "-testing"


def _get_appname() -> str:
    """Return the platformdirs appname, optionally suffixed by the active profile.

    If the ``AW_PROFILE`` environment variable is set to a non-empty string the
    appname becomes ``activitywatch-<profile>`` so that *all* platform
    directories (data, config, cache, log) are completely separate from the
    default profile.  An unset or empty ``AW_PROFILE`` returns the bare
    ``"activitywatch"`` name, which is identical to the pre-profile behaviour.

    The ``testing`` profile is special: see :func:`using_legacy_testing_root`.
    Existing testing data in the shared root is not orphaned; fresh setups and
    machines that already have ``activitywatch-testing/`` use the isolated root.

    This is the single authoritative place where profile isolation is applied.
    Every module that uses :func:`get_data_dir`, :func:`get_config_dir`,
    :func:`get_cache_dir` or :func:`get_log_dir` automatically inherits the
    correct root for the running profile; no ``profile=`` parameter needs to be
    threaded through the call chain.
    """
    profile = get_profile()
    if not profile:
        return _DEFAULT_APPNAME
    if profile == _TESTING_PROFILE and using_legacy_testing_root():
        return _DEFAULT_APPNAME
    return f"{_DEFAULT_APPNAME}-{profile}"


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
