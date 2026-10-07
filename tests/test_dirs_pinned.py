"""Pin the default on-disk locations of the Python modules, per platform.

The expected paths are derived from environment variables (LOCALAPPDATA,
HOME, XDG_*), not from platformdirs, so a dependency swap or bump that moves
them fails here instead of silently orphaning users' data. That happened on
the Rust side in v0.14.0, when ``appdirs`` was swapped for ``dirs`` and
Windows data moved from ``AppData\\Local`` to ``AppData\\Roaming``
(ActivityWatch/aw-server-rust#562, fixed in #791).

Keep in sync with:

- docs: https://docs.activitywatch.net/en/latest/directories.html
- aw-server-rust ``aw-datastore/src/legacy_import.rs``
  ``test_legacy_dbfile_path_is_pinned``: where aw-server-rust looks for the
  Python database pinned in ``test_peewee_db_path_is_pinned`` below, to
  import it. If one changes without the other, the Python→Rust migration
  silently imports nothing.
- aw-server-rust ``aw-server/src/dirs.rs`` ``test_default_paths_are_pinned``
  and aw-tauri ``src-tauri/src/dirs.rs`` ``test_default_paths_are_pinned``:
  the Rust modules' paths. On Windows these deliberately differ from the
  Python ones by one ``activitywatch`` level (platformdirs uses the appname
  as appauthor; the Rust code never did).

Do not update these expectations without a migration for existing installs.
"""

import os
import sys
from pathlib import Path

import pytest

from aw_core.dirs import get_cache_dir, get_config_dir, get_data_dir, get_log_dir

from . import context  # noqa: F401


def _xdg(var: str, fallback: str) -> Path:
    value = os.environ.get(var, "")
    if value and os.path.isabs(value):
        return Path(value)
    return Path.home() / fallback


def _expected_roots() -> dict:
    """Parents of ``<module>`` for the default profile."""
    if sys.platform == "win32":
        app = Path(os.environ["LOCALAPPDATA"]) / "activitywatch" / "activitywatch"
        return {
            "data": app,
            "config": app,
            "cache": app / "Cache",
            "log": app / "Logs",
        }
    if sys.platform == "darwin":
        lib = Path.home() / "Library"
        return {
            "data": lib / "Application Support" / "activitywatch",
            "config": lib / "Application Support" / "activitywatch",
            "cache": lib / "Caches" / "activitywatch",
            "log": lib / "Logs" / "activitywatch",
        }
    return {
        "data": _xdg("XDG_DATA_HOME", ".local/share") / "activitywatch",
        "config": _xdg("XDG_CONFIG_HOME", ".config") / "activitywatch",
        "cache": _xdg("XDG_CACHE_HOME", ".cache") / "activitywatch",
        "log": _xdg("XDG_CACHE_HOME", ".cache") / "activitywatch" / "log",
    }


@pytest.fixture(autouse=True)
def _default_profile(monkeypatch):
    monkeypatch.delenv("AW_PROFILE", raising=False)
    # The getters create missing dirs; don't write into the real profile.
    monkeypatch.setattr("aw_core.dirs.ensure_path_exists", lambda path: None)


@pytest.mark.parametrize(
    "kind, getter",
    [
        ("data", get_data_dir),
        ("config", get_config_dir),
        ("cache", get_cache_dir),
        ("log", get_log_dir),
    ],
)
def test_default_dirs_are_pinned(kind, getter):
    assert Path(getter("aw-server")) == _expected_roots()[kind] / "aw-server"


class _PathCaptured(Exception):
    pass


def test_peewee_db_path_is_pinned(monkeypatch):
    """The file aw-server-rust's legacy import looks for (see module docstring).

    Stops before anything is opened, so the real user database is untouched.
    """
    from aw_datastore.storages import peewee

    def capture(filepath):
        raise _PathCaptured(filepath)

    monkeypatch.setattr(peewee, "maybe_recover_malformed_sqlite", capture)
    with pytest.raises(_PathCaptured) as captured:
        peewee.PeeweeStorage(testing=False)
    assert Path(captured.value.args[0]) == (
        _expected_roots()["data"] / "aw-server" / "peewee-sqlite.v2.db"
    )
