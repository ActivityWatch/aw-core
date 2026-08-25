"""Tests for aw_cli.log.find_oldest_log after profile isolation (#149).

With per-profile appname dirs, every .log file in a module dir belongs to the
active profile — no filename-based filtering is needed.
"""

import time
from pathlib import Path

import pytest

from aw_cli.log import find_oldest_log

from . import context  # noqa: F401


@pytest.fixture()
def log_dir(tmp_path: Path) -> Path:
    return tmp_path / "aw-server"


def test_returns_none_for_missing_dir(tmp_path: Path) -> None:
    assert find_oldest_log(tmp_path / "nonexistent") is None


def test_returns_none_for_empty_dir(log_dir: Path) -> None:
    log_dir.mkdir()
    assert find_oldest_log(log_dir) is None


def test_returns_single_log(log_dir: Path) -> None:
    log_dir.mkdir()
    f = log_dir / "aw-server_2026-01-01.log"
    f.write_text("line\n")
    assert find_oldest_log(log_dir) == f


def test_returns_newest_by_mtime(log_dir: Path) -> None:
    log_dir.mkdir()
    old = log_dir / "aw-server_old.log"
    new = log_dir / "aw-server_new.log"
    old.write_text("old\n")
    time.sleep(0.01)
    new.write_text("new\n")
    assert find_oldest_log(log_dir) == new


def test_ignores_non_log_files(log_dir: Path) -> None:
    log_dir.mkdir()
    (log_dir / "notes.txt").write_text("not a log")
    assert find_oldest_log(log_dir) is None


def test_no_profile_filtering_in_filenames(log_dir: Path) -> None:
    """All .log files in the dir belong to the active profile — no name filter."""
    log_dir.mkdir()
    default_log = log_dir / "aw-server_2026-01-01.log"
    default_log.write_text("line\n")
    # A file named with 'testing' in its name is still returned;
    # profile selection happens at the directory level, not the filename level.
    testing_named = log_dir / "aw-server_testing_2026-01-02.log"
    time.sleep(0.01)
    testing_named.write_text("testing-named\n")
    assert find_oldest_log(log_dir) == testing_named
