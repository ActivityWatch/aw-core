"""Tests for profile isolation via AW_PROFILE in aw_core.dirs.

Three profiles (default / testing / research) must yield completely disjoint
directory roots so that an ActivityWatch research build cannot read from or
write to a participant's personal datastore.

The ``testing`` profile additionally follows the new-root-plus-legacy-fallback
rule from ActivityWatch/activitywatch#1399.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from aw_core.dirs import (
    _get_appname,
    get_cache_dir,
    get_config_dir,
    get_data_dir,
    legacy_testing_suffix,
    using_legacy_testing_root,
)

from . import context  # noqa: F401


@pytest.fixture
def fake_dirs(tmp_path, monkeypatch):
    """Point platformdirs at a tmp tree so tests never touch the real home."""
    data = tmp_path / "data"
    config = tmp_path / "config"
    cache = tmp_path / "cache"

    def _join(root: Path, appname: str) -> str:
        return str(root / appname)

    monkeypatch.setattr(
        "aw_core.dirs.platformdirs.user_data_dir",
        lambda appname, *a, **k: _join(data, appname),
    )
    monkeypatch.setattr(
        "aw_core.dirs.platformdirs.user_config_dir",
        lambda appname, *a, **k: _join(config, appname),
    )
    monkeypatch.setattr(
        "aw_core.dirs.platformdirs.user_cache_dir",
        lambda appname, *a, **k: _join(cache, appname),
    )
    monkeypatch.setattr(
        "aw_core.dirs.platformdirs.user_cache_path",
        lambda appname, *a, **k: cache / appname,
    )
    monkeypatch.setattr(
        "aw_core.dirs.platformdirs.user_log_dir",
        lambda appname, *a, **k: str(cache / appname / "log"),
    )
    monkeypatch.delenv("AW_PROFILE", raising=False)
    return {"data": data, "config": config, "cache": cache, "root": tmp_path}


def _plant_legacy_testing_db(fake_dirs) -> Path:
    aw_server = fake_dirs["data"] / "activitywatch" / "aw-server"
    aw_server.mkdir(parents=True)
    marker = aw_server / "peewee-sqlite-testing.v2.db"
    marker.write_text("")
    return marker


# ---------------------------------------------------------------------------
# _get_appname
# ---------------------------------------------------------------------------


class TestGetAppname:
    def test_unset_returns_bare_name(self, fake_dirs):
        """AW_PROFILE absent → bare 'activitywatch', identical to legacy."""
        with patch.dict(os.environ, {"AW_PROFILE": ""}):
            assert _get_appname() == "activitywatch"

    def test_empty_string_returns_bare_name(self, fake_dirs):
        """AW_PROFILE='' is treated the same as unset."""
        with patch.dict(os.environ, {"AW_PROFILE": ""}):
            assert _get_appname() == "activitywatch"

    def test_testing_profile_fresh_uses_new_root(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            assert _get_appname() == "activitywatch-testing"

    def test_research_profile(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "research"}):
            assert _get_appname() == "activitywatch-research"

    def test_arbitrary_profile(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "myproject"}):
            assert _get_appname() == "activitywatch-myproject"


# ---------------------------------------------------------------------------
# testing-root fallback (ActivityWatch/activitywatch#1399)
# ---------------------------------------------------------------------------


class TestTestingRootFallback:
    def test_fresh_setup_uses_new_root(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            assert using_legacy_testing_root() is False
            assert _get_appname() == "activitywatch-testing"
            assert legacy_testing_suffix(True) == ""
            data = get_data_dir()
            assert "activitywatch-testing" in data
            assert (fake_dirs["data"] / "activitywatch-testing").is_dir()

    def test_legacy_artifacts_keep_shared_root(self, fake_dirs):
        _plant_legacy_testing_db(fake_dirs)
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            assert using_legacy_testing_root() is True
            assert _get_appname() == "activitywatch"
            assert legacy_testing_suffix(True) == "-testing"
            data = get_data_dir()
            assert "activitywatch-testing" not in data
            assert data.endswith("activitywatch") or data.endswith("activitywatch/")
            assert not (fake_dirs["data"] / "activitywatch-testing").exists()

    def test_new_root_wins_over_legacy_artifacts(self, fake_dirs):
        _plant_legacy_testing_db(fake_dirs)
        (fake_dirs["data"] / "activitywatch-testing").mkdir()
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            assert using_legacy_testing_root() is False
            assert _get_appname() == "activitywatch-testing"
            assert legacy_testing_suffix(True) == ""

    def test_config_testing_toml_is_a_legacy_marker(self, fake_dirs):
        cfg = fake_dirs["config"] / "activitywatch"
        cfg.mkdir(parents=True)
        (cfg / "config-testing.toml").write_text("")
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            assert using_legacy_testing_root() is True
            assert _get_appname() == "activitywatch"

    def test_testing_without_aw_profile_keeps_filename_suffix(self, fake_dirs):
        """``testing=True`` with no profile still shares the default root."""
        assert legacy_testing_suffix(True) == "-testing"
        assert legacy_testing_suffix(False) == ""

    def test_named_profile_never_suffixes_filenames(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "research"}):
            assert legacy_testing_suffix(True) == ""
            assert legacy_testing_suffix(False) == ""


# ---------------------------------------------------------------------------
# Directory isolation
# ---------------------------------------------------------------------------


def _data_dir(profile: str) -> str:
    """Return get_data_dir() under the given profile (empty string = default)."""
    with patch.dict(os.environ, {"AW_PROFILE": profile}):
        return get_data_dir()


class TestDirsIsolation:
    """Three profiles must yield fully disjoint directory roots."""

    def test_default_has_no_profile_suffix(self, fake_dirs):
        d = _data_dir("")
        assert "activitywatch" in d
        # The bare appname must not contain a dash after "activitywatch"
        assert "activitywatch-" not in d

    def test_testing_suffix_present(self, fake_dirs):
        assert "activitywatch-testing" in _data_dir("testing")

    def test_research_suffix_present(self, fake_dirs):
        assert "activitywatch-research" in _data_dir("research")

    def test_three_profiles_are_disjoint(self, fake_dirs):
        """default, testing, research all produce distinct, non-nested paths."""
        default = _data_dir("")
        testing = _data_dir("testing")
        research = _data_dir("research")

        dirs = {default, testing, research}
        assert len(dirs) == 3, f"Profiles are not disjoint: {dirs}"

        for a in dirs:
            for b in dirs:
                if a != b:
                    assert not a.startswith(b + os.sep), f"{a!r} is a subpath of {b!r}"

    def test_config_dir_isolated(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "research"}):
            cfg = get_config_dir()
        assert "activitywatch-research" in cfg

    def test_cache_dir_isolated(self, fake_dirs):
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            cache = get_cache_dir()
        assert "activitywatch-testing" in cache
