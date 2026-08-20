"""Tests for profile isolation via AW_PROFILE in aw_core.dirs.

Three profiles (default / testing / research) must yield completely disjoint
directory roots so that an ActivityWatch research build cannot read from or
write to a participant's personal datastore.
"""

import os
from unittest.mock import patch

from aw_core.dirs import _get_appname, get_cache_dir, get_config_dir, get_data_dir

from . import context  # noqa: F401


# ---------------------------------------------------------------------------
# _get_appname
# ---------------------------------------------------------------------------


class TestGetAppname:
    def test_unset_returns_bare_name(self):
        """AW_PROFILE absent → bare 'activitywatch', identical to legacy."""
        with patch.dict(os.environ, {"AW_PROFILE": ""}):
            assert _get_appname() == "activitywatch"

    def test_empty_string_returns_bare_name(self):
        """AW_PROFILE='' is treated the same as unset."""
        with patch.dict(os.environ, {"AW_PROFILE": ""}):
            assert _get_appname() == "activitywatch"

    def test_testing_profile(self):
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            assert _get_appname() == "activitywatch-testing"

    def test_research_profile(self):
        with patch.dict(os.environ, {"AW_PROFILE": "research"}):
            assert _get_appname() == "activitywatch-research"

    def test_arbitrary_profile(self):
        with patch.dict(os.environ, {"AW_PROFILE": "myproject"}):
            assert _get_appname() == "activitywatch-myproject"


# ---------------------------------------------------------------------------
# Directory isolation
# ---------------------------------------------------------------------------


def _data_dir(profile: str) -> str:
    """Return get_data_dir() under the given profile (empty string = default)."""
    with patch.dict(os.environ, {"AW_PROFILE": profile}):
        return get_data_dir()


class TestDirsIsolation:
    """Three profiles must yield fully disjoint directory roots."""

    def test_default_has_no_profile_suffix(self):
        d = _data_dir("")
        assert "activitywatch" in d
        # The bare appname must not contain a dash after "activitywatch"
        assert "activitywatch-" not in d

    def test_testing_suffix_present(self):
        assert "activitywatch-testing" in _data_dir("testing")

    def test_research_suffix_present(self):
        assert "activitywatch-research" in _data_dir("research")

    def test_three_profiles_are_disjoint(self):
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

    def test_config_dir_isolated(self):
        with patch.dict(os.environ, {"AW_PROFILE": "research"}):
            cfg = get_config_dir()
        assert "activitywatch-research" in cfg

    def test_cache_dir_isolated(self):
        with patch.dict(os.environ, {"AW_PROFILE": "testing"}):
            cache = get_cache_dir()
        assert "activitywatch-testing" in cache
