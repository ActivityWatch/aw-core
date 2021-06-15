import shutil
from configparser import ConfigParser

import pytest
import deprecation

from aw_core import dirs
from aw_core.config import load_config, save_config, load_config_toml, save_config_toml

appname = "aw-core-test"
section = "section"
config_dir = dirs.get_config_dir(appname)

default_config_str = f"""# A default config file, with comments!
[{section}]
somestring = "Hello World!"    # A comment
somevalue = 12.3               # Another comment
somearray = ["asd", 123]"""


@pytest.fixture(autouse=True)
def clean_config():
    # Remove test config file if it already exists
    shutil.rmtree(config_dir, ignore_errors=True)

    # Rerun get_config dir to create config directory
    dirs.get_config_dir(appname)

    yield

    # Remove test config file if it already exists
    shutil.rmtree(config_dir)


def test_create():
    appname = "aw-core-test"
    section = "section"
    config_dir = dirs.get_config_dir(appname)


def test_config_defaults():
    # Load non-existing config (will create a out-commented default config file)
    config = load_config_toml(appname, default_config_str)

    # Check that load_config used defaults
    assert config[section]["somestring"] == "Hello World!"
    assert config[section]["somevalue"] == 12.3
    assert config[section]["somearray"] == ["asd", 123]


def test_config_no_defaults():
    # Write defaults to file
    save_config_toml(appname, default_config_str)

    # Load written defaults without defaults
    config = load_config_toml(appname, "")
    assert config[section]["somestring"] == "Hello World!"
    assert config[section]["somevalue"] == 12.3
    assert config[section]["somearray"] == ["asd", 123]


def test_config_override():
    # Create a minimal config file with one overridden value
    config = """[section]
somevalue = 1000.1"""
    save_config_toml(appname, config)

    # Open non-default config file and verify that values are correct
    config = load_config_toml(appname, default_config_str)
    assert config[section]["somevalue"] == 1000.1


@deprecation.fail_if_not_removed
def test_config_ini():
    # Create default config
    default_config = ConfigParser()
    default_config[section] = {"somestring": "Hello World!", "somevalue": 12.3}

    # Load non-existing config (will create a default config file)
    config = load_config(appname, default_config)

    # Check that current config file is same as default config file
    assert config[section]["somestring"] == default_config[section]["somestring"]
    assert config[section].getfloat("somevalue") == default_config[section].getfloat(
        "somevalue"
    )

    # Modify and save config file
    config[section]["somevalue"] = "1000.1"
    save_config(appname, config)

    # Open non-default config file and verify that values are correct
    new_config = load_config(appname, default_config)
    assert new_config[section]["somestring"] == config[section]["somestring"]
    assert new_config[section].getfloat("somevalue") == config[section].getfloat(
        "somevalue"
    )
