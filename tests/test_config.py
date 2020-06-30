import unittest
import shutil
from configparser import ConfigParser

from aw_core import dirs
from aw_core.config import load_config, save_config


def test_create():
    appname = "aw-core-test"
    section = "section"
    config_dir = dirs.get_config_dir(appname)

    # Remove test config file if it already exists
    shutil.rmtree(config_dir)

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

    # Remove test config file
    shutil.rmtree(config_dir)
