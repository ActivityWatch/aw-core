import os
import logging
from typing import Any, Dict, Union
from configparser import ConfigParser

from deprecation import deprecated
import tomlkit

from aw_core import dirs
from aw_core.__about__ import __version__

logger = logging.getLogger(__name__)


def _merge(a: dict, b: dict, path=None):
    """
    Recursively merges b into a, with b taking precedence.

    From: https://stackoverflow.com/a/7205107/965332
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                _merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a


def _comment_out_toml(s: str):
    return "\n".join(["#" + line for line in s.split("\n")])


def load_config_toml(
    appname: str, default_config: str
) -> Union[dict, tomlkit.container.Container]:
    config_dir = dirs.get_config_dir(appname)
    config_file_path = os.path.join(config_dir, "{}.toml".format(appname))

    # Run early to ensure input is valid toml before writing
    default_config_toml = tomlkit.parse(default_config)

    # Override defaults from existing config file
    if os.path.isfile(config_file_path):
        with open(config_file_path, "r") as f:
            config = f.read()
        config_toml = tomlkit.parse(config)
    else:
        # If file doesn't exist, write with commented-out default config
        with open(config_file_path, "w") as f:
            f.write(_comment_out_toml(default_config))
        config_toml = dict()

    config = _merge(default_config_toml, config_toml)

    return config


def save_config_toml(appname: str, config: str) -> None:
    # Check that passed config string is valid toml
    assert tomlkit.parse(config)

    config_dir = dirs.get_config_dir(appname)
    config_file_path = os.path.join(config_dir, "{}.toml".format(appname))

    with open(config_file_path, "w") as f:
        f.write(config)


@deprecated(
    details="Use the load_config_toml function instead",
    deprecated_in="0.5.3",
    current_version=__version__,
)
def load_config(appname, default_config):
    """
    Take the defaults, and if a config file exists, use the settings specified
    there as overrides for their respective defaults.
    """
    config = default_config

    config_dir = dirs.get_config_dir(appname)
    config_file_path = os.path.join(config_dir, "{}.toml".format(appname))

    # Override defaults from existing config file
    if os.path.isfile(config_file_path):
        with open(config_file_path, "r") as f:
            config.read_file(f)

    # Overwrite current config file (necessary in case new default would be added)
    save_config(appname, config)

    return config


@deprecated(
    details="Use the save_config_toml function instead",
    deprecated_in="0.5.3",
    current_version=__version__,
)
def save_config(appname, config):
    config_dir = dirs.get_config_dir(appname)
    config_file_path = os.path.join(config_dir, "{}.ini".format(appname))
    with open(config_file_path, "w") as f:
        config.write(f)
        # Flush and fsync to lower risk of corrupted files
        f.flush()
        os.fsync(f.fileno())
