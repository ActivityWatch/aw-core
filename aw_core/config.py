import os
import logging
from configparser import ConfigParser

from aw_core import dirs

logger = logging.getLogger(__name__)


def load_config(appname, default_config):
    """
    Take the defaults, and if a config file exists, use the settings specified
    there as overrides for their respective defaults.
    """
    config = default_config

    config_dir = dirs.get_config_dir(appname)
    config_file_path = os.path.join(config_dir, "{}.ini".format(appname))

    # Override defaults from existing config file
    if os.path.isfile(config_file_path):
        with open(config_file_path, "r") as f:
            config.read_file(f)

    # Overwrite current config file (necessary in case new default would be added)
    save_config(appname, config)

    return config


def save_config(appname, config):
    config_dir = dirs.get_config_dir(appname)
    config_file_path = os.path.join(config_dir, "{}.ini".format(appname))
    with open(config_file_path, "w") as f:
        config.write(f)
