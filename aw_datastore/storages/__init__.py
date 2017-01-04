import logging as _logging
logger = _logging.getLogger("aw.datastore.storages")  # type: _logging.Logger

import appdirs as _appdirs
import os as _os
# TODO: This should probably be placed in a differently named folder, but will certainly break stuff
data_dir = _appdirs.user_data_dir("aw-server", "activitywatch")  # type: str
if not _os.path.exists(data_dir):
    _os.makedirs(data_dir)

from .abstract import AbstractStorage

from .memory import MemoryStorage
from .mongodb import MongoDBStorage
from .tinydb import TinyDBStorage
from .peewee import PeeweeStorage
