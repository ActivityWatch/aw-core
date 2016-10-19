import logging as _logging
logger = _logging.getLogger("aw.datastore.storages")

from .abstract import AbstractStorage

from .sqlite import SQLiteStorage
from .memory import MemoryStorage
from .mongodb import MongoDBStorage
from .tinydb import TinyDBStorage
from .peewee import PeeweeStorage
