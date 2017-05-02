import logging as _logging

logger = _logging.getLogger("aw.datastore.storages")  # type: _logging.Logger

from .abstract import AbstractStorage

from .memory import MemoryStorage
from .mongodb import MongoDBStorage
from .peewee import PeeweeStorage
