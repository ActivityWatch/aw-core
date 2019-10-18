import logging as _logging

logger: _logging.Logger = _logging.getLogger(__name__)

from .abstract import AbstractStorage

from .memory import MemoryStorage
from .mongodb import MongoDBStorage
from .peewee import PeeweeStorage
from .sqlite import SqliteStorage
