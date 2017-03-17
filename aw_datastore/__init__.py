import platform as _platform

from . import storages
from .datastore import Datastore


def get_storage_methods():
    from .storages import MemoryStorage, MongoDBStorage, TinyDBStorage, PeeweeStorage
    methods = {
        PeeweeStorage.sid: PeeweeStorage,
        MemoryStorage.sid: MemoryStorage,
    }

    # TinyDB doesn't work on Windows
    if _platform.system() != "Windows":
        methods[TinyDBStorage.sid] = TinyDBStorage

    try:
        import pymongo
        methods[MongoDBStorage.sid] = MongoDBStorage
    except ImportError:  # pragma: no cover
        pass

    return methods
