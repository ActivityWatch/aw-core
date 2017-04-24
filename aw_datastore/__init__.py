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

    # MongoDB is not supported on Windows or macOS
    if _platform.system() == "Linux":  # pragma: no branch
        try:
            import pymongo
            #methods[MongoDBStorage.sid] = MongoDBStorage
        except ImportError:  # pragma: no cover
            pass

    return methods
