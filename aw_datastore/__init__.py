import platform as _platform

from . import storages
from .datastore import Datastore


def get_storage_methods():
    from .storages import MemoryStorage, MongoDBStorage, PeeweeStorage
    methods = {
        PeeweeStorage.sid: PeeweeStorage,
        MemoryStorage.sid: MemoryStorage,
    }

    # MongoDB is not supported on Windows or macOS
    if _platform.system() == "Linux":  # pragma: no branch
        try:
            import pymongo
            methods[MongoDBStorage.sid] = MongoDBStorage
        except ImportError:  # pragma: no cover
            pass

    return methods
