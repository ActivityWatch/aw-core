import platform as _platform

from . import storages
from .datastore import Datastore


def get_storage_methods():
    from .storages import MemoryStorage, MongoDBStorage, TinyDBStorage, PeeweeStorage
    methods = [PeeweeStorage, MemoryStorage]  # BerkeleyDBStorage

    # TinyDB doesn't work on Windows
    if _platform.system() != "Windows":  # pragma: no branch
        methods.append(TinyDBStorage)

    # MongoDB is only supported on Windows or macOS
    if _platform.system() == "Linux":  # pragma: no branch
        try:
            import pymongo
            methods.append(MongoDBStorage)
        except ImportError:  # pragma: no cover
            pass

    return methods


def get_storage_method_names():
    methods = get_storage_methods()
    names = [method.sid for method in methods]
    return names
