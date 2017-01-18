import platform as _platform

from . import storages
from .datastore import Datastore


def get_storage_methods():
    from .storages import MemoryStorage, MongoDBStorage, TinyDBStorage, PeeweeStorage
    methods = [PeeweeStorage, TinyDBStorage, MemoryStorage]  # BerkeleyDBStorage

    # TinyDB doesn't work on Windows
    if _platform.system() != "Windows":
        methods.append(TinyDBStorage)


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
