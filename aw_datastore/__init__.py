from .datastore import Datastore


def get_storage_methods():
    from .storages import MemoryStorage, MongoDBStorage, TinyDBStorage, SQLiteStorage
    methods = [TinyDBStorage, MemoryStorage]  #, SQLiteStorage

    try:
        import pymongo
        methods.append(MongoDBStorage)
    except ImportError:  # pragma: no cover
        pass

    return methods


def get_storage_method_names():
    methods = get_storage_methods()
    names = [method.__name__[:-15].lower() for method in methods]
    return names
