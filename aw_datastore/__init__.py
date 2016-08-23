from .datastore import Datastore

def get_storage_methods():
    from .storage_strategies import FileStorageStrategy, MemoryStorageStrategy, MongoDBStorageStrategy
    methods = [FileStorageStrategy, MemoryStorageStrategy]

    try:
        import pymongo
        methods.append(MongoDBStorageStrategy)
    except:  # pragma: no cover
        pass

    return methods
