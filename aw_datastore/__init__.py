from typing import Dict, Callable, Any
import platform as _platform

from . import storages
from .datastore import Datastore


# The Callable[[Any], ...] here should be Callable[..., ...] but Python 3.5.2 doesn't
# like ellipsises. See here: https://github.com/python/typing/issues/259
def get_storage_methods() -> Dict[str, Callable[[Any], storages.AbstractStorage]]:
    from .storages import MemoryStorage, MongoDBStorage, PeeweeStorage
    methods = {
        PeeweeStorage.sid: PeeweeStorage,
        MemoryStorage.sid: MemoryStorage,
    }  # type: Dict[str, Callable[[Any], storages.AbstractStorage]]

    # MongoDB is not supported on Windows or macOS
    if _platform.system() == "Linux":  # pragma: no branch
        try:
            import pymongo
            methods[MongoDBStorage.sid] = MongoDBStorage
        except ImportError:  # pragma: no cover
            pass

    return methods
