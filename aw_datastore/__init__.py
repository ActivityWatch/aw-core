import platform as _platform
from typing import Callable, Dict

from . import storages
from .datastore import Datastore
from .migration import check_for_migration


def get_storage_methods() -> Dict[str, Callable[..., storages.AbstractStorage]]:
    from .storages import (
        MemoryStorage,
        MongoDBStorage,
        PeeweeStorage,
        SqliteStorage,
    )

    methods: Dict[str, Callable[..., storages.AbstractStorage]] = {
        PeeweeStorage.sid: PeeweeStorage,
        MemoryStorage.sid: MemoryStorage,
        SqliteStorage.sid: SqliteStorage,
    }

    # MongoDB is not supported on Windows or macOS
    if _platform.system() == "Linux":  # pragma: no branch
        try:
            import pymongo  # noqa: F401

            methods[MongoDBStorage.sid] = MongoDBStorage
        except ImportError:  # pragma: no cover
            pass

    return methods


__all__ = ["Datastore", "get_storage_methods", "check_for_migration"]
