import logging
from typing import List, Union

from aw_core.models import Event

from .storage_strategies import StorageStrategy, MemoryStorageStrategy, FileStorageStrategy, MongoDBStorageStrategy

logger = logging.getLogger("aw_datastore")


MEMORY = MemoryStorageStrategy
FILES = FileStorageStrategy
MONGODB = MongoDBStorageStrategy


class Datastore:
    def __init__(self, storage_strategy: StorageStrategy = MEMORY, testing=False):
        self.logger = logging.getLogger("datastore")

        if storage_strategy not in [MEMORY, MONGODB, FILES]:
            raise Exception("Unsupported storage strategy: {}".format(storage_strategy))

        self.storage_strategy = storage_strategy()

    def create_bucket(self):
        return self.storage_strategy.create_bucket()

    def buckets(self):
        return self.storage_strategy.buckets()

    def __getitem__(self, bucket_id: str):
        return Bucket(self, bucket_id)


class Bucket:
    def __init__(self, datastore: Datastore, bucket_id: str):
        self.ds = datastore
        self.bucket_id = bucket_id

    def metadata(self):
        return self.ds.storage_strategy.metadata(self.bucket_id)

    def get(self, limit: int = 10**4):
        return self.ds.storage_strategy.get_events(self.bucket_id, limit)

    def insert(self, events: Union[Event, List[Event]]):
        return self.ds.storage_strategy.insert(self.bucket_id, events)

    def insert_one(self, event: Event):
        return self.ds.storage_strategy.insert_one(self.bucket_id, event)

    def insert_many(self, events: List[Event]):
        return self.ds.storage_strategy.insert_many(self.bucket_id, events)
