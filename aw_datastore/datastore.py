import logging
from datetime import datetime, timezone
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
        self.bucket_instances = {}

        if storage_strategy not in [MEMORY, MONGODB, FILES]:
            raise Exception("Unsupported storage strategy: {}".format(storage_strategy))

        self.storage_strategy = storage_strategy(testing=testing)

    def __repr__(self):
        return "<Datastore object using {}>".format(self.storage_strategy.__class__.__name__)

    def __getitem__(self, bucket_id: str):
        # If this bucket doesn't have a initialized object, create it
        if bucket_id not in self.bucket_instances:
            # If the bucket exists in the database, create an object representation of it
            if bucket_id in self.buckets():
                bucket = Bucket(self, bucket_id)
                self.bucket_instances[bucket_id] = bucket
            else:
                logging.error("Cannot create a Bucket object for {} because it doesn't exist in the database".format(bucket_id))

        return self.bucket_instances[bucket_id]

    def create_bucket(self, bucket_id: str, type: str, client: str, hostname: str,
                      created: datetime = datetime.now(timezone.utc), name: str = None):
        self.logger.info("Creating bucket '{}'".format(bucket_id))
        return self.storage_strategy.create_bucket(bucket_id, type, client, hostname, created)

    def delete_bucket(self, bucket_id):
        self.logger.info("Deleting bucket '{}'".format(bucket_id))
        del self.bucket_instances[bucket_id]
        return self.storage_strategy.delete_bucket(bucket_id)

    def buckets(self):
        return self.storage_strategy.buckets()


class Bucket:
    def __init__(self, datastore: Datastore, bucket_id: str):
        self.ds = datastore
        self.bucket_id = bucket_id

    def metadata(self):
        return self.ds.storage_strategy.get_metadata(self.bucket_id)

    def get(self, limit: int = 10**4):
        return self.ds.storage_strategy.get_events(self.bucket_id, limit)

    def insert(self, events: Union[Event, List[Event]]):
        return self.ds.storage_strategy.insert(self.bucket_id, events)

    def insert_one(self, event: Event):
        return self.ds.storage_strategy.insert_one(self.bucket_id, event)

    def insert_many(self, events: List[Event]):
        return self.ds.storage_strategy.insert_many(self.bucket_id, events)
