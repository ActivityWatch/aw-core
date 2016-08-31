import logging
from datetime import datetime, timezone
from typing import List, Union, Callable, Optional

from aw_core.models import Event

from .storage_strategies import StorageStrategy, MemoryStorageStrategy, FileStorageStrategy, MongoDBStorageStrategy

logger = logging.getLogger("aw.datastore")


MEMORY = MemoryStorageStrategy
FILES = FileStorageStrategy
MONGODB = MongoDBStorageStrategy


class Datastore:
    def __init__(self, storage_strategy: Callable[..., StorageStrategy] = StorageStrategy, testing=False) -> None:
        self.logger = logger.getChild("Datastore")
        self.bucket_instances = dict()  # type: Dict[str, Bucket]

        self.storage_strategy = storage_strategy(testing=testing)

    def __repr__(self):
        return "<Datastore object using {}>".format(self.storage_strategy.__class__.__name__)

    def __getitem__(self, bucket_id: str) -> "Bucket":
        # If this bucket doesn't have a initialized object, create it
        if bucket_id not in self.bucket_instances:
            # If the bucket exists in the database, create an object representation of it
            if bucket_id in self.buckets():
                bucket = Bucket(self, bucket_id)
                self.bucket_instances[bucket_id] = bucket
            else:
                self.logger.error("Cannot create a Bucket object for {} because it doesn't exist in the database".format(bucket_id))
                raise KeyError

        return self.bucket_instances[bucket_id]

    def create_bucket(self, bucket_id: str, type: str, client: str, hostname: str,
                      created: datetime = datetime.now(timezone.utc), name: Optional[str] = None) -> "Bucket":
        self.logger.info("Creating bucket '{}'".format(bucket_id))
        self.storage_strategy.create_bucket(bucket_id, type, client, hostname, created.isoformat(), name=name)
        return self[bucket_id]

    def delete_bucket(self, bucket_id: str):
        self.logger.info("Deleting bucket '{}'".format(bucket_id))
        del self.bucket_instances[bucket_id]
        return self.storage_strategy.delete_bucket(bucket_id)

    def buckets(self):
        return self.storage_strategy.buckets()


class Bucket:
    def __init__(self, datastore: Datastore, bucket_id: str) -> None:
        self.ds = datastore
        self.bucket_id = bucket_id

    def metadata(self) -> dict:
        return self.ds.storage_strategy.get_metadata(self.bucket_id)

    def get(self, limit: int = 10**4) -> List[dict]:
        return self.ds.storage_strategy.get_events(self.bucket_id, limit)

    def insert(self, events: Union[Event, List[Event]]):
        # NOTE: Should we keep the timestamp checking?
        # Get last event for timestamp check after insert
        last_event_list = self.get(1)
        last_event = None
        if len(last_event_list) > 0:
            last_event = last_event_list[0]

        # Call insert
        if isinstance(events, Event):
            oldest_event = events
            return self.ds.storage_strategy.insert_one(self.bucket_id, events)
        elif isinstance(events, list):
            oldest_event = sorted(events, key=lambda k: k['timestamp'])[0]
            return self.ds.storage_strategy.insert_many(self.bucket_id, events)
        else:
            raise TypeError

        # Warn if timestamp is older than last event
        # FIXME: Contains undefined names
        # if last_event:
        #     if oldest_event["timestamp"][0] < prev_event["timestamp"][0].replace(tzinfo=timezone.utc):
        #         logging.warning("Inserting event that has a older timestamp than previous event!"+
        #                         "\nPrevious:" + str(prev_event)+
        #                         "\nInserted:" + str(event))

    def replace_last(self, event):
        return self.ds.storage_strategy.replace_last(self.bucket_id, event)
