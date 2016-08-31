import json
import iso8601
import os
import logging
import sys
import copy
from typing import List, Union, Dict, Sequence
from datetime import datetime
from abc import ABCMeta, abstractmethod

import appdirs
from shutil import rmtree

from aw_core.models import Event

# MongoDB
try:
    import pymongo
except ImportError:  # pragma: no cover
    logging.warning("Could not import pymongo, not available as a datastore backend")

# TinyDB
from tinydb import TinyDB, Query, where
from tinydb.storages import JSONStorage
from tinydb_serialization import Serializer, SerializationMiddleware


logger = logging.getLogger("aw.datastore.strategies")


class StorageStrategy(metaclass=ABCMeta):
    """
    Interface for storage methods.
    """

    @abstractmethod
    def __init__(self, testing: bool) -> None:
        raise NotImplementedError

    @abstractmethod
    def create_bucket(self, bucket_id: str, type_id: str, client: str, hostname: str, created: str, name: str = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_bucket(self, bucket_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, bucket: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_events(self, bucket: str, limit: int,
                   starttime: datetime=None, endtime: datetime=None) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def buckets(self) -> Dict[str, dict]:
        raise NotImplementedError

    @abstractmethod
    def insert_one(self, bucket: str, event: Event) -> None:
        raise NotImplementedError

    def insert_many(self, bucket: str, events: List[Event]) -> None:
        for event in events:
            self.insert_one(bucket, event)

    @abstractmethod
    def replace_last(self, bucket_id: str, event: Event) -> None:
        raise NotImplementedError


class TinyDBStorage():
    """
    TinyDB storage method
    """

    class DateTimeSerializer(Serializer):
        OBJ_CLASS = datetime  # The class this serializer handles

        def encode(self, obj):
            return obj.isoformat()

        def decode(self, s):
            return iso8601.parse_date(s)

    def __init__(self, testing):
        # Create dirs
        self.user_data_dir = appdirs.user_data_dir("aw-server", "activitywatch")
        self.buckets_dir = os.path.join(self.user_data_dir, "testing" if testing else "", "buckets")
        if not os.path.exists(self.buckets_dir):
            os.makedirs(self.buckets_dir)

        self.db = {}
        self.events = {}
        self.metadata = {}
        for bucket_id in os.listdir(self.buckets_dir):
            self._add_bucket(bucket_id)

    def _add_bucket(self, bucket_id: str):
        dbfile = self._get_bucket_db_path(bucket_id)
        serializer = SerializationMiddleware(JSONStorage)
        serializer.register_serializer(self.DateTimeSerializer(), 'DateTime')

        self.db[bucket_id] = TinyDB(dbfile, storage=serializer)
        self.events[bucket_id] = self.db[bucket_id].table('events')
        self.metadata[bucket_id] = self.db[bucket_id].table('metadata')

    def _get_bucket_db_path(self, bucket_id):
        return "{}/{}".format(self.buckets_dir, bucket_id)

    def get_events(self, bucket_id: str, limit: int,
                   starttime: datetime=None, endtime: datetime=None):
        if limit <= 0:
            limit = sys.maxsize
        # Get all events
        events = [Event(**e) for e in self.events[bucket_id].all()]
        # Sort by timestamp
        events = sorted(events, key=lambda k: k['timestamp'])[::-1]
        # Filter starttime
        if starttime:
            e = []
            for event in events:
                if event['timestamp'][0] > starttime:
                    e.append(event)
            events = e
        # Filter endtime
        if endtime:
            e = []
            for event in events:
                if event['timestamp'][0] < endtime:
                    e.append(event)
            events = e
        # Limit
        events = events[:limit]
        for event in events:
            event = Event(**event)
        # Return
        return events

    def buckets(self):
        buckets = {}
        for bucket in self.db:
            buckets[bucket] = self.get_metadata(bucket)
        return buckets

    def get_metadata(self, bucket_id: str):
        metadata = self.metadata[bucket_id].all()[0]
        return metadata

    def insert_one(self, bucket_id: str, event: Event):
        self.events[bucket_id].insert(copy.deepcopy(event))

    def insert_many(self, bucket_id: str, events: List[Event]):
        self.events[bucket_id].insert_multiple(copy.deepcopy(events))

    def replace_last(self, bucket_id, event):
        e = self.events[bucket_id].get(where('timestamp') == self.get_events(bucket_id, 1)[0]["timestamp"])
        self.events[bucket_id].remove(eids=[e.eid])
        self.insert_one(bucket_id, event)

    def create_bucket(self, bucket_id, type_id, client, hostname, created, name=None):
        if not name:
            name = bucket_id
        metadata = {
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created
        }
        self._add_bucket(bucket_id)
        self.metadata[bucket_id].insert(metadata)

    def delete_bucket(self, bucket_id):
        self.db.pop(bucket_id)
        os.remove(self._get_bucket_db_path(bucket_id))


class MongoDBStorageStrategy(StorageStrategy):
    """Uses a MongoDB server as backend"""

    def __init__(self, testing) -> None:
        self.logger = logger.getChild("mongodb")

        self.client = pymongo.MongoClient(serverSelectionTimeoutMS=5000)
        # Try to connect to the server to make sure that it's available
        # If it isn't, it will raise pymongo.errors.ServerSelectionTimeoutError
        self.client.server_info()

        self.db = self.client["activitywatch" + ("-testing" if testing else "")]

    def create_bucket(self, bucket_id: str, type_id: str, client: str, hostname: str, created: str, name: str = None) -> None:
        if not name:
            name = bucket_id
        metadata = {
            "_id": "metadata",
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created,
        }
        self.db[bucket_id]["metadata"].insert_one(metadata)

    def delete_bucket(self, bucket_id: str) -> None:
        self.db[bucket_id]["events"].drop()
        self.db[bucket_id]["metadata"].drop()

    def buckets(self) -> Dict[str, dict]:
        bucketnames = set()
        for bucket_coll in self.db.collection_names():
            bucketnames.add(bucket_coll.split('.')[0])
        buckets = dict()
        for bucket_id in bucketnames:
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get_metadata(self, bucket_id: str) -> dict:
        metadata = self.db[bucket_id]["metadata"].find_one({"_id": "metadata"})
        if metadata:
            del metadata["_id"]
        return metadata

    def get_events(self, bucket_id: str, limit: int,
                   starttime: datetime=None, endtime: datetime=None) -> List[dict]:
        query_filter = {}
        if starttime:
            query_filter["timestamp"] = {}
            query_filter["timestamp"]["$gt"] = starttime
        if endtime:
            if "timestamp" not in query_filter:
                query_filter["timestamp"] = {}
            query_filter["timestamp"]["$lt"] = endtime
        if limit <= 0:
            limit = 10**9
        ds_events = list(self.db[bucket_id]["events"].find(query_filter).sort([("timestamp", -1)]).limit(limit))
        events = []
        for event in ds_events:
            event.pop('_id')
            events.append(Event(**event))
        return events

    def insert_one(self, bucket: str, event: Event):
        # .copy is needed because otherwise mongodb inserts a _id field into the event
        self.db[bucket]["events"].insert_one(event.copy())

    def replace_last(self, bucket_id: str, event: Event):
        last_event = list(self.db[bucket_id]["events"].find().sort([("timestamp", -1)]).limit(1))[0]
        self.db[bucket_id]["events"].replace_one({"_id": last_event["_id"]}, event.to_json_dict())


class MemoryStorageStrategy(StorageStrategy):
    """For storage of data in-memory, useful primarily in testing"""

    def __init__(self, testing):
        self.logger = logger.getChild("memory")
        # self.logger.warning("Using in-memory storage, any events stored will not be persistent and will be lost when server is shut down. Use the --storage parameter to set a different storage method.")
        self.db = {}  # type: Dict[str, List[Event]]
        self._metadata = dict()  # type: Dict[str, dict]

    def create_bucket(self, bucket_id, type_id, client, hostname, created, name=None) -> None:
        if not name:
            name = bucket_id
        self._metadata[bucket_id] = {
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created
        }
        self.db[bucket_id] = []

    def delete_bucket(self, bucket_id: str) -> None:
        del self.db[bucket_id]
        del self._metadata[bucket_id]

    def buckets(self):
        buckets = dict()
        for bucket_id in self.db:
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get_events(self, bucket: str, limit: int,
                   starttime: datetime=None, endtime: datetime=None):
        events = self.db[bucket]
        # Sort by timestamp
        events = sorted(events, key=lambda k: k['timestamp'])[::-1]
        # Filter by date
        if starttime:
            e = []
            for event in events:
                if event['timestamp'][0] > starttime:
                    e.append(event)
            events = e
        if endtime:
            e = []
            for event in events:
                if event['timestamp'][0] < endtime:
                    e.append(event)
            events = e
        # Limit
        if limit == -1:
            limit = sys.maxsize
        events = events[:limit]
        # Return
        return copy.deepcopy(events)

    def get_metadata(self, bucket_id: str):
        return self._metadata[bucket_id]

    def insert_one(self, bucket: str, event: Event):
        self.db[bucket].append(Event(**event))

    def replace_last(self, bucket_id, event):
        self.db[bucket_id][-1] = event
