import iso8601
import os
import sys
import copy
from typing import List
from datetime import datetime, timedelta

from tinydb import TinyDB, where
from tinydb.storages import JSONStorage
from tinydb_serialization import Serializer, SerializationMiddleware

from aw_core.models import Event

from . import logger, data_dir
from .abstract import AbstractStorage


# For TinyDBStorage
class DateTimeSerializer(Serializer):
    OBJ_CLASS = datetime  # The class this serializer handles

    def encode(self, obj):
        return obj.isoformat()

    def decode(self, s):
        return iso8601.parse_date(s)


class TimeDeltaSerializer(Serializer):
    OBJ_CLASS = timedelta  # The class this serializer handles

    def encode(self, td):
        # https://docs.python.org/3.5/library/datetime.html#datetime.timedelta.total_seconds
        return str(td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6)

    def decode(self, i):
        return timedelta(microseconds=int(i))


class TinyDBStorage(AbstractStorage):
    """
    TinyDB storage method
    """
    sid = "tinydb"

    def __init__(self, testing):
        self.logger = logger.getChild(self.sid)

        # Create dirs
        self.buckets_dir = os.path.join(data_dir, "testing" if testing else "", "buckets")
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
        serializer.register_serializer(DateTimeSerializer(), 'DateTime')
        serializer.register_serializer(TimeDeltaSerializer(), 'TimeDelta')

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
        events = sorted(events, key=lambda k: k.timestamp)[::-1]
        # Filter starttime
        if starttime:
            e = []
            for event in events:
                if event.timestamp > starttime:
                    e.append(event)
            events = e
        # Filter endtime
        if endtime:
            e = []
            for event in events:
                if event.timestamp < endtime:
                    e.append(event)
            events = e
        # Limit
        return events[:limit]

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
        e = self.events[bucket_id].get(where('timestamp') == self.get_events(bucket_id, 1)[0].timestamps)
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
        if bucket_id in self.db:
            self.db.pop(bucket_id)
            os.remove(self._get_bucket_db_path(bucket_id))
