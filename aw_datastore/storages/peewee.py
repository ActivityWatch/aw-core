from typing import Optional, List
from datetime import datetime
import json
import os
import logging

from peewee import Model, CharField, IntegerField, DecimalField, DateTimeField, ForeignKeyField
from playhouse.sqlite_ext import SqliteExtDatabase

from aw_core.models import Event
from aw_core.dirs import get_data_dir

from . import logger
from .abstract import AbstractStorage

logger = logger.getChild("peewee")

# Prevent debug output from propagating
peewee_logger = logging.getLogger("peewee")
peewee_logger.setLevel(logging.INFO)

# Init'd later in the PeeweeStorage constructor.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#run-time-database-configuration
# Another option would be to use peewee's Proxy.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#dynamic-db
_db = SqliteExtDatabase(None)


class BaseModel(Model):
    class Meta:
        database = _db


class BucketModel(BaseModel):
    key = IntegerField(primary_key=True)
    id = CharField(unique=True)
    created = DateTimeField(default=datetime.now)
    name = CharField(null=True)
    type = CharField()
    client = CharField()
    hostname = CharField()

    def json(self):
        return {"id": self.id, "created": self.created,
                "name": self.name, "type": self.type, "client": self.client,
                "hostname": self.hostname}


class EventModel(BaseModel):
    bucket = ForeignKeyField(BucketModel, related_name='events', index=True)
    timestamp = DateTimeField(index=True, default=datetime.now)
    duration = DecimalField()
    datastr = CharField()

    @classmethod
    def from_event(cls, bucket_key, event: Event):
        return cls(bucket=bucket_key, timestamp=event.timestamp, duration=event.duration.total_seconds(), datastr=json.dumps(event.data))

    def json(self):
        return {
            "timestamp": self.timestamp,
            "duration": float(self.duration),
            "data": json.loads(self.datastr)
        }


class PeeweeStorage(AbstractStorage):
    sid = "peewee"

    def __init__(self, testing):
        self.logger = logger.getChild(self.sid)

        filename = 'peewee-sqlite' + ('-testing' if testing else '') + '.db'
        filepath = os.path.join(get_data_dir("aw-server"), filename)
        self.db = _db
        self.db.init(filepath)

        # db.connect()

        self.bucket_keys = {}
        if not BucketModel.table_exists():
            BucketModel.create_table()
        if not EventModel.table_exists():
            EventModel.create_table()
        self.update_bucket_keys()

    def update_bucket_keys(self):
        buckets = BucketModel.select()
        self.bucket_keys = {bucket.id: bucket.key for bucket in buckets}

    def buckets(self):
        buckets = {bucket.id: bucket.json() for bucket in BucketModel.select()}
        return buckets

    def create_bucket(self, bucket_id: str, type_id: str, client: str,
                      hostname: str, created: str, name: Optional[str] = None):
        BucketModel.create(id=bucket_id, type=type_id, client=client,
                           hostname=hostname, created=created, name=name)
        self.update_bucket_keys()

    def delete_bucket(self, bucket_id: str):
        EventModel.delete().where(EventModel.bucket == self.bucket_keys[bucket_id]).execute()
        BucketModel.delete().where(BucketModel.key == self.bucket_keys[bucket_id]).execute()
        self.update_bucket_keys()

    def get_metadata(self, bucket_id: str):
        return BucketModel.get(BucketModel.key == self.bucket_keys[bucket_id]).json()

    def insert_one(self, bucket_id: str, event: Event):
        e = EventModel.from_event(self.bucket_keys[bucket_id], event)
        e.save()

    def insert_many(self, bucket_id, events: List[Event]):
        # FIXME: Breaks for 10**5 events, use chunking when inserting
        events_dict = [{"bucket": self.bucket_keys[bucket_id],
                        "timestamp": event.timestamp,
                        "duration": event.duration.total_seconds(),
                        "datastr": json.dumps(event.data)}
                       for event in events]
        with self.db.atomic():
            EventModel.insert_many(events_dict).execute()

    def _get_last(self, bucket_id, event):
        return EventModel.select() \
                         .where(EventModel.bucket == self.bucket_keys[bucket_id]) \
                         .order_by(EventModel.timestamp.desc()) \
                         .limit(1) \
                         .get()

    def replace_last(self, bucket_id, event):
        e = self._get_last(bucket_id, event)
        e.timestamp = event.timestamp
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        q = EventModel.select() \
                      .where(EventModel.bucket == self.bucket_keys[bucket_id]) \
                      .order_by(EventModel.timestamp.desc()) \
                      .limit(limit)
        if starttime:
            q = q.where(starttime < EventModel.timestamp)
        if endtime:
            q = q.where(EventModel.timestamp < endtime)
        return [Event(**e) for e in list(map(EventModel.json, q.execute()))]
