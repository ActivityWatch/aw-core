from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import json
import os
import logging
import iso8601

from peewee import (
    Model,
    CharField,
    IntegerField,
    DecimalField,
    DateTimeField,
    ForeignKeyField,
    AutoField,
)
from playhouse.sqlite_ext import SqliteExtDatabase

from aw_core.models import Event
from aw_core.dirs import get_data_dir

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

# Prevent debug output from propagating
peewee_logger = logging.getLogger("peewee")
peewee_logger.setLevel(logging.INFO)

# Init'd later in the PeeweeStorage constructor.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#run-time-database-configuration
# Another option would be to use peewee's Proxy.
#   See: http://docs.peewee-orm.com/en/latest/peewee/database.html#dynamic-db
_db = SqliteExtDatabase(None)


LATEST_VERSION = 2


def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From: https://stackoverflow.com/a/312464/965332"""
    for i in range(0, len(l), n):
        yield l[i : i + n]


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
        return {
            "id": self.id,
            "created": iso8601.parse_date(self.created)
            .astimezone(timezone.utc)
            .isoformat(),
            "name": self.name,
            "type": self.type,
            "client": self.client,
            "hostname": self.hostname,
        }


class EventModel(BaseModel):
    id = AutoField()
    bucket = ForeignKeyField(BucketModel, backref="events", index=True)
    timestamp = DateTimeField(index=True, default=datetime.now)
    duration = DecimalField()
    datastr = CharField()

    @classmethod
    def from_event(cls, bucket_key, event: Event):
        return cls(
            bucket=bucket_key,
            id=event.id,
            timestamp=event.timestamp,
            duration=event.duration.total_seconds(),
            datastr=json.dumps(event.data),
        )

    def json(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "duration": float(self.duration),
            "data": json.loads(self.datastr),
        }


class PeeweeStorage(AbstractStorage):
    sid = "peewee"

    def __init__(self, testing: bool = True, filepath: str = None) -> None:
        data_dir = get_data_dir("aw-server")

        if not filepath:
            filename = (
                "peewee-sqlite"
                + ("-testing" if testing else "")
                + ".v{}".format(LATEST_VERSION)
                + ".db"
            )
            filepath = os.path.join(data_dir, filename)
        self.db = _db
        self.db.init(filepath)
        logger.info("Using database file: {}".format(filepath))

        self.db.connect()

        self.bucket_keys: Dict[str, int] = {}
        BucketModel.create_table(safe=True)
        EventModel.create_table(safe=True)
        self.update_bucket_keys()

    def update_bucket_keys(self) -> None:
        buckets = BucketModel.select()
        self.bucket_keys = {bucket.id: bucket.key for bucket in buckets}

    def buckets(self) -> Dict[str, Dict[str, Any]]:
        buckets = {bucket.id: bucket.json() for bucket in BucketModel.select()}
        return buckets

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
    ):
        BucketModel.create(
            id=bucket_id,
            type=type_id,
            client=client,
            hostname=hostname,
            created=created,
            name=name,
        )
        self.update_bucket_keys()

    def delete_bucket(self, bucket_id: str) -> None:
        if bucket_id in self.bucket_keys:
            EventModel.delete().where(
                EventModel.bucket == self.bucket_keys[bucket_id]
            ).execute()
            BucketModel.delete().where(
                BucketModel.key == self.bucket_keys[bucket_id]
            ).execute()
            self.update_bucket_keys()
        else:
            raise Exception("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        if bucket_id in self.bucket_keys:
            return BucketModel.get(
                BucketModel.key == self.bucket_keys[bucket_id]
            ).json()
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        e = EventModel.from_event(self.bucket_keys[bucket_id], event)
        e.save()
        event.id = e.id
        return event

    def insert_many(self, bucket_id, events: List[Event], fast=False) -> None:
        events_dictlist = [
            {
                "bucket": self.bucket_keys[bucket_id],
                "timestamp": event.timestamp,
                "duration": event.duration.total_seconds(),
                "datastr": json.dumps(event.data),
            }
            for event in events
        ]
        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948
        for chunk in chunks(events_dictlist, 100):
            EventModel.insert_many(chunk).execute()

    def _get_event(self, bucket_id, event_id) -> EventModel:
        return (
            EventModel.select()
            .where(EventModel.id == event_id)
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .get()
        )

    def _get_last(self, bucket_id) -> EventModel:
        return (
            EventModel.select()
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .order_by(EventModel.timestamp.desc())
            .get()
        )

    def replace_last(self, bucket_id, event):
        e = self._get_last(bucket_id)
        e.timestamp = event.timestamp
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        return event

    def delete(self, bucket_id, event_id):
        return (
            EventModel.delete()
            .where(EventModel.id == event_id)
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .execute()
        )

    def replace(self, bucket_id, event_id, event):
        e = self._get_event(bucket_id, event_id)
        e.timestamp = event.timestamp
        e.duration = event.duration.total_seconds()
        e.datastr = json.dumps(event.data)
        e.save()
        event.id = e.id
        return event

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        if limit == 0:
            return []
        q = (
            EventModel.select()
            .where(EventModel.bucket == self.bucket_keys[bucket_id])
            .order_by(EventModel.timestamp.desc())
            .limit(limit)
        )
        if starttime:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            starttime = starttime.astimezone(timezone.utc)
            q = q.where(starttime <= EventModel.timestamp)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
            q = q.where(EventModel.timestamp <= endtime)
        return [Event(**e) for e in list(map(EventModel.json, q.execute()))]

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        q = EventModel.select().where(EventModel.bucket == self.bucket_keys[bucket_id])
        if starttime:
            # Important to normalize datetimes to UTC, otherwise any UTC offset will be ignored
            starttime = starttime.astimezone(timezone.utc)
            q = q.where(starttime <= EventModel.timestamp)
        if endtime:
            endtime = endtime.astimezone(timezone.utc)
            q = q.where(EventModel.timestamp <= endtime)
        return q.count()
