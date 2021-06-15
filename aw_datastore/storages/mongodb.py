import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone

import iso8601

# MongoDB
try:
    import pymongo
    from bson.objectid import ObjectId
except ImportError:  # pragma: no cover
    logging.warning("Could not import pymongo, not available as a datastore backend")

from aw_core.models import Event

from . import logger
from .abstract import AbstractStorage


class MongoDBStorage(AbstractStorage):
    """Uses a MongoDB server as backend"""

    sid = "mongodb"

    def __init__(self, testing) -> None:
        self.logger = logger.getChild(self.sid)

        self.client = pymongo.MongoClient(serverSelectionTimeoutMS=5000)
        # Try to connect to the server to make sure that it's available
        # If it isn't, it will raise pymongo.errors.ServerSelectionTimeoutError
        self.client.server_info()

        self.db = self.client["activitywatch" + ("-testing" if testing else "")]

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: str = None,
    ) -> None:
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
        print(self.db.collection_names())
        if bucket_id + ".metadata" in self.db.collection_names():
            self.db[bucket_id]["events"].drop()
            self.db[bucket_id]["metadata"].drop()
        else:
            # TODO: Create custom exception
            raise Exception("Bucket did not exist, could not delete")

    def buckets(self) -> Dict[str, dict]:
        bucketnames = set()
        for bucket_coll in self.db.collection_names():
            bucketnames.add(bucket_coll.split(".")[0])
        bucketnames.discard("system")  # Discard all system collections
        buckets = dict()
        for bucket_id in bucketnames:
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get_metadata(self, bucket_id: str) -> dict:
        metadata = self.db[bucket_id]["metadata"].find_one({"_id": "metadata"})
        if metadata:
            del metadata["_id"]
            return metadata
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        query_filter: Dict[str, dict] = {}
        if starttime or endtime:
            query_filter["timestamp"] = {}
            if starttime:
                query_filter["timestamp"]["$gte"] = starttime
            if endtime:
                query_filter["timestamp"]["$lte"] = endtime

        if limit == 0:
            return []
        elif limit < 0:
            limit = 10 ** 9
        ds_events = list(
            self.db[bucket_id]["events"]
            .find(query_filter)
            .sort([("timestamp", -1)])
            .limit(limit)
        )

        events = []
        for event in ds_events:
            event["id"] = str(event.pop("_id"))
            # Required since MongoDB doesn't handle timezones
            event["timestamp"] = event["timestamp"].replace(tzinfo=timezone.utc)
            event = Event(**event)
            events.append(event)
        return events

    def get_eventcount(
        self, bucket_id: str, starttime: datetime = None, endtime: datetime = None
    ) -> int:
        query_filter: Dict[str, dict] = {}
        if starttime or endtime:
            query_filter["timestamp"] = {}
            if starttime:
                query_filter["timestamp"]["$gte"] = starttime
            if endtime:
                query_filter["timestamp"]["$lte"] = endtime
        return self.db[bucket_id]["events"].find(query_filter).count()

    def _transform_event(self, event: dict) -> dict:
        if "duration" in event:  # pragma: no cover
            event["duration"] = event["duration"].total_seconds()
        return event

    def insert_one(self, bucket: str, event: Event) -> Event:
        # .copy is needed because otherwise mongodb inserts a _id field into the event
        dict_event = event.copy()
        dict_event = self._transform_event(dict_event)
        returned = self.db[bucket]["events"].insert_one(dict_event)
        event.id = returned.inserted_id
        return event

    def insert_many(self, bucket: str, events: List[Event]):
        # .copy is needed because otherwise mongodb inserts a _id field into the event
        dict_events: List[dict] = [
            self._transform_event(event.copy()) for event in events
        ]
        self.db[bucket]["events"].insert_many(dict_events)

    def delete(self, bucket_id: str, event_id) -> bool:
        result = self.db[bucket_id]["events"].delete_one({"_id": ObjectId(event_id)})
        return result.deleted_count >= 1

    def replace_last(self, bucket_id: str, event: Event):
        last_event = list(
            self.db[bucket_id]["events"].find().sort([("timestamp", -1)]).limit(1)
        )[0]
        self.db[bucket_id]["events"].replace_one(
            {"_id": last_event["_id"]}, self._transform_event(event.copy())
        )

    def replace(self, bucket_id: str, event_id, event: Event) -> bool:
        self.db[bucket_id]["events"].replace_one(
            {"_id": event_id}, self._transform_event(event.copy())
        )
        event.id = event_id
        return True
