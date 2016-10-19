import logging
import sys
import copy
from datetime import datetime

from aw_core.models import Event

from . import logger
from .abstract import AbstractStorage

logger = logger.getChild("memory")


class MemoryStorage(AbstractStorage):
    """For storage of data in-memory, useful primarily in testing"""

    def __init__(self, testing):
        # logger.warning("Using in-memory storage, any events stored will not be persistent and will be lost when server is shut down. Use the --storage parameter to set a different storage method.")
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
        if bucket_id in self.db:
            del self.db[bucket_id]
        if bucket_id in self._metadata:
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
                if event.timestamp > starttime:
                    e.append(event)
            events = e
        if endtime:
            e = []
            for event in events:
                if event.timestamp < endtime:
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
