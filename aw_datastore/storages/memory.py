import sys
import copy
from datetime import datetime
from typing import List, Dict, Optional

from aw_core.models import Event

from . import logger
from .abstract import AbstractStorage


class MemoryStorage(AbstractStorage):
    """For storage of data in-memory, useful primarily in testing"""

    sid = "memory"

    def __init__(self, testing: bool) -> None:
        self.logger = logger.getChild(self.sid)
        # self.logger.warning("Using in-memory storage, any events stored will not be persistent and will be lost when server is shut down. Use the --storage parameter to set a different storage method.")
        self.db: Dict[str, List[Event]] = {}
        self._metadata: Dict[str, dict] = dict()

    def create_bucket(
        self, bucket_id, type_id, client, hostname, created, name=None
    ) -> None:
        if not name:
            name = bucket_id
        self._metadata[bucket_id] = {
            "id": bucket_id,
            "name": name,
            "type": type_id,
            "client": client,
            "hostname": hostname,
            "created": created,
        }
        self.db[bucket_id] = []

    def delete_bucket(self, bucket_id: str) -> None:
        if bucket_id in self.db:
            del self.db[bucket_id]
        if bucket_id in self._metadata:
            del self._metadata[bucket_id]
        else:
            raise Exception("Bucket did not exist, could not delete")

    def buckets(self):
        buckets = dict()
        for bucket_id in self.db:
            buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        event = self._get_event(bucket_id, event_id)
        return copy.deepcopy(event)

    def get_events(
        self,
        bucket: str,
        limit: int,
        starttime: datetime = None,
        endtime: datetime = None,
    ) -> List[Event]:
        events = self.db[bucket]

        # Sort by timestamp
        events = sorted(events, key=lambda k: k["timestamp"])[::-1]

        # Filter by date
        if starttime:
            events = [e for e in events if starttime <= (e.timestamp + e.duration)]
        if endtime:
            events = [e for e in events if e.timestamp <= endtime]

        # Limit
        if limit == 0:
            return []
        elif limit < 0:
            limit = sys.maxsize
        events = events[:limit]
        # Return
        return copy.deepcopy(events)

    def get_eventcount(
        self, bucket: str, starttime: datetime = None, endtime: datetime = None
    ) -> int:
        return len(
            [
                e
                for e in self.db[bucket]
                if (not starttime or starttime <= e.timestamp)
                and (not endtime or e.timestamp <= endtime)
            ]
        )

    def get_metadata(self, bucket_id: str):
        if bucket_id in self._metadata:
            return self._metadata[bucket_id]
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket: str, event: Event) -> Event:
        if event.id is not None:
            self.replace(bucket, event.id, event)
        else:
            # We need to copy the event to avoid setting the ID on the passed event
            event = copy.copy(event)
            if self.db[bucket]:
                event.id = max(int(e.id or 0) for e in self.db[bucket]) + 1
            else:
                event.id = 0
            self.db[bucket].append(event)
        return event

    def delete(self, bucket_id, event_id):
        for idx in (
            idx
            for idx, event in reversed(list(enumerate(self.db[bucket_id])))
            if event.id == event_id
        ):
            self.db[bucket_id].pop(idx)
            return True
        return False

    def _get_event(self, bucket_id, event_id) -> Optional[Event]:
        events = [
            event
            for idx, event in reversed(list(enumerate(self.db[bucket_id])))
            if event.id == event_id
        ]
        if len(events) < 1:
            return None
        else:
            return events[0]

    def replace(self, bucket_id, event_id, event):
        for idx in (
            idx
            for idx, event in reversed(list(enumerate(self.db[bucket_id])))
            if event.id == event_id
        ):
            # We need to copy the event to avoid setting the ID on the passed event
            event = copy.copy(event)
            event.id = event_id
            self.db[bucket_id][idx] = event

    def replace_last(self, bucket_id, event):
        # NOTE: This does not actually get the most recent event, only the last inserted
        last = sorted(self.db[bucket_id], key=lambda e: e.timestamp)[-1]
        self.replace(bucket_id, last.id, event)
