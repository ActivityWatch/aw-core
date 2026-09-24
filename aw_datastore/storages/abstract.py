from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Dict, Iterator, List, Optional

from aw_core.models import Event


class AbstractStorage(metaclass=ABCMeta):
    """
    Interface for storage methods.
    """

    sid = "Storage id not set, fix me"

    @abstractmethod
    def __init__(self, testing: bool) -> None:
        self.testing = True
        raise NotImplementedError

    @abstractmethod
    def buckets(self) -> Dict[str, dict]:
        raise NotImplementedError

    def has_bucket(self, bucket_id: str) -> bool:
        return bucket_id in self.buckets()

    def buckets_with_last_updated(self) -> Dict[str, dict]:
        buckets = self.buckets()
        for bucket_id, metadata in buckets.items():
            events = self.get_events(bucket_id, 1)
            if events:
                metadata["last_updated"] = (
                    events[0].timestamp + events[0].duration
                ).isoformat()
        return buckets

    def iter_events(self, bucket_id: str) -> Iterator[Event]:
        """Iterate a bucket in the same order as an unbounded get_events call.

        Disk backends override this to avoid materializing all events. Callers
        must close the iterator if they stop consuming it early.
        """
        yield from self.get_events(bucket_id, -1)

    @abstractmethod
    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_bucket(
        self,
        bucket_id: str,
        type_id: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_bucket(self, bucket_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, bucket_id: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        raise NotImplementedError

    @abstractmethod
    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:
        raise NotImplementedError

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def insert_one(self, bucket_id: str, event: Event) -> Event:
        raise NotImplementedError

    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        for event in events:
            self.insert_one(bucket_id, event)

    @abstractmethod
    def delete(self, bucket_id: str, event_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace(self, bucket_id: str, event_id: int, event: Event) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace_last(self, bucket_id: str, event: Event) -> None:
        raise NotImplementedError
