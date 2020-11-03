import logging
from typing import List, Dict, Optional
from datetime import datetime
from abc import ABCMeta, abstractmethod, abstractproperty

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

    @abstractmethod
    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_bucket(self, bucket_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, bucket_id: str) -> dict:
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
