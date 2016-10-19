import logging
from typing import List, Dict, Optional
from datetime import datetime

from aw_core.models import Event

from . import AbstractStorage

logger = logging.getLogger("aw.datastore.storages.berkeleydb")


class BerkeleyDBStorage(AbstractStorage):
    """
    Storage with BerkeleyDB as backend.
    """

    def __init__(self, testing: bool) -> None:
        raise NotImplementedError

    def buckets(self) -> Dict[str, dict]:
        raise NotImplementedError

    def create_bucket(self, bucket_id: str, type_id: str, client: str,
                      hostname: str, created: str, name: str = None) -> None:
        raise NotImplementedError

    def delete_bucket(self, bucket_id: str) -> None:
        raise NotImplementedError

    def get_metadata(self, bucket_id: str) -> dict:
        raise NotImplementedError

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime]=None, endtime: Optional[datetime]=None) -> List[Event]:
        raise NotImplementedError

    def insert_one(self, bucket_id: str, event: Event) -> None:
        raise NotImplementedError

    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        for event in events:
            self.insert_one(bucket_id, event)

    def replace_last(self, bucket_id: str, event: Event) -> None:
        raise NotImplementedError
