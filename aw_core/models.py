import json
import logging

from datetime import datetime, timedelta, timezone

import typing
from typing import Any, List, Dict, Union, Optional

import iso8601

logger = logging.getLogger(__name__)


def _timestamp_parse(ts: Union[str, datetime]) -> datetime:
    """
    Takes something representing a timestamp and
    returns a timestamp in the representation we want.
    """
    if isinstance(ts, str):
        ts = iso8601.parse_date(ts)
    # Set resolution to milliseconds instead of microseconds
    # (Fixes incompability with software based on unix time, for example mongodb)
    ts = ts.replace(microsecond=int(ts.microsecond / 1000) * 1000)
    # Add timezone if not set
    if not ts.tzinfo:
        # Needed? All timestamps should be iso8601 so ought to always contain timezone.
        # Yes, because it is optional in iso8601
        logger.warning("timestamp without timezone found, using UTC: {}".format(ts))
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


Id = Union[int, str]
Timestamp = Union[datetime, str]
Duration = Union[timedelta, float]
Data = Dict[str, Any]


class Event(dict):
    """
    Used to represents an event.
    """

    def __init__(self, id: Id = None, timestamp: Timestamp = None,
                 duration: Duration = 0, data: Data = dict()) -> None:
        self.id = id
        if timestamp is None:
            logger.warning("Event initializer did not receive a timestamp argument, using now as timestamp")
            # FIXME: The typing.cast here was required for mypy to shut up, weird...
            self.timestamp = datetime.now(typing.cast(timezone, timezone.utc))
        else:
            self.timestamp = timestamp
        self.duration = duration
        self.data = data

    def __eq__(self, other):
        return self.timestamp == other.timestamp\
            and self.duration == other.duration\
            and self.data == other.data

    def to_json_dict(self) -> dict:
        """Useful when sending data over the wire.
        Any mongodb interop should not use do this as it accepts datetimes."""
        json_data = self.copy()
        json_data["timestamp"] = self.timestamp.astimezone(timezone.utc).isoformat()
        json_data["duration"] = self.duration.total_seconds()
        return json_data

    def to_json_str(self) -> str:
        data = self.to_json_dict()
        return json.dumps(data)

    def _hasprop(self, propname):
        """Badly named, but basically checks if the underlying
        dict has a prop, and if it is a non-empty list"""
        return propname in self and self[propname]

    @property
    def id(self) -> Any:
        return self["id"] if self._hasprop("id") else None

    @id.setter
    def id(self, id: Any):
        self["id"] = id

    @property
    def data(self) -> dict:
        return self["data"] if self._hasprop("data") else {}

    @data.setter
    def data(self, data: dict):
        self["data"] = data

    @property
    def timestamp(self) -> Optional[datetime]:
        return self["timestamp"]

    @timestamp.setter
    def timestamp(self, timestamp: Union[str, datetime]) -> None:
        self["timestamp"] = _timestamp_parse(timestamp).astimezone(timezone.utc)

    @property
    def duration(self) -> timedelta:
        return self["duration"] if self._hasprop("duration") else timedelta(0)

    @duration.setter
    def duration(self, duration: Union[timedelta, float]) -> None:
        if type(duration) == timedelta:
            self["duration"] = duration
        elif type(duration) == float:
            self["duration"] = timedelta(seconds=duration)  # type: ignore
        else:
            logger.error("Couldn't parse duration of invalid type {}".format(type(duration)))
