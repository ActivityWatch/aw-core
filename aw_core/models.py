import json
import logging
import numbers
import typing
from datetime import datetime, timedelta, timezone
from typing import Any, List, Dict, Union, Optional

import iso8601

logger = logging.getLogger(__name__)


Number = Union[int, float]
Id = Optional[Union[int, str]]
ConvertableTimestamp = Union[datetime, str]
Duration = Union[timedelta, Number]
Data = Dict[str, Any]


def _timestamp_parse(ts_in: ConvertableTimestamp) -> datetime:
    """
    Takes something representing a timestamp and
    returns a timestamp in the representation we want.
    """
    ts = iso8601.parse_date(ts_in) if isinstance(ts_in, str) else ts_in
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


class Event(dict):
    """
    Used to represents an event.
    """

    def __init__(
        self,
        id: Id = None,
        timestamp: ConvertableTimestamp = None,
        duration: Duration = 0,
        data: Data = dict(),
    ) -> None:
        self.id = id
        if timestamp is None:
            logger.warning(
                "Event initializer did not receive a timestamp argument, using now as timestamp"
            )
            # FIXME: The typing.cast here was required for mypy to shut up, weird...
            self.timestamp = datetime.now(typing.cast(timezone, timezone.utc))
        else:
            # The conversion needs to be explicit here for mypy to pick it up (lacks support for properties)
            self.timestamp = _timestamp_parse(timestamp)
        self.duration = duration  # type: ignore
        self.data = data

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Event):
            return (
                self.timestamp == other.timestamp
                and self.duration == other.duration
                and self.data == other.data
            )
        else:
            raise TypeError(
                "operator not supported between instances of '{}' and '{}'".format(
                    type(self), type(other)
                )
            )

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Event):
            return self.timestamp < other.timestamp
        else:
            raise TypeError(
                "operator not supported between instances of '{}' and '{}'".format(
                    type(self), type(other)
                )
            )

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

    def _hasprop(self, propname: str) -> bool:
        """Badly named, but basically checks if the underlying
        dict has a prop, and if it is a non-empty list"""
        return propname in self and self[propname] is not None

    @property
    def id(self) -> Id:
        return self["id"] if self._hasprop("id") else None

    @id.setter
    def id(self, id: Id) -> None:
        self["id"] = id

    @property
    def data(self) -> dict:
        return self["data"] if self._hasprop("data") else {}

    @data.setter
    def data(self, data: dict) -> None:
        self["data"] = data

    @property
    def timestamp(self) -> datetime:
        return self["timestamp"]

    @timestamp.setter
    def timestamp(self, timestamp: ConvertableTimestamp) -> None:
        self["timestamp"] = _timestamp_parse(timestamp).astimezone(timezone.utc)

    @property
    def duration(self) -> timedelta:
        return self["duration"] if self._hasprop("duration") else timedelta(0)

    @duration.setter
    def duration(self, duration: Duration) -> None:
        if isinstance(duration, timedelta):
            self["duration"] = duration
        elif isinstance(duration, numbers.Real):
            self["duration"] = timedelta(seconds=duration)  # type: ignore
        else:
            raise TypeError(
                "Couldn't parse duration of invalid type {}".format(type(duration))
            )
