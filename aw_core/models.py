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


class Event(dict):
    """
    Used to represents an event.
    """

    # TODO: Use JSONSchema as specification
    ALLOWED_FIELDS = {
        "timestamp": datetime,
        "duration": timedelta,
        "data": dict,
    }

    def __init__(self, **kwargs: Any) -> None:
        for k, v in list(kwargs.items()):
            if k not in self.ALLOWED_FIELDS:
                kwargs.pop(k)
                logger.warning("Removed invalid field {} from Event kwargs: {}".format(k, kwargs))

        # Set all the kwargs
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        self.verify()

    def __eq__(self, other):
        return self.timestamp == other.timestamp\
            and self.duration == other.duration\
            and self.data == other.data

    def verify(self):
        success = True

        if not self._hasprop("timestamp"):
            logger.warning("Event did not have a timestamp, using now as timestamp")
            # FIXME: The typing.cast here was required for mypy to shut up, weird...
            self.timestamp = datetime.now(typing.cast(timezone, timezone.utc))

        for k in self.ALLOWED_FIELDS.keys():
            if k in self:
                t = type(getattr(self, k))
                if t != self.ALLOWED_FIELDS[k] and not isinstance(t, type(None)):
                    success = False
                    logger.warning("Event from models.py was unable to set attribute {} to correct type\n Supposed to be {}, while actual is {}".format(k, self.ALLOWED_FIELDS[k], type(getattr(self, k))))

        self._drop_invalid_types()
        return success

    def _drop_invalid_types(self):
        # Check for invalid types
        invalid_keys = []
        for k in self.keys():
            v = self[k]
            if not isinstance(v, self.ALLOWED_FIELDS[k]):
                # FIXME: Optionals are invalidly defaulted to None, this is just a workaround so the logs don't get spammed
                if v is not None:
                    logger.error("Found value {} in field {} that was not of proper instance ({}, expected: {}). Event: {}"
                                 .format(v, k, type(v), self.ALLOWED_FIELDS[k], self))
                invalid_keys.append(k)
        for k in invalid_keys:
            del self[k]

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
            self["duration"] = timedelta(seconds=duration) # type: ignore
        else:
            logger.error("Couldn't parse duration of invalid type {}".format(type(duration)))
