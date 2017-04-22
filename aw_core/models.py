import json
import logging

from datetime import datetime, timedelta, timezone

import typing
from typing import Any, List, Dict, Union, Optional

import iso8601

logger = logging.getLogger("aw.client.models")


def _duration_parse(duration: Union[dict, timedelta]) -> timedelta:
    """
    Takes something representing a timestamp and
    returns a timestamp in the representation we want.
    """
    if isinstance(duration, dict):
        if duration["unit"] == "s":
            duration = timedelta(seconds=duration["value"])
        else:
            raise Exception("Unknown unit '{}'".format(duration["unit"]))
    return duration


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
    # FIXME: Some other databases (such as Zenobase) use tag instead of label, we should consider changing
    ALLOWED_FIELDS = {
        "timestamp": datetime,
        "count": int,
        "duration": timedelta,
        "label": str,
        "data": dict,
        "note": str,
    }

    def __init__(self, **kwargs: Any) -> None:
        for k, v in list(kwargs.items()):
            if k not in self.ALLOWED_FIELDS:
                kwargs.pop(k)
                logger.warning("Removed invalid field {} from Event kwargs: {}".format(k, kwargs))

        # FIXME: If I remove **kwargs here, tests start failing... Troubling.
        dict.__init__(self, **kwargs)

        # Set all the kwargs
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        if not self.timestamp:
            logger.warning("Event did not have a timestamp, using now as timestamp")
            # The typing.cast here was required for mypy to shut up, weird...
            self.timestamp = datetime.now(typing.cast(timezone, timezone.utc))

        self.verify()


    def verify(self):
        success = True
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
                if v != None: # FIXME: Optionals are invalidly defaulted to None, this is just a workaround so the logs don't get spammed
                    logger.error("Found value {} in field {} that was not of proper instance ({}, expected: {}). Event: {}"
                             .format(v, k, type(v), self.ALLOWED_FIELDS[k], self))
                invalid_keys.append(k)
        for k in invalid_keys:
            del self[k]

    def to_json_dict(self) -> dict:
        """Useful when sending data over the wire.
        Any mongodb interop should not use do this as it accepts datetimes."""
        data = self.copy()
        data["timestamp"] = self["timestamp"].astimezone().isoformat()
        if "duration" in data:
            data["duration"] = {"value": self["duration"].total_seconds(), "unit": "s"}
        return data

    def to_json_str(self) -> str:
        # TODO: Extend serializers in aw_core.json instead of using self.to_json_dict
        data = self.to_json_dict()
        return json.dumps(data)

    """
    def __getattr__(self, attr):
        # NOTE: Major downside of using this solution is getting zero ability to typecheck
        if attr in self.ALLOWED_FIELDS:
            return self[attr][0] if attr in self and self[attr] else None
        elif attr[:-1] in self.ALLOWED_FIELDS:
            # For pluralized versions of properties
            attr = attr[:-1]
            return self[attr] if attr in self else []
        else:
            return dict.__getattr__(self, attr)

    def __setattr__(self, attr, value):
        # NOTE: Major downside of using this solution is getting zero ability to typecheck
        if attr in self.ALLOWED_FIELDS:
            self[attr] = [value]
        elif attr[:-1] in self.ALLOWED_FIELDS:
            # For pluralized versions of setters
            attr = attr[:-1]
            self[attr] = value
        else:
            raise AttributeError
    """

    def _hasprop(self, propname):
        """Badly named, but basically checks if the underlying
        dict has a prop, and if it is a non-empty list"""
        return propname in self and self[propname]

    @property
    def label(self) -> Optional[str]:
        return self["label"] if self._hasprop("label") else None

    @label.setter
    def label(self, label: str) -> None:
        self["label"] = label

    @property
    def data(self) -> dict:
        return self["data"] if self._hasprop("data") else {}

    @data.setter
    def data(self, data: dict):
        self["data"] = data

    @property
    def timestamp(self) -> Optional[datetime]:
        return self["timestamp"] if self._hasprop("timestamp") else None

    @timestamp.setter
    def timestamp(self, timestamp: Union[str, datetime]) -> None:
        self["timestamp"] = _timestamp_parse(timestamp)

    @property
    def duration(self) -> timedelta:
        return self["duration"] if self._hasprop("duration") else timedelta(0)

    @duration.setter
    def duration(self, duration: Union[timedelta, dict]) -> None:
        self["duration"] = _duration_parse(duration)

    @property
    def count(self) -> Optional[int]:
        return self["count"] if self._hasprop("count") else None

    @count.setter
    def count(self, count: int) -> None:
        self["count"] = count
