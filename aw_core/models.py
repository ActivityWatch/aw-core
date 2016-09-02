import json
import logging

from datetime import datetime, timedelta, timezone

from typing import Any, List, Union, Optional

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
    # FIXME: tags and label have similar/same meaning, pick one
    # FIXME: Some other databases (such as Zenobase) use tag instead of label, we should consider changing
    ALLOWED_FIELDS = {
        "timestamp": datetime,
        "count": int,
        "duration": timedelta,
        "label": str,
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
            # Use the pluralized setter when kwarg value is a list
            if isinstance(kwargs[arg], list):
                setattr(self, arg + "s", kwargs[arg])
            else:
                setattr(self, arg, kwargs[arg])

        if not self.timestamp:
            logger.warning("Event did not have a timestamp, using now as timestamp")
            self.timestamp = datetime.now(timezone.utc)

        self._drop_invalid_types()
        self._drop_empty_keys()

    def _drop_invalid_types(self):
        # Check for invalid types
        for k in self.keys():
            for i, v in reversed(list(enumerate(self[k]))):
                if not isinstance(v, self.ALLOWED_FIELDS[k]):
                    logger.error("Found value {} in field {} that was not of proper instance ({}, expected: {}). Event: {}".format(v, k, type(v), self.ALLOWED_FIELDS[k], self))
                    self[k].pop(i)

    def _drop_empty_keys(self):
        # Drop dict keys whose values are only an empty list
        for k in list(self.keys()):
            if not self[k]:
                del self[k]

    def to_json_dict(self) -> dict:
        """Useful when sending data over the wire.
        Any mongodb interop should not use do this as it accepts datetimes."""
        data = self.copy()
        if "timestamp" in data:
            data["timestamp"] = [dt.astimezone().isoformat() for dt in data["timestamp"]]
        if "duration" in data:
            data["duration"] = [{"value": td.total_seconds(), "unit": "s"} for td in data["duration"]]
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
        return self["label"][0] if self._hasprop("label") else None

    @label.setter
    def label(self, label: str) -> None:
        self["label"] = label

    @property
    def timestamp(self) -> Optional[datetime]:
        return self["timestamp"][0] if self._hasprop("timestamp") else None

    @timestamp.setter
    def timestamp(self, timestamp: Union[str, datetime]) -> None:
        self["timestamp"] = [_timestamp_parse(timestamp)]

    @property
    def duration(self) -> Optional[timedelta]:
        return self["duration"][0] if self._hasprop("duration") else None

    @duration.setter
    def duration(self, duration: Union[timedelta, dict]) -> None:
        self["duration"] = [_duration_parse(duration)]

    @property
    def count(self) -> Optional[int]:
        return self["count"][0] if self._hasprop("count") else None

    @count.setter
    def count(self, count: int) -> None:
        self["count"] = [count]

    """
    Below comes all the plural-versions of the above
    """

    @property
    def labels(self) -> List[int]:
        return self["label"] if "label" in self else []

    @labels.setter
    def labels(self, labels: List[str]) -> None:
        self["label"] = labels

    @property
    def timestamps(self) -> List[datetime]:
        return self["timestamp"] if "timestamp" in self else []

    @timestamps.setter
    def timestamps(self, timestamps: List[Union[str, datetime]]) -> None:
        self["timestamp"] = list(map(_timestamp_parse, timestamps))

    @property
    def durations(self) -> List[timedelta]:
        return self["duration"] if "duration" in self else []

    @durations.setter
    def durations(self, durations: List[Union[dict, timedelta]]) -> None:
        self["duration"] = list(map(_duration_parse, durations))

    @property
    def counts(self) -> List[int]:
        return self["count"] if "count" in self else []

    @counts.setter
    def counts(self, counts: List[int]) -> None:
        self["count"] = counts
