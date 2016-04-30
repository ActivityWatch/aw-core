import json
import logging

import threading
from datetime import datetime, timedelta

from typing import Iterable, List, Set, Tuple, Union, Sequence


class BaseEvent(dict):
    ALLOWED_FIELDS = [
        # Required
        "type",

        # Metadata
        "_id",
        "session",
        "stored_at",
    ]

    def __init__(self,
                 event_type: str,
                 **kwargs):
        dict.__init__(self)
        self["type"] = event_type   # type: str
        for k, v in kwargs.items():
            if k not in self.ALLOWED_FIELDS:
                print("Field {} not allowed".format(k))
        self.update(kwargs)

    def from_json_dict(self) -> dict:
        data = self.copy()
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = v.isoformat()
        return data

    def to_json_dict(self) -> dict:
        """Useful when sending data over the wire.
        Any mongodb interop should not use do this as it accepts datetimes."""
        data = self.copy()
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = v.isoformat()
            elif isinstance(v, Sequence) and isinstance(v[0], datetime):
                data[k] = list(map(lambda dt: dt.isoformat(), v))
        return data

    def to_json_str(self) -> str:
        data = self.to_json_dict()
        return json.dumps(data)


class SubEvent(dict):
    pass


class Event(BaseEvent):
    """
    Used to represents an activity/event.
    """

    def __init__(self,
                 timestamp: Union[datetime, Sequence[datetime]],
                 event_type="event",
                 **kwargs):
        # FIXME: tags and label have similar/same meaning, pick one
        self.ALLOWED_FIELDS.extend([
            "timestamp",

            "tag",
            "label",
            "note",
            "name",

            # Used with session-initializer
            "settings",

        ])

        BaseEvent.__init__(self, event_type, timestamp=timestamp, **kwargs)


class Activity(Event):
    """
    Used to represents an activity, an event which always has a start and end-time.
    """

    def __init__(self, timestamp: Tuple[datetime, datetime], **kwargs):
        Event.__init__(self, timestamp=timestamp, **kwargs)

    @property
    def duration(self) -> timedelta:
        return self["timestamps"][1] - self["timestamps"][0]


class Window(Activity):
    def __init__(self, timestamp: Tuple[datetime, datetime], **kwargs):
        self.ALLOWED_FIELDS.extend([
            # Used for windows
            "id",
            "role",
            "command",
            "active",
            "desktop",
        ])

        Activity.__init__(self, timestamp=timestamp, **kwargs)
