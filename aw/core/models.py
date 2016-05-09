import json
import logging

import threading
from datetime import datetime, timedelta

from typing import Iterable, List, Set, Tuple, Union, Sequence

logger = logging.getLogger("aw.client.models")

class BaseEvent(dict):
    ALLOWED_FIELDS = {
        # Required
        "type": str,

        # Metadata
        "_id": str,
        "session": str,
        "stored_at": datetime,
    }

    def __init__(self,
                 event_type: str,
                 **kwargs):
        dict.__init__(self)
        self["type"] = event_type   # type: str
        for k, v in kwargs.items():
            if k not in self.ALLOWED_FIELDS:
                logger.warning("Field {} not allowed, event: {}".format(k, kwargs))
            elif not isinstance(v, self.ALLOWED_FIELDS[k]):
                logger.warning("Field {} was not of proper instance, event: {}".format(k, kwargs))
        self.update(kwargs)

    @classmethod
    def from_json_dict(cls, d) -> dict:
        # TODO: Is this needed, or should the constructor deal with this behavior?
        raise NotImplementedError

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


class SubEvent(BaseEvent):
    # Not sure if this will be used, will probably be removed for simplicity
    pass


class Event(BaseEvent):
    """
    Used to represents an activity/event.
    """

    def __init__(self,
                 timestamp: Union[datetime, Sequence[datetime]]=None,
                 event_type="event",
                 **kwargs):
        # FIXME: tags and label have similar/same meaning, pick one
        self.ALLOWED_FIELDS.update({
            "timestamp": Union[datetime, Sequence[datetime]],

            "label": Union[str, Sequence[str]],
            "note": str,

            # Used with session-initializer
            "settings": dict,
        })

        if not timestamp:
            logger.warning("Event did not have a timestamp, using now as timestamp.")
            timestamp = datetime.now()

        BaseEvent.__init__(self, event_type, timestamp=timestamp, **kwargs)


class Activity(Event):
    """
    Used to represents an activity, an event which always has a start and end-time.
    """

    def __init__(self, timestamp: Tuple[datetime, datetime] = None, **kwargs):
        if not timestamp or len(timestamp) != 2:
            raise TypeError("Activities require start and end-times, cannot be inferred")
        Event.__init__(self, **kwargs)

    @property
    def duration(self) -> timedelta:
        return self["timestamp"][1] - self["timestamp"][0]


class Window(Activity):
    def __init__(self, timestamp: Tuple[datetime, datetime] = None, **kwargs):
        self.ALLOWED_FIELDS.update({
            # Used for windows
            "id": str,
            "role": str,
            "command": str,
            "active": bool,
            "desktop": str,
        })

        Activity.__init__(self, timestamp=timestamp, **kwargs)
