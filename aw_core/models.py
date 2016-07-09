import json
import logging

import threading
from datetime import datetime, timedelta

from typing import Iterable, List, Set, Tuple, Union, Sequence

logger = logging.getLogger("aw_client.models")

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
            # Currently causes issues due to not being allowed on Unions
            # https://github.com/python/typing/issues/62
            #elif not issubclass(v, self.ALLOWED_FIELDS[k]):
            #    logger.warning("Field {} was not of proper instance, event: {}".format(k, kwargs))
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


class Event(BaseEvent):
    """
    Used to represents an activity/event.
    """

    def __init__(self,
                 timestamp: Union[datetime, Sequence[datetime]]=None,
                 event_type="event",
                 **kwargs):
        # FIXME: tags and label have similar/same meaning, pick one
        # FIXME: Some other databases (such as Zenobase) use tag instead of label, we should consider changing
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

# TODO: This will likely have to go, we need to solve the problem this tries to solve
#       in some other way before we do however.
class Window(Event):
    def __init__(self, timestamp: Tuple[datetime, datetime] = None, **kwargs):
        self.ALLOWED_FIELDS.update({
            # Used for windows
            "id": str,
            "role": str,
            "command": str,
            "active": bool,
            "desktop": str,
        })

        Event.__init__(self, timestamp=timestamp, **kwargs)
