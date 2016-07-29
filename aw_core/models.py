import json
import logging

import threading
from datetime import datetime, timedelta

from typing import Iterable, List, Set, Tuple, Union

import iso8601
import pytz

logger = logging.getLogger("aw_client.models")


class Event(dict):
    """
    Used to represents an event.
    """

    # TODO: Use JSONSchema as specification
    # FIXME: tags and label have similar/same meaning, pick one
    # FIXME: Some other databases (such as Zenobase) use tag instead of label, we should consider changing
    ALLOWED_FIELDS = {
        "timestamp": datetime,
        "label": str,
        "note": str,

        #"stored_at": datetime,
    }

    def __init__(self, **kwargs):
        dict.__init__(self)

        if "timestamp" not in kwargs:
            logger.warning("Event did not have a timestamp, using now as timestamp")
            kwargs["timestamp"] = [datetime.now(pytz.utc)]

        for k, v in kwargs.items():
            if k not in self.ALLOWED_FIELDS:
                logger.warning("Field {} not allowed, event: {}".format(k, kwargs))

            if not isinstance(v, List):
                kwargs[k] = [v]

        if "timestamp" in kwargs:
            kwargs["timestamp"] = [iso8601.parse_date(ts) if isinstance(ts, str) else ts for ts in kwargs["timestamp"]]

        for i, dt in enumerate(kwargs["timestamp"]):
            if not dt.tzinfo:
                logger.warning("timestamp without timezone found, using UTC: {}".format(dt))
                kwargs["timestamp"][i] = dt.replace(tzinfo=pytz.utc)
        # Needed? All timestamps should be iso8601 so ought to always contain timezone.
        # kwargs["timestamp"] = [dt if dt.tzinfo else dt.replace(tzinfo=pytz.utc) for dt in kwargs["timestamp"]]

        for k, v in kwargs.items():
            for value in kwargs[k]:
                if not (k in self.ALLOWED_FIELDS) or not isinstance(value, self.ALLOWED_FIELDS[k]):
                    logger.error("Found value '{}' in field {} that was not of proper instance, discarding (event: {})".format(value, k, kwargs))

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
                data[k] = [v.astimezone().isoformat()]
            elif isinstance(v, List) and isinstance(v[0], datetime):
                data[k] = list(map(lambda dt: dt.astimezone().isoformat(), v))
        return data

    def to_json_str(self) -> str:
        data = self.to_json_dict()
        return json.dumps(data)
