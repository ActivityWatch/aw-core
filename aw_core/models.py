import json
import logging

from datetime import datetime, timedelta, timezone

from typing import List

import iso8601

logger = logging.getLogger("aw.client.models")


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
        "duration": dict,
        "label": str,
        "note": str,

        #"stored_at": datetime,
    }

    def __init__(self, **kwargs):
        dict.__init__(self)

        if "timestamp" not in kwargs:
            logger.warning("Event did not have a timestamp, using now as timestamp")
            kwargs["timestamp"] = [datetime.now(timezone.utc)]
        invalid_keys = []
        for k, v in kwargs.items():
            if k not in self.ALLOWED_FIELDS:
                logger.warning("Field {} not allowed, event: {}".format(k, kwargs))
                invalid_keys.append(k)

            elif not isinstance(v, list):
                kwargs[k] = [v]
        for k in invalid_keys:
            del kwargs[k]

        if "timestamp" in kwargs:
            kwargs["timestamp"] = [iso8601.parse_date(ts)
                                   if isinstance(ts, str) else ts
                                   for ts in kwargs["timestamp"]]

        if "duration" in kwargs:
            kwargs["duration"] = [{"value": td.total_seconds(), "unit": "s"}
                                  if isinstance(td, timedelta) else td
                                  for td in kwargs["duration"]]

        for i, dt in enumerate(kwargs["timestamp"]):
            if not dt.tzinfo:
                logger.warning("timestamp without timezone found, using UTC: {}".format(dt))
                kwargs["timestamp"][i] = dt.replace(tzinfo=timezone.utc)
        # Needed? All timestamps should be iso8601 so ought to always contain timezone.
        # kwargs["timestamp"] = [dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc) for dt in kwargs["timestamp"]]

        for k, v in kwargs.items():
            for value in kwargs[k]:
                if not isinstance(value, self.ALLOWED_FIELDS[k]):
                    logger.error("Found value {} in field {} that was not of proper instance ({}). Event: {}".format(value, k, self.ALLOWED_FIELDS[k], kwargs))

        self.update(kwargs)

    @classmethod
    def from_json_dict(cls, d) -> dict:
        # TODO: Is this needed, or should the constructor deal with this behavior?
        raise NotImplementedError

    def to_json_dict(self) -> dict:
        """Useful when sending data over the wire.
        Any mongodb interop should not use do this as it accepts datetimes."""
        data = self.copy()
        data["timestamp"] = [dt.astimezone().isoformat() for dt in data["timestamp"]]
        return data

    def to_json_str(self) -> str:
        data = self.to_json_dict()
        return json.dumps(data)
