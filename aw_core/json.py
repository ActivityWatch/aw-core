"""
An alternative to the json module in the stdlib.
This one serializes datetimes and timedeltas.
"""

import json
from datetime import datetime, timedelta


def _datetime_to_str(dt: datetime):
    return dt.isoformat()


def _timedelta_to_dict(td: timedelta):
    return {
        "value": td.total_seconds(),
        "unit": "s"
    }


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return _datetime_to_str(o)
        elif isinstance(o, timedelta):
            return json.JSONEncoder.default(self, _timedelta_to_dict(o))
        else:
            return json.JSONEncoder.default(self, o)


def loads(s: str):
    return json.loads(s)


def dumps(json_obj):
    return json.dumps(json_obj, cls=JSONEncoder)
