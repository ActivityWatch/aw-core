from datetime import datetime, timedelta, timezone
import json

from aw_core.models import Event

valid_timestamp="1937-01-01T12:00:27.87+00:20"

now = datetime.now(timezone.utc)
td1s = timedelta(seconds=1)


def test_create():
    Event(timestamp=now, duration=timedelta(hours=13, minutes=37), data={"key": "val"})
    Event(timestamp=valid_timestamp, duration=timedelta(hours=13, minutes=37), data={"key": "val"})


def test_json_serialization():
    e = Event(timestamp=now, duration=timedelta(hours=13, minutes=37), data={"key": "val"})
    assert e == Event(**json.loads(e.to_json_str()))


def test_sort():
    e1 = Event(timestamp=now)
    e2 = Event(timestamp=now + td1s)
    e_sorted = sorted([e2, e1])
    assert e_sorted[0] == e1
    assert e_sorted[1] == e2
