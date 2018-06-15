from datetime import datetime, timedelta, timezone
import json
import logging

import pytest

from aw_core.models import Event

valid_timestamp="1937-01-01T12:00:27.87+00:20"


def test_create():
    Event(timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37), data={"key": "val"})


def test_json_serialization():
    e = Event(timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37), data={"key": "val"})
    json_str = e.to_json_str()
    logging.error(json_str)
    assert e == Event(**json.loads(json_str))


def test_set_invalid_duration():
    e = Event()
    with pytest.raises(TypeError):
        e.duration = "12"
