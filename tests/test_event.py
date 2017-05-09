from datetime import datetime, timedelta, timezone
import json
import logging

from aw_core.models import Event

import unittest

valid_timestamp="1937-01-01T12:00:27.87+00:20"


class EventTest(unittest.TestCase):
    def test_create(self):
        Event(label="test", timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37), data={"key": "val"})

    def test_invalid_type(self):
        e = Event(duration="test", timestamp=datetime.now(timezone.utc))
        # Field containing invalid type should be dropped
        assert "duration" not in e
        assert e.data == {}
        e.data = "a"
        e.verify()
        assert e.data == {}

    def test_invalid_field(self):
        e = Event(timestamp=datetime.now(timezone.utc), invalid_field="What am I doing here?")
        # Invalid field should be dropped
        assert "invalid_field" not in e

    def test_json_serialization(self):
        e = Event(timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37), data={"key": "val"})
        json_str = e.to_json_str()
        logging.error(json_str)
        assert e == Event(**json.loads(json_str))
