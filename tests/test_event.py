from datetime import datetime, timedelta, timezone
import json
import logging

from aw_core.models import Event

import unittest

valid_timestamp="1937-01-01T12:00:27.87+00:20"


class EventTest(unittest.TestCase):
    def test_create(self):
        Event(timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37), data={"key": "val"})

    def test_json_serialization(self):
        e = Event(timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37), data={"key": "val"})
        json_str = e.to_json_str()
        logging.error(json_str)
        assert e == Event(**json.loads(json_str))
