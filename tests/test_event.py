from datetime import datetime, timedelta, timezone

from aw_core.models import Event

import unittest

valid_timestamp="1937-01-01T12:00:27.87+00:20"


class EventTest(unittest.TestCase):
    def test_create(self):
        Event(label=["test"], timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37))

    def test_to_json(self):
        e = Event(label=["test"], timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37))
        e.to_json_str()
