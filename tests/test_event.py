from datetime import datetime, timedelta, timezone

from aw_core.models import Event

import unittest

valid_timestamp="1937-01-01T12:00:27.87+00:20"


class EventTest(unittest.TestCase):
    def test_create(self):
        Event(label=["test"], timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37))

    def test_invalid_type(self):
        e = Event(label=[1], timestamp=datetime.now(timezone.utc))
        # Field containing invalid type should be dropped
        assert "label" not in e

    def test_invalid_field(self):
        e = Event(label=["test"], timestamp=datetime.now(timezone.utc), invalid_field="What am I doing here?")
        # Invalid field should be dropped
        assert "invalid_field" not in e

    def test_to_json(self):
        e = Event(label=["test"], timestamp=datetime.now(timezone.utc), duration=timedelta(hours=13, minutes=37))
        e.to_json_str()
