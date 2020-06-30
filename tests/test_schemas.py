import unittest

from jsonschema import validate as _validate, FormatChecker
from jsonschema.exceptions import ValidationError

from aw_core import schema
from aw_core.models import Event

# TODO: Include date-time format
# https://python-jsonschema.readthedocs.io/en/latest/validate/#jsonschema.FormatChecker

# The default FormatChecker, uses the date-time checker
fc = FormatChecker(["date-time"])

valid_timestamp = "1937-01-01T12:00:27.87+00:20"


event_schema = schema.get_json_schema("event")


class EventSchemaTest(unittest.TestCase):
    def setUp(self):
        self.schema = event_schema

    def validate(self, obj):
        _validate(obj, self.schema, format_checker=fc)

    def test_event(self):
        event = Event(timestamp=valid_timestamp, data={"label": "test"})
        self.validate(event.to_json_dict())

    def test_data(self):
        self.validate(
            {"timestamp": valid_timestamp, "data": {"label": "test", "number": 1.1}}
        )

    def test_timestamp(self):
        self.validate({"timestamp": valid_timestamp})

    def test_timestamp_invalid_string(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": ""})
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": "123"})

    def test_timestamp_invalid_number(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": 2})

    def test_duration(self):
        self.validate({"timestamp": valid_timestamp, "duration": 1000})
        self.validate({"timestamp": valid_timestamp, "duration": 3.13})

    def test_duration_invalid_string(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": valid_timestamp, "duration": "not a number"})


if __name__ == "__main__":
    unittest.main()
