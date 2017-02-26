import unittest

from jsonschema import validate as _validate, FormatChecker
from jsonschema.exceptions import ValidationError

from aw_core import schema

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

    def test_label(self):
        self.validate({"label": ["test-label"]})
        self.validate({
            "timestamp": [valid_timestamp],
            "label": ["test", "test2"]
        })

    def test_count(self):
        self.validate({"timestamp": [valid_timestamp], "count": [10]})

    def test_timestamp(self):
        self.validate({"timestamp": [valid_timestamp]})
        self.validate({"timestamp": 2 * [valid_timestamp]})

    def test_timestamp_invalid_string(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": ""})
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": "123"})

    def test_timestamp_invalid_array(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": [""]})
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": ["123"]})

    def test_timestamp_invalid_number(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": 2})

    def test_timestamp_empty_array(self):
        with self.assertRaises(ValidationError):
            self.validate({"timestamp": []})

    def test_duration(self):
        self.validate({"duration": [{"value": 1000, "unit": "s"}]})
        self.validate({"duration": [{"value": 3.13, "unit": "s", "label": "Bad approximation of pi"}]})

    def test_duration_invalid_unit(self):
        with self.assertRaises(ValidationError):
            self.validate({"duration": {"value": 1000, "unit": "rainbows"}})


if __name__ == "__main__":
    unittest.main()
