import os
import json

from jsonschema import validate as _validate, FormatChecker
from jsonschema.exceptions import ValidationError

import unittest

# TODO: Include date-time format
# https://python-jsonschema.readthedocs.io/en/latest/validate/#jsonschema.FormatChecker

# The default FormatChecker, uses the date-time checker
fc = FormatChecker(["date-time"])

class EventTest(unittest.TestCase):
    def setUp(self):
        testdir = os.path.dirname(os.path.realpath(__file__))
        with open(testdir + "/../schemas/event.json") as f:
            self.schema = json.load(f)

    def validate(self, obj):
        _validate(obj, self.schema, format_checker=fc)

    def test_label(self):
        self.validate({"label": ["test-label"]})
        self.validate({
            "timestamp": ["1937-01-01T12:00:27.87+00:20"],
            "label": ["test", "test2"]
        })

    def test_timestamp(self):
        self.validate({"timestamp": ["1937-01-01T12:00:27.87+00:20"]})
        self.validate({"timestamp": 2 * ["1937-01-01T12:00:27.87+00:20"]})

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

    def test_duration_invalid_unit(self):
        with self.assertRaises(ValidationError):
            self.validate({"duration": {"value": 1000, "unit": "rainbows"}})


if __name__ == "__main__":
    unittest.main()
