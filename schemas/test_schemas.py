import json

from jsonschema import validate
from jsonschema.exceptions import ValidationError

import unittest

class ExampleTest(unittest.TestCase):
    def setUp(self):
        self.schema = json.load(open("example.json"))

    def test_timestamp(self):
        validate({"timestamp": ["1937-01-01T12:00:27.87+00:20"]}, self.schema)
        validate({"timestamp": ["1937-01-01T12:00:27.87+00:20"]*2}, self.schema)

    def test_timestamp_missing(self):
        with self.assertRaises(ValidationError):
            validate({"label": "test-label"}, self.schema)

    # FIXME: Validates when it shouldn't
    # IMPORTANT: This means that no date-time validation happens within the array
    #def test_timestamp_invalid_array(self):
    #    with self.assertRaises(ValidationError):
    #        validate({"timestamp": [""]}, self.schema)

    def test_timestamp_empty_array(self):
        with self.assertRaises(ValidationError):
            validate({"timestamp": []}, self.schema)

    def test_label(self):
        validate({"label": "test-label", "timestamp": ["1937-01-01T12:00:27.87+00:20"]}, self.schema)


if __name__ == "__main__":
    unittest.main()
