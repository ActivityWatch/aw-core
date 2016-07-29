import logging
import unittest
from datetime import datetime

from nose_parameterized import parameterized

from aw_core.models import Event
from aw_datastore import Datastore, get_storage_methods

logging.basicConfig(level=logging.DEBUG)


def get_buckets(strategies=get_storage_methods()):
    buckets = []
    for strategy in strategies:
        buckets.append(Datastore(storage_strategy=strategy)["test"])
    return buckets


def get_param_args():
    return [(bucket.ds.storage_strategy.__class__.__name__, bucket)
            for bucket in get_buckets()]


class DatastoreTest(unittest.TestCase):
    @parameterized.expand(get_param_args())
    def test_insert_one(self, _, bucket):
        l = len(bucket.get())
        bucket.insert(Event(**{"label": "test"}))
        self.assertEqual(l + 1, len(bucket.get()))

    @parameterized.expand(get_param_args())
    def test_insert_many(self, _, bucket):
        l = len(bucket.get())
        bucket.insert([Event(**{"label": "test"}), Event(**{"label": "test2"})])
        self.assertEqual(l + 2, len(bucket.get()))

    @parameterized.expand(get_param_args())
    def test_limit(self, _, bucket):
        for i in range(5):
            bucket.insert(Event(**{"label": "test"}))

        print(len(bucket.get(limit=1)))
        self.assertEqual(1, len(bucket.get(limit=1)))
        self.assertEqual(5, len(bucket.get(limit=5)))
