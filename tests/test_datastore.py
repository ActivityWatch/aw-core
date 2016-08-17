import logging
import unittest
from datetime import datetime

from nose_parameterized import parameterized

from aw_core.models import Event
from aw_datastore import Datastore, get_storage_methods

logging.basicConfig(level=logging.DEBUG)


def testing_buckets(strategies=get_storage_methods()):
    buckets = []
    for strategy in strategies:
        datastore = Datastore(storage_strategy=strategy)
        datastore.create_bucket(bucket_id="test", type="test", client="test", hostname="test", created="testdate")
        buckets.append(datastore["test"])
    return buckets


def param_testing_buckets():
    return [(bucket.ds.storage_strategy.__class__.__name__, bucket)
            for bucket in testing_buckets()]


def param_datastore_objects():
    return [(Datastore(storage_strategy=strategy), )
            for strategy in get_storage_methods()[1:]]


class DatastoreTest(unittest.TestCase):
    @parameterized.expand(param_datastore_objects())
    def test_get_buckets(self, datastore):
        datastore.buckets()

    @parameterized.expand(param_testing_buckets())
    def test_insert_one(self, _, bucket):
        l = len(bucket.get())
        bucket.insert(Event(**{"label": "test"}))
        self.assertEqual(l + 1, len(bucket.get()))


    @parameterized.expand(param_testing_buckets())
    def test_get_metadata(self, _, bucket):
        bucket.metadata()

    @parameterized.expand(param_testing_buckets())
    def test_insert_many(self, _, bucket):
        l = len(bucket.get())
        bucket.insert([Event(**{"label": "test"}), Event(**{"label": "test2"})])
        self.assertEqual(l + 2, len(bucket.get()))

    @parameterized.expand(param_testing_buckets())
    def test_limit(self, _, bucket):
        for i in range(5):
            bucket.insert(Event(**{"label": "test"}))

        print(len(bucket.get(limit=1)))
        self.assertEqual(1, len(bucket.get(limit=1)))
        self.assertEqual(5, len(bucket.get(limit=5)))
