import logging
import random
from datetime import datetime, timedelta, timezone

from nose.tools import assert_equal, assert_dict_equal, assert_raises
from nose_parameterized import parameterized

from aw_core.models import Event
from aw_datastore import Datastore, get_storage_methods, get_storage_method_names

logging.basicConfig(level=logging.DEBUG)


class TempTestBucket:
    """Context manager for creating a test bucket"""

    def __init__(self, datastore):
        self.ds = datastore
        self.bucket_id = "test-{}".format(random.randint(0, 10**4))

    def __enter__(self):
        self.ds.create_bucket(bucket_id=self.bucket_id, type="test", client="test", hostname="test")
        return self.ds[self.bucket_id]

    def __exit__(self, *_):
        self.ds.delete_bucket(bucket_id=self.bucket_id)

    def __repr__(self):
        return "<TempTestBucket using {}>".format(self.ds.storage_strategy.__class__.__name__)


def param_datastore_objects():
    return [[Datastore(storage_strategy=strategy, testing=True)]
            for strategy in get_storage_methods()]


def param_testing_buckets_cm():
    datastores = [Datastore(storage_strategy=strategy, testing=True)
                  for strategy in get_storage_methods()]
    return [[TempTestBucket(ds)] for ds in datastores]


def test_get_storage_method_names():
    assert get_storage_method_names()


@parameterized(param_datastore_objects())
def test_get_buckets(datastore):
    datastore.buckets()


@parameterized(param_datastore_objects())
def test_nonexistant_bucket(datastore):
    with assert_raises(KeyError):
        datastore["I-do-not-exist"]


@parameterized(param_testing_buckets_cm())
def test_insert_one(bucket_cm):
    with bucket_cm as bucket:
        l = len(bucket.get())
        event = Event(**{"label": "test", "timestamp": datetime.now(timezone.utc)})
        bucket.insert(event)
        assert_equal(l + 1, len(bucket.get()))
        assert_dict_equal(event, Event(**bucket.get(1)[0]))


@parameterized(param_testing_buckets_cm())
def test_insert_many(bucket_cm):
    with bucket_cm as bucket:
        events = (2 * [Event(**{"label": "test", "timestamp": datetime.now(timezone.utc)})])
        bucket.insert(events)
        assert_equal(2, len(bucket.get()))
        fetched_events = bucket.get(2)
        for i in range(2):
            assert_dict_equal(events[i], Event(**fetched_events[i]))


@parameterized(param_testing_buckets_cm())
def test_get_datefilter(bucket_cm):
    with bucket_cm as bucket:
        eventcount = 10
        events = []
        for i in range(10):
            events.append(Event(**{"label": "test", "timestamp": datetime.now(timezone.utc)+timedelta(seconds=i)}))
        bucket.insert(events)
        for i in range(eventcount):
            fetched_events = bucket.get(-1, starttime=events[i]["timestamp"])
            assert_equal(eventcount-i-1, len(fetched_events))
        for i in range(eventcount):
            fetched_events = bucket.get(-1, endtime=events[i]["timestamp"])
            assert_equal(i, len(fetched_events))


@parameterized(param_testing_buckets_cm())
def test_replace_last(bucket_cm):
    with bucket_cm as bucket:
        # Create first event
        event1 = Event(**{"label": "test1", "timestamp": datetime.now(timezone.utc)})
        bucket.insert(event1)
        eventcount = len(bucket.get(-1))
        # Create second event to replace with the first one
        event2 = Event(**{"label": "test2", "timestamp": datetime.now(timezone.utc)})
        bucket.replace_last(event2)
        # Assert length and content
        assert_equal(eventcount, len(bucket.get(-1)))
        assert_dict_equal(event2, Event(**bucket.get(-1)[-1]))



@parameterized(param_testing_buckets_cm())
def test_limit(bucket_cm):
    with bucket_cm as bucket:
        for i in range(5):
            bucket.insert(Event(**{"label": "test"}))

        print(len(bucket.get(limit=1)))
        assert_equal(1, len(bucket.get(limit=1)))
        assert_equal(5, len(bucket.get(limit=5)))


@parameterized(param_testing_buckets_cm())
def test_get_metadata(bucket_cm):
    with bucket_cm as bucket:
        bucket.metadata()
