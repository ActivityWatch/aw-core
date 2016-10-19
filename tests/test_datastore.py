import logging
import random
from datetime import datetime, timedelta, timezone

from nose.tools import assert_equal, assert_dict_equal, assert_raises
from nose_parameterized import parameterized

from aw_core.models import Event
from aw_datastore import Datastore, get_storage_methods, get_storage_method_names

logging.basicConfig(level=logging.DEBUG)

# Useful when you just want some placeholder time in your events, saves typing
now = datetime.now(timezone.utc)


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
    """
    Tests fetching buckets
    """
    datastore.buckets()


@parameterized(param_datastore_objects())
def test_create_bucket(datastore):
    name = "A label/name for a test bucket"
    bid = "test-identifier"
    bucket = datastore.create_bucket(bucket_id=bid, type="test", client="test", hostname="test", name=name)
    try:
        assert_equal(name, bucket.metadata()["name"])
    finally:
        datastore.delete_bucket(bid)


@parameterized(param_datastore_objects())
def test_nonexistant_bucket(datastore):
    """
    Tests that a KeyError is raised if you request a non-existant bucket
    """
    with assert_raises(KeyError):
        datastore["I-do-not-exist"]


@parameterized(param_testing_buckets_cm())
def test_insert_one(bucket_cm):
    """
    Tests inserting one event into a bucket
    """
    with bucket_cm as bucket:
        l = len(bucket.get())
        event = Event(label="test", timestamp=now, duration=timedelta(seconds=1))
        bucket.insert(event)
        fetched_events = bucket.get()
        assert_equal(l + 1, len(fetched_events))
        assert_equal(Event, type(fetched_events[0]))
        assert_dict_equal(event, Event(**fetched_events[0]))


@parameterized(param_testing_buckets_cm())
def test_empty_bucket(bucket_cm):
    """
    Ensures empty buckets are empty
    """
    with bucket_cm as bucket:
        assert_equal(0, len(bucket.get()))


@parameterized(param_testing_buckets_cm())
def test_insert_many(bucket_cm):
    """
    Tests that you can insert many events at the same time to a bucket
    """
    with bucket_cm as bucket:
        events = (2 * [Event(label="test", timestamp=now, duration=timedelta(seconds=1))])
        bucket.insert(events)
        fetched_events = bucket.get()
        assert_equal(2, len(fetched_events))
        for e, fe in zip(events, fetched_events):
            assert_dict_equal(e, fe)


@parameterized(param_testing_buckets_cm())
def test_insert_badtype(bucket_cm):
    """
    Tests that you cannot insert non-event types into a bucket
    """
    with bucket_cm as bucket:
        l = len(bucket.get())
        badevent = 1
        handled = False
        try:
            bucket.insert(badevent)
        except TypeError:
            handled = True
        assert_equal(handled, True)
        assert_equal(l, len(bucket.get()))


@parameterized(param_testing_buckets_cm())
def test_get_ordered(bucket_cm):
    """
    Makes sure that received events are ordered
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = []
        for i in range(10):
            events.append(Event(label="test",
                                timestamp=now + timedelta(seconds=i)))
        random.shuffle(events)
        print(events)
        bucket.insert(events)
        fetched_events = bucket.get(-1)
        for i in range(eventcount - 1):
            print("1:" + fetched_events[i].to_json_str())
            print("2:" + fetched_events[i + 1].to_json_str())
            assert_equal(True, fetched_events[i].timestamp > fetched_events[i + 1].timestamp)


@parameterized(param_testing_buckets_cm())
def test_get_datefilter(bucket_cm):
    """
    Tests the datetimefilter when fetching events
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = []
        for i in range(10):
            events.append(Event(label="test",
                                timestamp=now + timedelta(seconds=i)))
        bucket.insert(events)

        # Starttime
        for i in range(eventcount):
            fetched_events = bucket.get(-1, starttime=events[i].timestamp)
            assert_equal(eventcount - i - 1, len(fetched_events))

        # Endtime
        for i in range(eventcount):
            fetched_events = bucket.get(-1, endtime=events[i].timestamp)
            assert_equal(i, len(fetched_events))

        # Both
        for i in range(eventcount):
            for j in range(i + 1, eventcount):
                fetched_events = bucket.get(starttime=events[i].timestamp, endtime=events[j].timestamp)
                assert_equal(j - i - 1, len(fetched_events))


@parameterized(param_testing_buckets_cm())
def test_insert_invalid(bucket_cm):
    with bucket_cm as bucket:
        event = "not a real event"
        with assert_raises(TypeError):
            bucket.insert(event)


@parameterized(param_testing_buckets_cm())
def test_replace_last(bucket_cm):
    """
    Tests the replace last event in bucket functionality
    """
    with bucket_cm as bucket:
        # Create first event
        event1 = Event(label="test1", timestamp=now)
        bucket.insert(event1)
        eventcount = len(bucket.get(-1))
        # Create second event to replace with the first one
        event2 = Event(label="test2",
                       timestamp=now + timedelta(seconds=1))
        bucket.replace_last(event2)
        # Assert length and content
        assert_equal(eventcount, len(bucket.get(-1)))
        assert_dict_equal(event2, bucket.get(-1)[-1])


@parameterized(param_testing_buckets_cm())
def test_limit(bucket_cm):
    """
    Tests setting the result limit when fetching events
    """
    with bucket_cm as bucket:
        for i in range(5):
            bucket.insert(Event(label="test", timestamp=now))

        print(len(bucket.get(limit=1)))
        assert_equal(1, len(bucket.get(limit=1)))
        assert_equal(5, len(bucket.get(limit=5)))


@parameterized(param_testing_buckets_cm())
def test_get_metadata(bucket_cm):
    """
    Tests the get_metadata function
    """
    with bucket_cm as bucket:
        print(bucket.ds.storage_strategy)
        metadata = bucket.metadata()
        print(metadata)
        assert_equal(True, 'created' in metadata)
        assert_equal(True, 'client' in metadata)
        assert_equal(True, 'hostname' in metadata)
        assert_equal(True, 'id' in metadata)
        assert_equal(True, 'name' in metadata)
        assert_equal(True, 'type' in metadata)
