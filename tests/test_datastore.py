import logging
import random
import iso8601
from datetime import datetime, timedelta, timezone
import iso8601

import pytest

from . import context

from aw_core.models import Event
from aw_datastore import get_storage_methods

from .utils import param_datastore_objects, param_testing_buckets_cm


logging.basicConfig(level=logging.DEBUG)

# Useful when you just want some placeholder time in your events, saves typing
now = datetime.now(timezone.utc)


def test_get_storage_methods():
    assert get_storage_methods()


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_get_buckets(datastore):
    """
    Tests fetching buckets
    """
    datastore.buckets()


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_create_bucket(datastore):
    name = "A label/name for a test bucket"
    bid = "test-identifier"
    try:
        bucket = datastore.create_bucket(
            bucket_id=bid,
            type="testtype",
            client="testclient",
            hostname="testhost",
            name=name,
            created=now,
        )
        assert bid == bucket.metadata()["id"]
        assert name == bucket.metadata()["name"]
        assert "testtype" == bucket.metadata()["type"]
        assert "testclient" == bucket.metadata()["client"]
        assert "testhost" == bucket.metadata()["hostname"]
        assert now == iso8601.parse_date(bucket.metadata()["created"])
        assert bid in datastore.buckets()
    finally:
        datastore.delete_bucket(bid)
    assert bid not in datastore.buckets()


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_delete_bucket(datastore):
    bid = "test"
    datastore.create_bucket(
        bucket_id=bid, type="test", client="test", hostname="test", name="test"
    )
    datastore.delete_bucket(bid)
    assert bid not in datastore.buckets()
    with pytest.raises(Exception):
        datastore.delete_bucket(bid)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_nonexistant_bucket(datastore):
    """
    Tests that a KeyError is raised if you request a non-existant bucket
    """
    with pytest.raises(KeyError):
        datastore["I-do-not-exist"]


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_insert_one(bucket_cm):
    """
    Tests inserting one event into a bucket
    """
    with bucket_cm as bucket:
        l = len(bucket.get())
        event = Event(timestamp=now, duration=timedelta(seconds=1), data={"key": "val"})
        bucket.insert(event)
        fetched_events = bucket.get()
        assert l + 1 == len(fetched_events)
        assert isinstance(fetched_events[0], Event)
        assert event == fetched_events[0]
        logging.info(event)
        logging.info(fetched_events[0].to_json_str())


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_empty_bucket(bucket_cm):
    """
    Ensures empty buckets are empty
    """
    with bucket_cm as bucket:
        assert 0 == len(bucket.get())


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_insert_many(bucket_cm):
    """
    Tests that you can insert many events at the same time to a bucket
    """
    num_events = 5000
    with bucket_cm as bucket:
        events = num_events * [
            Event(timestamp=now, duration=timedelta(seconds=1), data={"key": "val"})
        ]
        bucket.insert(events)
        fetched_events = bucket.get(limit=-1)
        assert num_events == len(fetched_events)
        for e, fe in zip(events, fetched_events):
            assert e == fe


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_delete(bucket_cm):
    """
    Tests deleting single events
    """
    num_events = 10
    with bucket_cm as bucket:
        events = num_events * [
            Event(timestamp=now, duration=timedelta(seconds=1), data={"key": "val"})
        ]
        bucket.insert(events)

        fetched_events = bucket.get(limit=-1)
        print(fetched_events[0])
        assert num_events == len(fetched_events)

        # Test deleting event
        assert bucket.delete(fetched_events[0]["id"])

        # Test deleting non-existant event
        # FIXME: Doesn't work due to lazy evaluation in SqliteDatastore
        # assert not bucket.delete(fetched_events[0]["id"])

        fetched_events = bucket.get(limit=-1)
        assert num_events - 1 == len(fetched_events)


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_insert_badtype(bucket_cm):
    """
    Tests that you cannot insert non-event types into a bucket
    """
    with bucket_cm as bucket:
        bucket_len = len(bucket.get())
        badevent = 1
        with pytest.raises(TypeError):
            bucket.insert(badevent)
        assert bucket_len == len(bucket.get())


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_ordered(bucket_cm):
    """
    Makes sure that received events are ordered
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = []
        for i in range(10):
            events.append(Event(timestamp=now + timedelta(seconds=i)))
        random.shuffle(events)
        print(events)
        bucket.insert(events)
        fetched_events = bucket.get(-1)
        for i in range(eventcount - 1):
            print("1:" + fetched_events[i].to_json_str())
            print("2:" + fetched_events[i + 1].to_json_str())
            assert fetched_events[i].timestamp > fetched_events[i + 1].timestamp


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_event_with_timezone(bucket_cm):
    """Tries to retrieve an event using a timezone aware datetime."""
    hour = timedelta(hours=1)
    td_offset = 2 * hour
    tz = timezone(td_offset)

    dt_utc = datetime(2017, 10, 27, hour=0, minute=5, tzinfo=timezone.utc)
    dt_with_tz = dt_utc.replace(tzinfo=tz)

    with bucket_cm as bucket:
        bucket.insert(Event(timestamp=dt_with_tz))
        fetched_events = bucket.get(
            starttime=dt_with_tz - hour, endtime=dt_with_tz + hour
        )
        assert len(fetched_events) == 1

        fetched_events = bucket.get(
            starttime=dt_utc - td_offset - hour, endtime=dt_utc - td_offset + hour
        )
        assert len(fetched_events) == 1


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_datefilter(bucket_cm):
    """
    Tests the datetimefilter when fetching events
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = []
        for i in range(10):
            events.append(Event(timestamp=now + timedelta(seconds=i)))
        bucket.insert(events)

        # Starttime
        for i in range(eventcount):
            fetched_events = bucket.get(-1, starttime=events[i].timestamp)
            assert eventcount - i == len(fetched_events)

        # Endtime
        for i in range(eventcount):
            fetched_events = bucket.get(-1, endtime=events[i].timestamp)
            assert i + 1 == len(fetched_events)

        # Both
        for i in range(eventcount):
            for j in range(i + 1, eventcount):
                fetched_events = bucket.get(
                    starttime=events[i].timestamp, endtime=events[j].timestamp
                )
                assert j - i + 1 == len(fetched_events)


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_insert_invalid(bucket_cm):
    with bucket_cm as bucket:
        event = "not a real event"
        with pytest.raises(TypeError):
            bucket.insert(event)


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_replace(bucket_cm):
    """
    Tests the replace event event in bucket functionality
    """
    with bucket_cm as bucket:
        # Create two events
        e1 = bucket.insert(Event(data={"label": "test1"}, timestamp=now))
        assert e1
        assert e1.id is not None
        e2 = bucket.insert(
            Event(data={"label": "test2"}, timestamp=now + timedelta(seconds=1))
        )
        assert e2
        assert e2.id is not None

        e1.data["label"] = "test1-replaced"
        bucket.replace(e1.id, e1)

        bucket.insert(
            Event(data={"label": "test3"}, timestamp=now + timedelta(seconds=2))
        )

        e2.data["label"] = "test2-replaced"
        bucket.replace(e2.id, e2)

        # Assert length
        assert 3 == len(bucket.get(-1))
        assert bucket.get(-1)[0]["data"]["label"] == "test3"
        assert bucket.get(-1)[1]["data"]["label"] == "test2-replaced"
        assert bucket.get(-1)[2]["data"]["label"] == "test1-replaced"


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_replace_last(bucket_cm):
    """
    Tests the replace last event in bucket functionality (simple)
    """
    with bucket_cm as bucket:
        # Create two events
        bucket.insert(Event(data={"label": "test1"}, timestamp=now))
        bucket.insert(
            Event(data={"label": "test2"}, timestamp=now + timedelta(seconds=1))
        )
        # Create second event to replace with the second one
        bucket.replace_last(
            Event(
                data={"label": "test2-replaced"}, timestamp=now + timedelta(seconds=1)
            )
        )
        bucket.insert(
            Event(data={"label": "test3"}, timestamp=now + timedelta(seconds=2))
        )
        # Assert data
        result = bucket.get(-1)
        assert 3 == len(result)
        assert result[1]["data"]["label"] == "test2-replaced"


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_replace_last_complex(bucket_cm):
    """
    Tests the replace last event in bucket functionality (complex)
    """
    with bucket_cm as bucket:
        # Create first event
        event1 = Event(data={"label": "test1"}, timestamp=now, duration=timedelta(1))
        bucket.insert(event1)
        eventcount = len(bucket.get(-1))
        # Create second event to replace with the first one
        event2 = Event(
            data={"label": "test2"},
            duration=timedelta(0),
            timestamp=now + timedelta(seconds=1),
        )
        bucket.replace_last(event2)
        # Assert length and content
        result = bucket.get(-1)
        assert eventcount == len(result)
        assert event2 == result[0]


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_last(bucket_cm):
    """
    Tests setting the result limit when fetching events
    """
    now = datetime.now()
    second = timedelta(seconds=1)
    with bucket_cm as bucket:
        events = [
            Event(data={"label": "test"}, timestamp=ts, duration=timedelta(0))
            for ts in [now + second, now + second * 2, now + second * 3]
        ]

        for event in events:
            bucket.insert(event)

        assert bucket.get(limit=1)[0] == events[-1]
        for event in bucket.get(limit=5):
            print(event.timestamp, event.data["label"])


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_limit(bucket_cm):
    """
    Tests setting the result limit when fetching events
    """
    with bucket_cm as bucket:
        for i in range(5):
            bucket.insert(Event(timestamp=now))

        assert 0 == len(bucket.get(limit=0))
        assert 1 == len(bucket.get(limit=1))
        assert 3 == len(bucket.get(limit=3))
        assert 5 == len(bucket.get(limit=5))
        assert 5 == len(bucket.get(limit=-1))


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_metadata(bucket_cm):
    """
    Tests the get_metadata function
    """
    with bucket_cm as bucket:
        print(bucket.ds.storage_strategy)
        metadata = bucket.metadata()
        print(metadata)
        assert "created" in metadata
        assert "client" in metadata
        assert "hostname" in metadata
        assert "id" in metadata
        assert "name" in metadata
        assert "type" in metadata
    with pytest.raises(Exception):
        bucket.metadata()


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_eventcount(bucket_cm):
    """
    Tests the get_eventcount function
    """
    with bucket_cm as bucket:
        print(bucket.ds.storage_strategy)
        assert bucket.get_eventcount() == 0
        for _ in range(5):
            bucket.insert(Event(timestamp=now))
        assert bucket.get_eventcount() == 5
        # TODO: Test with timestamps and start/endtime filtering

        bucket.insert(Event(timestamp=now + timedelta(seconds=5)))
        assert (
            bucket.get_eventcount(starttime=now - timedelta(seconds=1), endtime=now)
            == 5
        )
        assert bucket.get_eventcount(endtime=now + timedelta(seconds=1)) == 5
        assert bucket.get_eventcount(starttime=now + timedelta(seconds=1)) == 1
