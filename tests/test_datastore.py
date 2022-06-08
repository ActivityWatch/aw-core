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
now = datetime.now(tz=timezone.utc)
td1s = timedelta(seconds=1)
td1d = timedelta(days=1)


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
def test_nonexistent_bucket(datastore):
    """
    Tests that a KeyError is raised if you request a non-existent bucket
    """
    with pytest.raises(KeyError):
        datastore["I-do-not-exist"]


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_insert_one(bucket_cm):
    """
    Tests inserting one event into a bucket
    """
    with bucket_cm as bucket:
        n_events = len(bucket.get())
        event = Event(timestamp=now, duration=td1s, data={"key": "val"})
        bucket.insert(event)
        fetched_events = bucket.get()
        assert n_events + 1 == len(fetched_events)
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
        events = num_events * [Event(timestamp=now, duration=td1s, data={"key": "val"})]
        bucket.insert(events)
        fetched_events = bucket.get(limit=-1)
        assert num_events == len(fetched_events)
        for e, fe in zip(events, fetched_events):
            assert e == fe


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_insert_many_upsert(bucket_cm):
    """
    Tests that you can update/upsert many events at the same time to a bucket
    """
    num_events = 10
    with bucket_cm as bucket:
        events = num_events * [Event(timestamp=now, duration=td1s, data={"key": "val"})]
        # insert events to get IDs assigned
        bucket.insert(events)

        events = bucket.get(limit=-1)
        assert num_events == len(events)
        for e in events:
            assert e.id is not None
            e.data["key"] = "new val"

        # Upsert the events
        bucket.insert(events)

        events = bucket.get(limit=-1)
        assert num_events == len(events)
        for e in events:
            assert e.data["key"] == "new val"


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_delete(bucket_cm):
    """
    Tests deleting single events
    """
    num_events = 10
    with bucket_cm as bucket:
        events = num_events * [Event(timestamp=now, duration=td1s, data={"key": "val"})]
        bucket.insert(events)

        fetched_events = bucket.get(limit=-1)
        print(fetched_events[0])
        assert num_events == len(fetched_events)

        # Test deleting event
        assert bucket.delete(fetched_events[0]["id"])

        # Test deleting non-existent event
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
            events.append(Event(timestamp=now + i * td1s, duration=td1s))
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
def test_get_datefilter_simple(bucket_cm):
    with bucket_cm as bucket:
        eventcount = 3
        events = [
            Event(timestamp=now + i * td1s, duration=td1s) for i in range(eventcount)
        ]
        bucket.insert(events)

        # Get first event, but expect only half the event to match the interval
        fetched_events = bucket.get(
            -1,
            starttime=now - 0.5 * td1s,
            endtime=now + 0.5 * td1s,
        )
        assert 1 == len(fetched_events)

        # Get first two events, but expect only half of each to match the interval
        fetched_events = bucket.get(
            -1,
            starttime=now + 0.5 * td1s,
            endtime=now + 1.5 * td1s,
        )
        assert 2 == len(fetched_events)

        # Get last event, but expect only half to match the interval
        fetched_events = bucket.get(
            -1,
            starttime=now + 2.5 * td1s,
            endtime=now + 3.5 * td1s,
        )
        assert 1 == len(fetched_events)

        # Check approx precision
        fetched_events = bucket.get(
            -1,
            starttime=now - 0.01 * td1s,
            endtime=now + 0.01 * td1s,
        )
        assert 1 == len(fetched_events)

        # Check precision of start
        fetched_events = bucket.get(
            -1,
            starttime=now,
            endtime=now,
        )
        assert 1 == len(fetched_events)

        # Check approx precision of end
        fetched_events = bucket.get(
            -1,
            starttime=now + 2.99 * td1s,
            endtime=now + 3.01 * td1s,
        )
        assert 1 == len(fetched_events)


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_event_by_id(bucket_cm):
    """Test that we can retrieve single events by their IDs"""
    with bucket_cm as bucket:
        eventcount = 2
        # Create 1-day long events
        events = [
            Event(timestamp=now + i * td1d, duration=td1d) for i in range(eventcount)
        ]
        bucket.insert(events)

        # Retrieve stored events
        events = bucket.get()
        for e in events:
            # Query them one-by-one
            event = bucket.get_by_id(e.id)
            assert e == event


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_event_by_id_notfound(bucket_cm):
    """Test fetching an ID that does not exist"""
    with bucket_cm as bucket:
        assert bucket.get_by_id(1337 * 10**6) is None


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_event_trimming(bucket_cm):
    """Test that event trimming works correctly (when querying events that intersect with the query range)"""
    # TODO: Trimming should be possible to disable
    # (needed in raw data view, among other places where event editing is permitted)
    from aw_datastore.storages import PeeweeStorage

    with bucket_cm as bucket:
        if not isinstance(bucket.ds.storage_strategy, PeeweeStorage):
            pytest.skip("Trimming not supported for datastore")

        eventcount = 2
        # Create 1-day long events
        events = [
            Event(timestamp=now + i * td1d, duration=td1d) for i in range(eventcount)
        ]
        bucket.insert(events)

        # Result should contain half of each event
        fetched_events = bucket.get(
            -1,
            starttime=now + td1d / 2,
            endtime=now + 1.5 * td1d,
        )
        assert 2 == len(fetched_events)
        total_duration = sum((e.duration for e in fetched_events), timedelta())
        assert td1d == timedelta(seconds=round(total_duration.total_seconds()))


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_datefilter_start(bucket_cm):
    """
    Tests the datetimefilter when fetching events
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = [
            Event(timestamp=now + i * td1s, duration=td1s) for i in range(eventcount)
        ]
        bucket.insert(events)

        # Starttime
        for i in range(eventcount):
            fetched_events = bucket.get(-1, starttime=events[i].timestamp + 0.01 * td1s)
            assert eventcount - i == len(fetched_events)


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_datefilter_end(bucket_cm):
    """
    Tests the datetimefilter when fetching events
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = [
            Event(timestamp=now + i * td1s, duration=td1s) for i in range(eventcount)
        ]
        bucket.insert(events)

        # Endtime
        for i in range(eventcount):
            fetched_events = bucket.get(-1, endtime=events[i].timestamp - 0.01 * td1s)
            assert i == len(fetched_events)


@pytest.mark.parametrize("bucket_cm", param_testing_buckets_cm())
def test_get_datefilter_both(bucket_cm):
    """
    Tests the datetimefilter when fetching events
    """
    with bucket_cm as bucket:
        eventcount = 10
        events = [
            Event(timestamp=now + i * td1s, duration=td1s) for i in range(eventcount)
        ]
        bucket.insert(events)

        # Both
        for i in range(eventcount):
            for j in range(i + 1, eventcount):
                fetched_events = bucket.get(
                    starttime=events[i].timestamp + timedelta(seconds=0.01),
                    endtime=events[j].timestamp
                    + events[j].duration
                    - timedelta(seconds=0.01),
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
