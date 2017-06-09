import sys
from typing import Callable
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

from aw_core.models import Event

from takethetime import ttt

from . import get_storage_methods, Datastore
from .storages import AbstractStorage


def create_test_events(n):
    now = datetime.now(timezone.utc)

    events = [None] * n
    for i in range(n):
        events[i] = Event(timestamp=now + i * timedelta(hours=1), data={"label": "asd"})

    return events


@contextmanager
def temporary_bucket(ds):
    bucket_id = "test_bucket"
    try:
        ds.delete_bucket(bucket_id)
    except KeyError:
        pass
    bucket = ds.create_bucket(bucket_id, "testingtype", "test-client", "testing-box")
    yield bucket
    ds.delete_bucket(bucket_id)


def benchmark(storage: Callable[..., AbstractStorage]):
    ds = Datastore(storage, testing=True)
    num_events = 5 * 10**4
    events = create_test_events(num_events)

    print(storage.__name__)

    with temporary_bucket(ds) as bucket:
        with ttt(" sum"):
            with ttt(" insert {} events".format(num_events)):
                bucket.insert(events)

            with ttt(" insert 1 event"):
                bucket.insert(events[-1])

            with ttt(" get one"):
                events_tmp = bucket.get(limit=1)
                # print("Total number of events: {}".format(len(events)))

            with ttt(" get all"):
                events_tmp = bucket.get(limit=num_events)
                print(len(events_tmp))
                assert len(events_tmp) == num_events
                for e1, e2 in zip(events, sorted(events_tmp, key=lambda e: e.timestamp)):
                    try:
                        # Can't do direct comparison since tz will differ in object type (but have identical meaning)
                        # TODO: Fix the above by overriding __eq__ on Event
                        assert e1.timestamp.second == e2.timestamp.second
                        assert e1.timestamp.microsecond == e2.timestamp.microsecond
                    except AssertionError as e:
                        print(e1)
                        print(e2)
                        raise e
                # print("Total number of events: {}".format(len(events)))

            def events_in_interval(n):
                with ttt(" get {} events within time interval".format(n)):
                    events_tmp = bucket.get(limit=num_events,
                                            starttime=events[0].timestamp,
                                            endtime=events[n].timestamp)
                    assert len(events_tmp) == n - 1
                    # print("Events within time interval: {}".format(len(events)))

            events_in_interval(int(num_events / 2))
            events_in_interval(10)


if __name__ == "__main__":
    for storage in get_storage_methods().values():
        if len(sys.argv) <= 1 or storage.__name__ in sys.argv:
            benchmark(storage)
