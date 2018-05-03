#!/usr/bin/env python3
import sys
from typing import Callable
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

from aw_core.models import Event

from takethetime import ttt

from aw_datastore import get_storage_methods, Datastore
from aw_datastore.storages import AbstractStorage


def create_test_events(n):
    now = datetime.now(timezone.utc) - timedelta(days=1000)

    events = []
    for i in range(n):
        events.append(Event(timestamp=now + i * timedelta(seconds=1), data={"label": "asd"}))

    return events


def create_tmpbucket(ds, num):
    bucket_id = "benchmark_test_bucket_{}".format(str(num))
    try:
        ds.delete_bucket(bucket_id)
    except KeyError:
        pass
    ds.create_bucket(bucket_id, "testingtype", "test-client", "testing-box")
    return bucket_id


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
    num_single_events = 50
    num_bulk_events = 2 * 10**3
    num_events = num_single_events + num_bulk_events + 1
    events = create_test_events(num_events)
    single_events = events[:num_single_events]
    bulk_events = events[num_single_events:-1]
    print(events[0])
    print(events[num_single_events])
    print(events[-1])

    print(storage.__name__)

    with temporary_bucket(ds) as bucket:
        with ttt(" sum"):
            with ttt(" single insert {} events".format(num_single_events)):
                for event in single_events:
                    bucket.insert(event)

            with ttt(" bulk insert {} events".format(num_bulk_events)):
                bucket.insert(bulk_events)

            with ttt(" insert 1 event"):
                bucket.insert(events[-1])

            with ttt(" get one"):
                events_tmp = bucket.get(limit=1)

            with ttt(" get all"):
                events_tmp = bucket.get(limit=num_events)
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
            # FIXME: This is broken with the SQLite datastore
            """
            def events_in_interval(n):
                with ttt(" get {} events within time interval".format(n)):
                    events_tmp = bucket.get(limit=num_events,
                                            starttime=events[0].timestamp,
                                            endtime=events[n].timestamp)
                    print("Events within time interval: {}".format(len(events)))
                    print(n)
                    assert len(events_tmp) == n - 1

            events_in_interval(int(num_events / 2))
            events_in_interval(10)
            """


if __name__ == "__main__":
    for storage in get_storage_methods().values():
        if len(sys.argv) <= 1 or storage.__name__ in sys.argv:
            benchmark(storage)
