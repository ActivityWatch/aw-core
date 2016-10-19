from typing import Optional, List
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

from aw_core.models import Event

from takethetime import ttt

from . import get_storage_methods, Datastore


def create_test_events(n):
    now = datetime.now(timezone.utc)

    events = [None] * n
    for i in range(n):
        events[i] = Event(label="asd", timestamp=now - i * timedelta(hours=1))

    return events


@contextmanager
def temporary_bucket(ds):
    bucket_id = "test_bucket"
    ds.delete_bucket(bucket_id)
    bucket = ds.create_bucket(bucket_id, "testingtype", "test-client", "testing-box")
    yield bucket
    ds.delete_bucket(bucket_id)


def benchmark(ds: Datastore):
    num_events = 10000
    events = create_test_events(num_events)

    with temporary_bucket(ds) as bucket:
        with ttt(ds):
            with ttt("insert"):
                bucket.insert(events)

            with ttt("get one"):
                events = bucket.get(limit=1)
                # print("Total number of events: {}".format(len(events)))

            with ttt("get all"):
                events = bucket.get()
                # print("Total number of events: {}".format(len(events)))

            with ttt("get within time interval"):
                events = bucket.get(starttime=events[-int(num_events / 2)].timestamp, endtime=events[0].timestamp)
                assert len(events) == int(num_events / 2) - 1
                # print("Events within time interval: {}".format(len(events)))

            assert events == sorted(events, reverse=True, key=lambda e: e.timestamp)
            # print(len(events))


if __name__ == "__main__":
    for storage in get_storage_methods():
        print(storage.__name__)
        ds = Datastore(storage, testing=True)
        benchmark(ds)
