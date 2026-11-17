"""Accepted event durations must not be capped implicitly by range queries."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event as ThreadEvent

import pytest
from aw_core.models import Event
from aw_datastore import Datastore
from aw_datastore.storages import PeeweeStorage
from aw_datastore.storages.peewee import EventModel

UTC = timezone.utc
BASE = datetime(2026, 10, 1, tzinfo=UTC)


@pytest.fixture()
def bucket(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "aw_datastore.storages.peewee.get_data_dir", lambda _: str(tmp_path)
    )
    ds = Datastore(PeeweeStorage, testing=True, filepath=str(tmp_path / "events.db"))
    try:
        yield ds.create_bucket(
            "long-events", type="test", client="test", hostname="test"
        )
    finally:
        ds.storage_strategy.db.close()


@pytest.mark.parametrize("hours", [24, 48, 240])
@pytest.mark.parametrize(
    "start_hour,end_hour,expected",
    [
        (None, None, True),
        (None, -1, False),
        (None, 0, True),
        (0, None, True),
        (12, None, True),
        (None, 13, True),
        (12, 13, True),
        (36, 37, None),  # Inside day two for >24h events; outside for 24h.
        (None, 48, True),
        (48, 49, None),
        (240, 241, None),
        (241, None, False),
    ],
)
def test_long_event_overlap(bucket, hours, start_hour, end_hour, expected):
    original = Event(
        timestamp=BASE, duration=timedelta(hours=hours), data={"label": "long"}
    )
    bucket.insert(original)
    start = BASE + timedelta(hours=start_hour) if start_hour is not None else None
    end = BASE + timedelta(hours=end_hour) if end_hour is not None else None
    if expected is None:
        expected = start_hour <= hours

    events = bucket.get(starttime=start, endtime=end)
    assert bucket.get_eventcount(starttime=start, endtime=end) == int(expected)
    assert len(events) == int(expected)
    if expected:
        event = events[0]
        assert event.id == original.id
        assert event.data == original.data
        # Peewee already clips returned copies, including zero-length boundary hits.
        clipped_start = max(BASE, start) if start else BASE
        clipped_end = (
            min(BASE + original.duration, end) if end else BASE + original.duration
        )
        assert event.timestamp == clipped_start
        assert event.duration == clipped_end - clipped_start
    # Reads must not rewrite the stored event or mutate the caller's object.
    stored = bucket.get()[0]
    assert stored.timestamp == original.timestamp == BASE
    assert stored.duration == original.duration == timedelta(hours=hours)


def test_long_and_short_events_keep_order_limits_and_offsets(bucket):
    bucket.insert(
        [
            Event(timestamp=BASE, duration=172800, data={"label": "long"}),
            Event(
                timestamp=BASE + timedelta(hours=36),
                duration=60,
                data={"label": "short"},
            ),
            Event(
                timestamp=BASE + timedelta(hours=36),
                duration=120,
                data={"label": "tie"},
            ),
            Event(
                timestamp=BASE + timedelta(hours=35),
                duration=60,
                data={"label": "before"},
            ),
            Event(
                timestamp=BASE + timedelta(hours=38),
                duration=60,
                data={"label": "after"},
            ),
        ]
    )
    start, end = BASE + timedelta(hours=36), BASE + timedelta(hours=37)
    offset = timezone(timedelta(hours=5, minutes=30))
    for bounds in [(start, end), (start.astimezone(offset), end.astimezone(offset))]:
        result = bucket.get(starttime=bounds[0], endtime=bounds[1])
        assert [e.data["label"] for e in result] == ["short", "tie", "long"]
        assert bucket.get_eventcount(*bounds) == 3
        assert bucket.get(limit=0, starttime=bounds[0], endtime=bounds[1]) == []
        assert [e.id for e in bucket.get(2, *bounds)] == [e.id for e in result[:2]]
    assert bucket.get(starttime=BASE + timedelta(days=20)) == []
    assert bucket.get_eventcount(starttime=BASE + timedelta(days=20)) == 0


def test_empty_long_event_range(bucket):
    assert bucket.get(starttime=BASE, endtime=BASE + timedelta(hours=1)) == []
    assert bucket.get_eventcount(BASE, BASE + timedelta(hours=1)) == 0


def test_actual_overlap_query_keeps_bucket_timestamp_index(bucket):
    storage = bucket.ds.storage_strategy
    query = (
        EventModel.select()
        .where(EventModel.bucket == storage.bucket_keys[bucket.bucket_id])
        .order_by(
            EventModel.timestamp.desc(), EventModel.duration.asc(), EventModel.id.asc()
        )
        .limit(100)
    )
    query = storage._where_range(
        query,
        bucket.bucket_id,
        BASE + timedelta(hours=36),
        BASE + timedelta(hours=37),
    )
    sql, params = query.sql()
    plan = storage.db.execute_sql("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    assert "eventmodel_bucket_id_timestamp" in str(plan)
    # The max-duration lookback bounds the index range on both sides, so a
    # range query does not scan the bucket's whole history.
    assert "timestamp>? AND timestamp<?" in str(plan)
    # Sorting equal-timestamp runs is fine; sorting the entire result is not.
    assert "TEMP B-TREE FOR ORDER BY" not in str(plan)


def test_inserting_longer_event_widens_lookback(bucket):
    # Prime the cached bound with a short event, then insert a longer one
    # through each write path; the interior query must still find it.
    bucket.insert(Event(timestamp=BASE, duration=timedelta(minutes=1)))
    day_two = (BASE + timedelta(hours=36), BASE + timedelta(hours=37))
    assert bucket.get_eventcount(*day_two) == 0

    bucket.insert(Event(timestamp=BASE, duration=timedelta(hours=48)))
    assert bucket.get_eventcount(*day_two) == 1

    bucket.insert(
        [Event(timestamp=BASE - timedelta(days=5), duration=timedelta(days=7))]
    )
    assert (
        bucket.get_eventcount(BASE + timedelta(hours=40), BASE + timedelta(hours=41))
        == 2
    )


def test_replace_with_longer_duration_widens_lookback(bucket):
    short = bucket.insert(Event(timestamp=BASE, duration=timedelta(minutes=1)))
    interior = (BASE + timedelta(hours=60), BASE + timedelta(hours=61))
    assert bucket.get_eventcount(*interior) == 0

    bucket.replace(
        short.id, Event(timestamp=BASE, duration=timedelta(hours=72), data={})
    )
    assert bucket.get_eventcount(*interior) == 1

    last = Event(timestamp=BASE, duration=timedelta(hours=96), data={})
    bucket.replace_last(last)
    assert (
        bucket.get_eventcount(BASE + timedelta(hours=90), BASE + timedelta(hours=91))
        == 1
    )


def test_lookback_bound_reset_when_bucket_recreated(bucket, tmp_path):
    ds = bucket.ds
    bucket.insert(Event(timestamp=BASE, duration=timedelta(hours=48)))
    interior = (BASE + timedelta(hours=36), BASE + timedelta(hours=37))
    assert bucket.get_eventcount(*interior) == 1
    ds.delete_bucket(bucket.bucket_id)
    fresh = ds.create_bucket(
        bucket.bucket_id, type="test", client="test", hostname="test"
    )
    assert fresh.get_eventcount(*interior) == 0
    fresh.insert(Event(timestamp=BASE, duration=timedelta(hours=48)))
    assert fresh.get_eventcount(*interior) == 1


def test_bound_covers_write_racing_a_cold_read(bucket):
    # A writer raises the floor before its row is visible; a reader that
    # filled the cache from the older database maximum must still honor it.
    storage = bucket.ds.storage_strategy
    bucket.insert(Event(timestamp=BASE, duration=timedelta(minutes=1)))
    storage._forget_duration_bound(bucket.bucket_id)
    storage._raise_max_duration(bucket.bucket_id, 48 * 3600)
    assert storage._bucket_max_duration(bucket.bucket_id) >= 48 * 3600


@pytest.mark.parametrize("read", ["get", "get_eventcount"])
@pytest.mark.parametrize("write", ["insert", "insert_many", "replace", "replace_last"])
def test_write_between_range_construction_and_execution(
    bucket, monkeypatch, read, write
):
    storage = bucket.ds.storage_strategy
    short = bucket.insert(Event(timestamp=BASE, duration=60))
    bounds = (BASE + timedelta(hours=36), BASE + timedelta(hours=37))
    assert bucket.get_eventcount(*bounds) == 0  # Warm the short lookback.
    original_where_range = storage._where_range
    writer_started = ThreadEvent()
    writer_finished = ThreadEvent()
    committed_before_execution = []

    def write_long_event():
        try:
            writer_started.set()
            event = Event(timestamp=BASE, duration=timedelta(hours=48))
            if write == "insert_many":
                bucket.insert([event])
            elif write == "replace":
                bucket.replace(short.id, event)
            elif write == "replace_last":
                bucket.replace_last(event)
            else:
                bucket.insert(event)
            writer_finished.set()
        finally:
            storage.db.close()  # Peewee connections are thread-local.

    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = []

        def pause_after_range(*args, **kwargs):
            query = original_where_range(*args, **kwargs)
            pending.append(executor.submit(write_long_event))
            assert writer_started.wait(5)
            # A serialized read may finish before the writer. Otherwise its
            # predicate must include the write committed before SQL execution.
            committed_before_execution.append(writer_finished.wait(0.2))
            return query

        monkeypatch.setattr(storage, "_where_range", pause_after_range)
        if read == "get":
            result = len(bucket.get(starttime=bounds[0], endtime=bounds[1]))
        else:
            result = bucket.get_eventcount(*bounds)
        pending[0].result(timeout=5)
    assert result == int(committed_before_execution[0])
    monkeypatch.setattr(storage, "_where_range", original_where_range)
    assert bucket.get_eventcount(*bounds) == 1
    assert len(bucket.get(starttime=bounds[0], endtime=bounds[1])) == 1


def test_failed_batch_keeps_raised_bound(bucket, monkeypatch):
    bucket.insert(Event(timestamp=BASE, duration=timedelta(minutes=1)))
    real_insert_many = EventModel.insert_many
    calls = []

    def flaky_insert_many(rows):
        calls.append(len(rows))
        if len(calls) > 1:
            raise RuntimeError("database is locked")
        return real_insert_many(rows)

    monkeypatch.setattr(EventModel, "insert_many", flaky_insert_many)
    batch = [
        Event(timestamp=BASE + timedelta(seconds=i), duration=timedelta(hours=48))
        for i in range(150)
    ]
    with pytest.raises(RuntimeError):
        bucket.insert(batch)
    monkeypatch.undo()
    # The first chunk (100 long events) was saved; bounded reads must see it.
    assert (
        bucket.get_eventcount(BASE + timedelta(hours=36), BASE + timedelta(hours=37))
        == 100
    )
