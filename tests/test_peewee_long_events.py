"""Accepted event durations must not be capped implicitly by range queries."""

from datetime import datetime, timedelta, timezone

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
        query, BASE + timedelta(hours=36), BASE + timedelta(hours=37)
    )
    sql, params = query.sql()
    plan = storage.db.execute_sql("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    assert "eventmodel_bucket_id_timestamp" in str(plan)
    # Sorting equal-timestamp runs is fine; sorting the entire result is not.
    assert "TEMP B-TREE FOR ORDER BY" not in str(plan)
