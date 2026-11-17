from datetime import datetime, timedelta, timezone

import pytest

from aw_core.models import Event
from aw_datastore.storages.sqlite import SqliteStorage


@pytest.mark.parametrize("year", [1, 1960, 2026, 2300, 9998])
@pytest.mark.parametrize(
    "operation", ["insert_one", "insert_many", "replace", "replace_last"]
)
def test_sqlite_preserves_exact_event_microseconds(tmp_path, year, operation):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "precision.db"))
    store.create_bucket("window", "t", "c", "h", "2026-01-01T00:00:00Z")
    timestamp = datetime(year, 1, 1, microsecond=123457, tzinfo=timezone.utc)
    expected = Event(timestamp=timestamp, duration=timedelta(microseconds=3), data={})
    try:
        if operation == "insert_one":
            store.insert_one("window", expected)
        elif operation == "insert_many":
            store.insert_many("window", [expected])
        else:
            original = store.insert_one(
                "window", Event(timestamp="2026-01-01T00:00:00Z")
            )
            if operation == "replace":
                assert store.replace("window", original.id, expected)
            else:
                assert store.replace_last("window", expected)
        store.commit()
        row = store.conn.execute("SELECT id,starttime,endtime FROM events").fetchone()
        delta = timestamp - datetime(1970, 1, 1, tzinfo=timezone.utc)
        micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
        assert row[1:] == (micros, micros + 3)
        actual = store.get_event("window", row[0])
        assert actual.timestamp == timestamp
        assert actual.duration == timedelta(microseconds=3)
    finally:
        store.conn.close()


def test_archive_iterator_includes_pre_epoch_history(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "archive.db"))
    peer = store.for_device("peer")
    peer.create_bucket("window", "t", "c", "peer", "2026-01-01T00:00:00Z")
    expected = Event(
        timestamp="1960-01-01T00:00:00.123457Z",
        duration=0.000003,
        data={"archive": True},
    )
    peer.insert_one("window", expected)
    store.commit()
    snapshot = store.snapshot()
    try:
        events = list(snapshot.for_device("peer").iter_events("window", flush=False))
        assert len(events) == 1
        assert events[0].timestamp == expected.timestamp
        assert events[0].duration == expected.duration
        assert events[0].data == expected.data
        metadata = snapshot.for_device("peer").buckets_with_last_updated()["window"]
        assert (
            metadata["last_updated"]
            == (expected.timestamp + expected.duration).isoformat()
        )
    finally:
        snapshot.conn.close()
        store.conn.close()


@pytest.mark.parametrize("year", [1, 1960, 2026, 2300, 9998])
def test_unbounded_reads_and_exact_range_boundaries(tmp_path, year):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "bounds.db"))
    store.create_bucket("window", "t", "c", "h", "2026-01-01T00:00:00Z")
    timestamp = datetime(year, 1, 1, microsecond=123457, tzinfo=timezone.utc)
    try:
        store.insert_one("window", Event(timestamp=timestamp, duration=0))
        assert len(store.get_events("window", -1)) == 1
        assert store.get_eventcount("window") == 1
        assert len(store.get_events("window", -1, timestamp, timestamp)) == 1
        assert store.get_eventcount("window", timestamp, timestamp) == 1
        after = timestamp + timedelta(microseconds=1)
        before = timestamp - timedelta(microseconds=1)
        assert store.get_eventcount("window", after, after) == 0
        assert store.get_eventcount("window", before, before) == 0
        assert store.get_events("window", -1, after, after) == []
        assert store.get_events("window", -1, before, before) == []
    finally:
        store.conn.close()


def test_event_json_and_assignment_preserve_offset_microseconds():
    expected = datetime(2026, 1, 1, microsecond=123457, tzinfo=timezone.utc)
    event = Event(timestamp="2026-01-01T01:00:00.123457+01:00", duration=0.000003)
    assert event.timestamp == expected
    assert Event(**event.to_json_dict()) == event
    event.timestamp = "2026-01-01T00:00:00.654321Z"
    assert event.timestamp.microsecond == 654321


def test_reads_do_not_rewrite_legacy_real_timestamps(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "legacy-real.db"))
    store.create_bucket("window", "t", "c", "h", "2026-01-01T00:00:00Z")
    row = store.conn.execute("SELECT rowid FROM buckets").fetchone()[0]
    # The old float writer could leave SQLite REAL values, despite INTEGER affinity.
    store.conn.execute(
        "INSERT INTO events(bucketrow,starttime,endtime,datastr) VALUES (?, ?, ?, ?)",
        (row, -315619199999999.5, -315619199999996.5, '{"legacy":true}'),
    )
    store.commit()
    sql = "SELECT id,bucketrow,starttime,endtime,datastr,typeof(starttime) FROM events"
    before = store.conn.execute(sql).fetchall()
    try:
        events = store.get_events("window", -1)
        assert len(events) == 1
        assert events[0].duration == timedelta(microseconds=3)
        assert events[0].data == {"legacy": True}
        assert store.conn.execute(sql).fetchall() == before
    finally:
        store.conn.close()


def test_naive_query_bounds_keep_local_time_interpretation(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "naive.db"))
    store.create_bucket("window", "t", "c", "h", "2026-01-01T00:00:00Z")
    bound = datetime(2026, 1, 1, microsecond=123457)
    try:
        store.insert_one("window", Event(timestamp=bound.astimezone(timezone.utc)))
        assert len(store.get_events("window", -1, bound, bound)) == 1
        assert store.get_eventcount("window", bound, bound) == 1
    finally:
        store.conn.close()
