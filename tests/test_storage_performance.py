"""Storage performance contracts, using only disposable databases."""

from datetime import datetime, timedelta, timezone
import sqlite3
from unittest.mock import Mock

import pytest
from aw_core.models import Event
from aw_datastore import Datastore, get_storage_methods


@pytest.fixture(params=["memory", "peewee", "sqlite"])
def store(request, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "aw_datastore.storages.peewee.get_data_dir", lambda _: str(tmp_path)
    )
    kwargs = (
        {} if request.param == "memory" else {"filepath": str(tmp_path / "test.db")}
    )
    ds = Datastore(get_storage_methods()[request.param], testing=True, **kwargs)
    yield ds
    if request.param == "peewee":
        ds.storage_strategy.db.close()
    elif request.param == "sqlite":
        ds.storage_strategy.conn.close()


def create(ds, bid):
    return ds.create_bucket(bid, type="test", client="test", hostname="test")


def event(seconds=0, duration=1, label="test"):
    return Event(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc)
        + timedelta(seconds=seconds),
        duration=duration,
        data={"label": label},
    )


def test_cold_lookup_does_not_enumerate_buckets(store, monkeypatch):
    create(store, "a")
    store.bucket_instances.clear()
    monkeypatch.setattr(store, "buckets", Mock(side_effect=AssertionError("full scan")))
    assert store.has_bucket("a")
    assert not store.has_bucket("missing")
    assert store["a"].metadata()["id"] == "a"
    store.delete_bucket("a")
    assert not store.has_bucket("a")


def test_listing_matches_latest_event_and_includes_empty_buckets(store):
    a = create(store, "a")
    create(store, "empty")
    a.insert([event(0, duration=30), event(10), event(10, label="tie")])
    latest = a.get(1)[0]
    result = store.buckets(include_last_updated=True)
    assert (
        result["a"]["last_updated"] == (latest.timestamp + latest.duration).isoformat()
    )
    assert "last_updated" not in result["empty"]
    assert "last_updated" not in store.buckets()["a"]


def test_listing_uses_one_select(store):
    if store.storage_strategy.sid == "memory":
        return
    for i in range(10):
        create(store, str(i)).insert(event(i))
    storage = store.storage_strategy
    conn = storage.db.connection() if storage.sid == "peewee" else storage.conn
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        assert len(store.buckets(include_last_updated=True)) == 10
    finally:
        conn.set_trace_callback(None)
    assert sum(s.lstrip().upper().startswith("SELECT") for s in statements) == 1


def test_replace_is_bucket_scoped_and_reports_missing_events(store):
    a, b = create(store, "a"), create(store, "b")
    original = a.insert(event())
    assert not b.replace(original.id, event(1))
    assert a.get(1)[0].timestamp == original.timestamp
    assert not a.replace(9999, event())
    replacement = event(2)
    assert a.replace(original.id, replacement)
    assert replacement.id == original.id or store.storage_strategy.sid == "memory"
    assert a.get(1)[0].timestamp == replacement.timestamp


def test_replace_does_not_select_event_first(store):
    if store.storage_strategy.sid == "memory":
        return
    bucket = create(store, "a")
    original = bucket.insert(event())
    storage = store.storage_strategy
    conn = storage.db.connection() if storage.sid == "peewee" else storage.conn
    statements = []
    conn.set_trace_callback(statements.append)
    try:
        assert bucket.replace(original.id, event(1))
    finally:
        conn.set_trace_callback(None)
    assert not any(s.lstrip().upper().startswith("SELECT") for s in statements)
    assert sum(s.lstrip().upper().startswith("UPDATE") for s in statements) == 1


def test_iterator_preserves_event_data_order_and_duplicate_timestamps(store):
    bucket = create(store, "a")
    bucket.insert([event(2), event(0, duration=30), event(2, label="tie")])
    expected = [e.to_json_dict() for e in bucket.get()]
    iterator = bucket.iter_events()
    try:
        assert [e.to_json_dict() for e in iterator] == expected
    finally:
        iterator.close()
    assert list(create(store, "empty").iter_events()) == []


def test_disk_iterator_does_not_materialize_get_events(store, monkeypatch):
    if store.storage_strategy.sid == "memory":
        return
    bucket = create(store, "a")
    bucket.insert([event(i) for i in range(100)])
    monkeypatch.setattr(
        store.storage_strategy, "get_events", Mock(side_effect=AssertionError("eager"))
    )
    iterator = bucket.iter_events()
    assert next(iterator).timestamp == event(99).timestamp
    iterator.close()
    # A subsequent write succeeds after an early close.
    bucket.insert(event(100))


def test_peewee_cold_lookup_refreshes_externally_created_bucket_key(store):
    storage = store.storage_strategy
    if storage.sid != "peewee":
        return
    with sqlite3.connect(storage.db.database) as other:
        other.execute(
            "INSERT INTO bucketmodel (id, type, client, hostname, created, datastr) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("external", "test", "test", "test", "2024-01-01T00:00:00+00:00", "{}"),
        )
    assert "external" not in storage.bucket_keys
    bucket = store["external"]
    assert bucket.get() == []
    assert bucket.metadata()["id"] == "external"
    with sqlite3.connect(storage.db.database) as other:
        other.execute("DELETE FROM bucketmodel WHERE id = ?", ("external",))
    store.bucket_instances.clear()
    with pytest.raises(KeyError):
        store["external"]
    assert "external" not in storage.bucket_keys
