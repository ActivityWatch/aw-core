import sqlite3
from datetime import datetime, timezone

import pytest

from aw_core.models import Event
from aw_datastore.storages import MemoryStorage, SqliteStorage


@pytest.fixture
def storage(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "test.db"))
    try:
        yield store
    finally:
        store.conn.close()


def create(store, bucket_id="shared", device_id="local"):
    return store.create_bucket(
        bucket_id,
        "test",
        "client",
        "host",
        "2026-01-01T00:00:00+00:00",
        device_id=device_id,
    )


def event():
    return Event(timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc), data={"test": 1})


@pytest.mark.parametrize(
    "operation",
    [
        "has_bucket",
        "get_metadata",
        "get_events",
        "get_eventcount",
        "get_event",
        "iter_events",
        "update_bucket",
        "delete_bucket",
        "insert_one",
        "insert_many",
        "replace",
        "replace_last",
        "delete",
    ],
)
def test_ambiguous_bucket_fails_without_changing_either_device(storage, operation):
    create(storage)
    stored = storage.insert_one("shared", event())
    storage.commit()
    # Migration or another connection may introduce a collision after a caller
    # has already obtained/cached a bucket handle.
    storage.conn.execute(
        "INSERT INTO buckets(id, device_id, type, client, hostname, created, datastr) "
        "SELECT id, 'remote', type, client, hostname, created, datastr FROM buckets"
    )
    storage.conn.execute(
        "INSERT INTO events(bucketrow, starttime, endtime, datastr) "
        "SELECT (SELECT rowid FROM buckets WHERE device_id='remote'), "
        "starttime, endtime, datastr FROM events"
    )
    storage.commit()
    before = list(storage.conn.iterdump())
    args = {
        "get_events": (-1,),
        "get_event": (stored.id,),
        "update_bucket": (),
        "insert_one": (event(),),
        "insert_many": ([event()],),
        "replace": (stored.id, event()),
        "replace_last": (event(),),
        "delete": (stored.id,),
    }.get(operation, ())
    kwargs = {"name": "changed"} if operation == "update_bucket" else {}
    with pytest.raises(ValueError, match="Ambiguous bucket id: shared"):
        result = getattr(storage, operation)("shared", *args, **kwargs)
        if operation == "iter_events":
            list(result)
    assert list(storage.conn.iterdump()) == before


def test_create_returns_metadata_of_exact_device(storage):
    assert create(storage)["device_id"] == "local"
    assert create(storage, "other", device_id="remote")["device_id"] == "remote"


@pytest.mark.parametrize(
    "first, second",
    [("local", "remote"), ("remote", "local"), ("local", "local")],
)
def test_create_refuses_id_already_used_by_any_device(storage, first, second):
    # A second row with the same id would make both buckets unaddressable and
    # break every listing, so it must be refused before it is written.
    create(storage, device_id=first)
    storage.insert_one("shared", event())
    with pytest.raises(sqlite3.IntegrityError, match="shared"):
        create(storage, device_id=second)
    assert not storage.conn.in_transaction
    assert storage.get_metadata("shared")["device_id"] == first
    assert set(storage.buckets()) == {"shared"}
    assert set(storage.buckets_with_last_updated()) == {"shared"}
    storage.insert_one("shared", event())
    assert storage.get_eventcount("shared") == 2


def test_create_refuses_id_committed_by_another_connection(tmp_path):
    path = str(tmp_path / "shared.db")
    first = SqliteStorage(testing=True, filepath=path)
    second = SqliteStorage(testing=True, filepath=path)
    try:
        create(first, device_id="remote")
        with pytest.raises(sqlite3.IntegrityError, match="shared"):
            create(second)
        assert second.get_metadata("shared")["device_id"] == "remote"
    finally:
        first.conn.close()
        second.conn.close()


@pytest.mark.parametrize("device_id", ["local", "remote"])
def test_memory_duplicate_creation_does_not_discard_events(device_id):
    store = MemoryStorage(testing=True)
    create(store)
    store.insert_one("shared", event())
    before = store.get_metadata("shared"), store.get_events("shared", -1)
    with pytest.raises(ValueError):
        create(store, device_id=device_id)
    assert (store.get_metadata("shared"), store.get_events("shared", -1)) == before


def test_replace_last_never_modifies_another_bucket_with_same_endtime(storage):
    create(storage, "other")
    other = storage.insert_one("other", event())
    create(storage)
    storage.insert_one("shared", event())
    replacement = event()
    replacement.data = {"changed": True}
    storage.replace_last("shared", replacement)
    assert storage.get_event("other", other.id).data == {"test": 1}
    assert storage.get_events("shared", -1)[0].data == {"changed": True}


def test_peewee_rejects_device_id_it_cannot_store(tmp_path):
    # Peewee has no device_id column: a non-local identity must be refused,
    # not accepted and then reported back as "local".
    from aw_datastore.storages import PeeweeStorage

    store = PeeweeStorage(testing=True, filepath=str(tmp_path / "peewee.db"))
    with pytest.raises(ValueError, match="device_id"):
        create(store, "remote-only", device_id="remote")
    assert "remote-only" not in store.buckets()
    create(store, "local-ok")
    assert store.get_metadata("local-ok")["device_id"] == "local"


def test_insert_rejects_bucket_deleted_after_lookup(storage, monkeypatch):
    # Another connection may delete the bucket between _bucketrow() and the
    # INSERT; the write must fail rather than orphan the event.
    create(storage)
    storage.commit()
    resolve = storage._bucketrow

    def racing(bucket_id):
        row = resolve(bucket_id)
        storage.conn.execute("DELETE FROM buckets WHERE rowid = ?", (row,))
        return row

    monkeypatch.setattr(storage, "_bucketrow", racing)
    with pytest.raises(sqlite3.IntegrityError):
        storage.insert_one("shared", event())
