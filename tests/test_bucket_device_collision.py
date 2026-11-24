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
    assert create(storage, device_id="remote")["device_id"] == "remote"


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
