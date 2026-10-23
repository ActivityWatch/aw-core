import sqlite3
from datetime import datetime, timezone

import pytest

from aw_core.models import Event
from aw_datastore.storages import MemoryStorage, PeeweeStorage, SqliteStorage


@pytest.mark.parametrize("bulk", [False, True])
def test_insert_locks_bucket_resolution_against_other_connection(
    tmp_path, monkeypatch, bulk
):
    path = str(tmp_path / "race.db")
    store = SqliteStorage(testing=True, filepath=path)
    store.create_bucket("window", "test", "client", "host", "2026-01-01T00:00:00+00:00")
    contender = sqlite3.connect(path, timeout=0)
    resolve = store._bucket_row
    outcomes = []

    def resolve_with_competing_delete(*args, **kwargs):
        row = resolve(*args, **kwargs)
        try:
            contender.execute("DELETE FROM buckets WHERE rowid = ?", [row])
            contender.commit()
        except sqlite3.OperationalError as exc:
            assert "locked" in str(exc)
            contender.rollback()
            outcomes.append("blocked")
        else:
            outcomes.append("deleted")
        return row

    monkeypatch.setattr(store, "_bucket_row", resolve_with_competing_delete)
    event = Event(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc), duration=1, data={}
    )
    try:
        if bulk:
            store.insert_many("window", [event])
        else:
            store.insert_one("window", event)
        store.commit()
        assert outcomes == ["blocked"]
        assert (
            store.conn.execute(
                "SELECT count(*) FROM events JOIN buckets ON events.bucketrow = buckets.rowid"
            ).fetchone()[0]
            == 1
        )
    finally:
        contender.close()
        store.conn.close()


def test_snapshot_excludes_later_writer_commits(tmp_path):
    path = str(tmp_path / "snapshot.db")
    store = SqliteStorage(testing=True, filepath=path)
    store.create_bucket("window", "test", "client", "host", "2026-01-01T00:00:00+00:00")
    store.commit()
    snap = store.snapshot()
    try:
        writer = sqlite3.connect(path)
        row = writer.execute("SELECT rowid FROM buckets WHERE id='window'").fetchone()[
            0
        ]
        writer.execute(
            "INSERT INTO events(bucketrow,starttime,endtime,datastr) VALUES (?, ?, ?, ?)",
            (row, 1704110400000000, 1704110401000000, "{}"),
        )
        writer.commit()
        writer.close()
        assert list(snap.iter_events("window", flush=False)) == []
        assert store.get_eventcount("window") == 1
    finally:
        snap.conn.close()
        store.conn.close()


@pytest.mark.parametrize("backend", [MemoryStorage, PeeweeStorage])
@pytest.mark.parametrize("device_id", ["peer", ""])
def test_unsupported_device_creation_preserves_existing_history(
    tmp_path, backend, device_id
):
    store = (
        backend(testing=True)
        if backend is MemoryStorage
        else backend(testing=True, filepath=str(tmp_path / "peewee.db"))
    )
    created = "2026-01-01T00:00:00+00:00"
    store.create_bucket("window", "test", "client", "host", created)
    store.insert_one(
        "window",
        Event(
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            duration=1,
            data={"kept": True},
        ),
    )
    try:
        with pytest.raises(NotImplementedError, match="device"):
            store.create_bucket(
                "window", "test", "client", "peer-host", created, device_id=device_id
            )
        with pytest.raises(NotImplementedError, match="device"):
            store.create_bucket(
                "new", "test", "client", "peer-host", created, device_id=device_id
            )
        assert store.get_metadata("window")["hostname"] == "host"
        assert store.get_events("window", -1)[0].data == {"kept": True}
        assert not store.has_bucket("new")
    finally:
        if backend is PeeweeStorage:
            store.db.close()


@pytest.mark.parametrize("bulk", [False, True])
def test_failed_insert_releases_new_transaction_but_preserves_pending_work(
    tmp_path, bulk
):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "failure.db"))
    event = Event(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc), duration=1, data={}
    )

    def insert_missing():
        if bulk:
            store.insert_many("missing", [event])
        else:
            store.insert_one("missing", event)

    try:
        with pytest.raises((ValueError, sqlite3.IntegrityError)):
            insert_missing()
        assert not store.conn.in_transaction
        store.create_bucket(
            "window", "test", "client", "host", "2026-01-01T00:00:00+00:00"
        )
        store.insert_one("window", event)
        assert store.conn.in_transaction
        # Bulk insertion routes events carrying IDs through replace; use a new event.
        event.id = None
        with pytest.raises((ValueError, sqlite3.IntegrityError)):
            insert_missing()
        assert store.conn.in_transaction
        store.commit()
        assert store.get_eventcount("window") == 1
    finally:
        store.conn.close()


def test_three_devices_can_create_the_same_exact_bucket_name(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "three.db"))
    try:
        for device in ("first", "second", "third"):
            scoped = store.for_device(device)
            scoped.create_bucket("window", "t", "c", device, "2026-01-01T00:00:00Z")
            scoped.insert_one(
                "window",
                Event(timestamp="2026-01-01T00:00:00Z", data={"device": device}),
            )
        assert set(store.device_buckets()) == {"first", "second", "third"}
        for device in ("first", "second", "third"):
            assert store.for_device(device).get_events("window", -1)[0].data == {
                "device": device
            }
    finally:
        store.conn.close()


def test_creation_locks_alias_checks_and_insert_together(tmp_path, monkeypatch):
    path = str(tmp_path / "create-race.db")
    store = SqliteStorage(testing=True, filepath=path)
    contender = sqlite3.connect(path, timeout=0)
    resolve = SqliteStorage._bucket_row
    attempts = []

    def competing_alias_creation(self, *args, **kwargs):
        row = resolve(self, *args, **kwargs)
        if not attempts:
            try:
                contender.execute(
                    "INSERT INTO buckets(id,device_id,type,client,hostname,created,datastr) VALUES ('window','local','t','c','host','2026-01-01T00:00:00Z','{}')"
                )
                contender.commit()
            except sqlite3.OperationalError as exc:
                assert "locked" in str(exc)
                contender.rollback()
                attempts.append("blocked")
            else:
                attempts.append("created")
        return row

    monkeypatch.setattr(SqliteStorage, "_bucket_row", competing_alias_creation)
    try:
        store.for_device("local").create_bucket(
            "window_host", "t", "c", "host", "2026-01-01T00:00:00Z"
        )
        assert attempts == ["blocked"]
        assert store.conn.execute("SELECT id FROM buckets").fetchall() == [
            ("window_host",)
        ]
        assert not store.conn.in_transaction
    finally:
        contender.close()
        store.conn.close()


@pytest.mark.parametrize(
    "operation", ["get_event", "get_events", "get_eventcount", "iter_events"]
)
def test_snapshot_ordinary_reads_keep_the_original_view(tmp_path, operation):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "mixed-snapshot.db"))
    store.create_bucket("window", "t", "c", "h", "2026-01-01T00:00:00Z")
    first = store.insert_one(
        "window", Event(timestamp="2026-01-01T00:00:00Z", data={"view": "old"})
    )
    store.commit()
    snapshot = store.snapshot()
    scoped = snapshot.for_device("local")
    try:
        if operation == "get_event":
            assert scoped.get_event("window", first.id).data == {"view": "old"}
        elif operation == "get_events":
            assert len(scoped.get_events("window", -1)) == 1
        elif operation == "get_eventcount":
            assert scoped.get_eventcount("window") == 1
        else:
            assert len(list(scoped.iter_events("window"))) == 1
        store.insert_one(
            "window", Event(timestamp="2026-01-01T00:00:01Z", data={"view": "new"})
        )
        store.commit()
        assert len(list(scoped.iter_events("window", flush=False))) == 1
        assert snapshot.conn.in_transaction
        assert store.get_eventcount("window") == 2
    finally:
        snapshot.conn.close()
        store.conn.close()
