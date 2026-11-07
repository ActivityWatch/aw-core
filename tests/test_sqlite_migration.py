import os
import sqlite3
from datetime import datetime, timezone

import pytest

from aw_datastore.migration import sqlite_v1_to_v2
from aw_datastore.storages import AbstractStorage


@pytest.fixture
def v1_db_path(tmpdir):
    """A v1-schema sqlite db (no device_id, UNIQUE(id)) with one bucket+events."""
    path = os.path.join(str(tmpdir), "sqlite-testing.v1.db")
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE buckets ("
        "  id TEXT NOT NULL UNIQUE,"
        "  name TEXT,"
        "  type TEXT NOT NULL,"
        "  client TEXT NOT NULL,"
        "  hostname TEXT NOT NULL,"
        "  created TEXT NOT NULL,"
        "  datastr TEXT NOT NULL"
        ")"
    )
    conn.execute(
        "CREATE TABLE events ("
        "  id INTEGER,"
        "  bucketrow INTEGER NOT NULL,"
        "  starttime INTEGER NOT NULL,"
        "  endtime INTEGER NOT NULL,"
        "  datastr TEXT NOT NULL"
        ")"
    )
    created = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO buckets VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "test-bucket",
            "test-bucket",
            "test.type",
            "test-client",
            "test-host",
            created,
            "{}",
        ],
    )
    bucketrow = conn.execute(
        "SELECT rowid FROM buckets WHERE id = 'test-bucket'"
    ).fetchone()[0]
    start_us = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
    for i in range(3):
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
            [
                i + 1,
                bucketrow,
                start_us + i * 1_000_000,
                start_us + (i + 1) * 1_000_000,
                "{}",
            ],
        )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def v2_datastore(tmpdir) -> AbstractStorage:
    from aw_datastore.storages import SqliteStorage

    return SqliteStorage(testing=True, filepath=str(tmpdir) + "/sqlite-testing.v2.db")


def test_migration_copies_buckets_and_events(v1_db_path, v2_datastore):
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    buckets = v2_datastore.buckets()
    assert list(buckets.keys()) == ["test-bucket"]
    assert buckets["test-bucket"]["device_id"] == "local"
    events = v2_datastore.get_events("test-bucket", -1)
    assert len(events) == 3


def test_v1_file_retained_after_migration(v1_db_path, v2_datastore):
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    # Keep the original path so any open writer and WAL remain associated.
    assert os.path.exists(v1_db_path)


def test_migration_rerun_is_idempotent(v1_db_path, v2_datastore):
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    # Re-detection must neither duplicate events nor restore deleted v2 data.
    assert os.path.exists(v1_db_path)

    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    assert list(v2_datastore.buckets().keys()) == ["test-bucket"]
    assert len(v2_datastore.get_events("test-bucket", -1)) == 3


def test_retry_restores_missing_events(v1_db_path, v2_datastore):
    # A previous attempt committed the bucket and only its first event.
    with sqlite3.connect(v1_db_path) as source:
        bucket = source.execute("SELECT * FROM buckets").fetchone()
        first = source.execute(
            "SELECT starttime, endtime, datastr FROM events LIMIT 1"
        ).fetchone()
    v2_datastore.create_bucket(bucket[0], bucket[2], bucket[3], bucket[4], bucket[5])
    v2_datastore.conn.execute(
        "INSERT INTO events(bucketrow, starttime, endtime, datastr) VALUES (1, ?, ?, ?)",
        first,
    )
    v2_datastore.commit()
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert len(v2_datastore.get_events("test-bucket", -1)) == 3
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert len(v2_datastore.get_events("test-bucket", -1)) == 3


def test_wal_source_remains_readable(v1_db_path, v2_datastore):
    source = sqlite3.connect(v1_db_path)
    try:
        source.execute("PRAGMA journal_mode=WAL")
        source.execute("PRAGMA wal_autocheckpoint=0")
        source.execute(
            "INSERT INTO events SELECT 4, bucketrow, starttime + 10000000, endtime + 10000000, datastr FROM events LIMIT 1"
        )
        source.commit()
        assert os.path.exists(v1_db_path + "-wal")
        sqlite_v1_to_v2(v2_datastore, v1_db_path)
        assert len(v2_datastore.get_events("test-bucket", -1)) == 4
        with sqlite3.connect(v1_db_path) as backup:
            assert backup.execute("SELECT count(*) FROM events").fetchone()[0] == 4
    finally:
        source.close()


def test_startup_retries_existing_v2(v1_db_path, v2_datastore, monkeypatch):
    from aw_datastore.storages import SqliteStorage

    monkeypatch.setattr(
        "aw_datastore.storages.sqlite.get_data_dir",
        lambda _: os.path.dirname(v1_db_path),
    )
    monkeypatch.setattr(
        "aw_datastore.migration.get_data_dir", lambda _: os.path.dirname(v1_db_path)
    )
    v2_datastore.conn.close()
    reopened = SqliteStorage(testing=True)
    try:
        assert len(reopened.get_events("test-bucket", -1)) == 3
    finally:
        reopened.conn.close()


def test_failure_rolls_back_copy_and_completion(v1_db_path, v2_datastore):
    v2_datastore.conn.execute(
        "CREATE TRIGGER interrupt_copy BEFORE INSERT ON events WHEN NEW.starttime > 1767225600000000 BEGIN SELECT RAISE(ABORT, 'interrupted'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="interrupted"):
        sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert not v2_datastore.buckets()
    assert v2_datastore.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 0
    assert (
        v2_datastore.conn.execute("SELECT count(*) FROM migrated_sources").fetchone()[0]
        == 0
    )
    assert os.path.exists(v1_db_path)
    v2_datastore.conn.execute("DROP TRIGGER interrupt_copy")
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert len(v2_datastore.get_events("test-bucket", -1)) == 3


def test_completion_does_not_restore_deleted_events(v1_db_path, v2_datastore):
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    v2_datastore.conn.execute("DELETE FROM events")
    v2_datastore.commit()
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert not v2_datastore.get_events("test-bucket", -1)


def test_identical_source_rows_keep_multiplicity(v1_db_path, v2_datastore):
    with sqlite3.connect(v1_db_path) as source:
        source.execute(
            "INSERT INTO events SELECT 4, bucketrow, starttime, endtime, datastr FROM events LIMIT 1"
        )
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert len(v2_datastore.get_events("test-bucket", -1)) == 4


def test_completion_is_durable_on_reopen(v1_db_path, v2_datastore):
    from aw_datastore.storages import SqliteStorage

    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    v2_datastore.conn.close()
    reopened = SqliteStorage(
        testing=True,
        filepath=os.path.join(os.path.dirname(v1_db_path), "sqlite-testing.v2.db"),
    )
    try:
        assert len(reopened.get_events("test-bucket", -1)) == 3
        reopened.conn.execute("DELETE FROM events")
        reopened.commit()
        sqlite_v1_to_v2(reopened, v1_db_path)
        assert not reopened.get_events("test-bucket", -1)
    finally:
        reopened.conn.close()


def test_failed_schema_rebuild_rolls_back_and_restores_foreign_keys(
    v1_db_path, monkeypatch
):
    from aw_datastore.storages import SqliteStorage

    connection = sqlite3.connect(v1_db_path)
    connection.execute("CREATE TABLE buckets_v2 (sentinel TEXT)")
    connection.commit()
    monkeypatch.setattr(
        "aw_datastore.storages.sqlite.sqlite3.connect", lambda _: connection
    )
    try:
        with pytest.raises(sqlite3.OperationalError, match="already exists"):
            SqliteStorage(testing=True, filepath=v1_db_path)
        assert not connection.in_transaction
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM buckets").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 3
    finally:
        connection.close()


def test_schema_rebuild_rolls_back_after_copy(v1_db_path, monkeypatch):
    from aw_datastore.storages import SqliteStorage

    connection = sqlite3.connect(v1_db_path)
    connection.set_authorizer(
        lambda action, *_: (
            sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_DROP_TABLE
            else sqlite3.SQLITE_OK
        )
    )
    monkeypatch.setattr(
        "aw_datastore.storages.sqlite.sqlite3.connect", lambda _: connection
    )
    try:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            SqliteStorage(testing=True, filepath=v1_db_path)
        assert not connection.in_transaction
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM buckets").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 3
        assert not connection.execute(
            "SELECT name FROM sqlite_master WHERE name = 'buckets_v2'"
        ).fetchall()
    finally:
        connection.close()


def test_migration_local_bucket_coexists_with_remote(v1_db_path, v2_datastore):
    v2_datastore.create_bucket(
        "test-bucket",
        "test.type",
        "remote-client",
        "remote-host",
        "2026-01-01T00:00:00+00:00",
        device_id="remote",
    )
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    assert v2_datastore.conn.execute(
        "SELECT device_id FROM buckets ORDER BY device_id"
    ).fetchall() == [("local",), ("remote",)]
    assert (
        v2_datastore.conn.execute(
            "SELECT count(*) FROM events JOIN buckets ON events.bucketrow = buckets.rowid "
            "WHERE device_id = 'local'"
        ).fetchone()[0]
        == 3
    )


def test_successful_in_place_rebuild_preserves_sparse_bucket_rows(v1_db_path):
    from aw_datastore.storages import SqliteStorage

    with sqlite3.connect(v1_db_path) as source:
        source.execute("UPDATE buckets SET rowid = 7")
        source.execute("UPDATE events SET bucketrow = 7")
        source.execute(
            "INSERT INTO buckets (rowid, id, name, type, client, hostname, created, datastr) "
            "SELECT 23, 'other', name, type, client, hostname, created, datastr FROM buckets"
        )
        source.execute(
            "INSERT INTO events VALUES (19, 23, 1767225600000000, 1767225601000000, '{\"other\": true}')"
        )
    store = SqliteStorage(testing=True, filepath=v1_db_path)
    try:
        assert store.conn.execute(
            "SELECT rowid FROM buckets ORDER BY rowid"
        ).fetchall() == [(7,), (23,)]
        assert len(store.for_device("local").get_events("test-bucket", -1)) == 3
        assert store.get_events("other", -1)[0].data == {"other": True}
        peer = store.for_device("peer")
        peer.create_bucket(
            "test-bucket", "test", "client", "peer-host", "2026-01-01T00:00:00+00:00"
        )
        assert peer.get_eventcount("test-bucket") == 0
        assert store.conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.conn.close()
    reopened = SqliteStorage(testing=True, filepath=v1_db_path)
    try:
        assert reopened.for_device("local").get_eventcount("test-bucket") == 3
        assert reopened.for_device("peer").get_eventcount("test-bucket") == 0
        assert reopened.get_events("other", -1)[0].data == {"other": True}
        assert reopened.conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        reopened.conn.close()


def test_device_migration_preserves_exact_archive_rows(v1_db_path):
    from aw_datastore.storages import SqliteStorage

    with sqlite3.connect(v1_db_path) as source:
        source.execute(
            "INSERT INTO events VALUES (99, 1, ?, ?, ?)",
            (-315619199876543, -315619199876540, '{ "archive": "unchanged bytes" }'),
        )
        original = source.execute(
            "SELECT id,bucketrow,starttime,endtime,datastr FROM events ORDER BY id"
        ).fetchall()
        names = source.execute("SELECT rowid,id FROM buckets").fetchall()
    store = SqliteStorage(testing=True, filepath=v1_db_path)
    try:
        assert (
            store.conn.execute(
                "SELECT id,bucketrow,starttime,endtime,datastr FROM events ORDER BY id"
            ).fetchall()
            == original
        )
        assert store.conn.execute("SELECT rowid,id FROM buckets").fetchall() == names
        events = store.for_device("local").get_events("test-bucket", -1)
        assert len(events) == 4
        archived = next(event for event in events if event.id == 99)
        assert archived.timestamp.isoformat() == "1960-01-01T00:00:00.123457+00:00"
        assert archived.duration.total_seconds() == 0.000003
    finally:
        store.conn.close()


def test_migration_discovery_ignores_wal_companions_first(
    v1_db_path, v2_datastore, monkeypatch
):
    from aw_datastore.migration import check_for_migration, detect_db_files

    directory = os.path.dirname(v1_db_path)
    main = os.path.basename(v1_db_path)
    filenames = [main + "-wal", main + "-shm", "unrelated", main, main + ".bak"]
    monkeypatch.setattr("aw_datastore.migration.os.listdir", lambda _: filenames)
    monkeypatch.setattr("aw_datastore.migration.get_data_dir", lambda _: directory)
    assert detect_db_files(directory, "sqlite-testing", 1) == [main]
    check_for_migration(v2_datastore)
    assert v2_datastore.get_eventcount("test-bucket") == 3
