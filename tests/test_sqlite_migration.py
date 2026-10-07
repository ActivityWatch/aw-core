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
