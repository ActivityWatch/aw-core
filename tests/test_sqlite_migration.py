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


def test_v1_file_renamed_after_migration(v1_db_path, v2_datastore):
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    # The v1 file must no longer be detectable: the version token moved out of
    # the split(".")[1] position that detect_db_files() filters on.
    assert not os.path.exists(v1_db_path)
    renamed = v1_db_path.replace(".v1.", ".migrated-v1.", 1)
    assert os.path.exists(renamed)


def test_migration_rerun_is_idempotent(v1_db_path, v2_datastore):
    sqlite_v1_to_v2(v2_datastore, v1_db_path)
    # Simulate re-detection of the v1 file (e.g. rename failed on an earlier run):
    # re-running must not raise IntegrityError and must not duplicate data.
    os.rename(v1_db_path.replace(".v1.", ".migrated-v1.", 1), v1_db_path)

    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    assert list(v2_datastore.buckets().keys()) == ["test-bucket"]
    assert len(v2_datastore.get_events("test-bucket", -1)) == 3
