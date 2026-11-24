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


@pytest.mark.parametrize("listing", ["buckets", "buckets_with_last_updated"])
def test_bucket_listing_rejects_cross_device_name_collision(
    v1_db_path, v2_datastore, listing
):
    v2_datastore.create_bucket(
        "test-bucket",
        "test.type",
        "remote-client",
        "remote-host",
        "2026-01-01T00:00:00+00:00",
        device_id="remote",
    )
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    with pytest.raises(ValueError, match="Ambiguous bucket id: test-bucket"):
        getattr(v2_datastore, listing)()


@pytest.mark.parametrize("listing", ["buckets", "buckets_with_last_updated"])
def test_bucket_listing_preserves_unambiguous_string_keys(v2_datastore, listing):
    for bucket_id, device_id in [
        ("local-bucket", "local"),
        ("remote-bucket", "remote"),
    ]:
        v2_datastore.create_bucket(
            bucket_id,
            "test.type",
            "test-client",
            "test-host",
            "2026-01-01T00:00:00+00:00",
            device_id=device_id,
        )

    buckets = getattr(v2_datastore, listing)()
    assert set(buckets) == {"local-bucket", "remote-bucket"}
    assert buckets["local-bucket"]["device_id"] == "local"
    assert buckets["remote-bucket"]["device_id"] == "remote"


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


def test_detect_db_files_ignores_sqlite_sidecars(tmpdir, monkeypatch):
    # os.listdir order is unspecified; a -wal/-shm sidecar listed first must
    # never be selected as the v1 database to migrate.
    names = [
        "sqlite.v1.db-wal",
        "sqlite.v1.db-shm",
        "sqlite.v1.db",
        "sqlite.v1.db-journal",
    ]
    for name in names:
        open(os.path.join(str(tmpdir), name), "w").close()
    monkeypatch.setattr(os, "listdir", lambda _: list(names))
    from aw_datastore.migration import detect_db_files

    assert detect_db_files(str(tmpdir), "sqlite", 1) == ["sqlite.v1.db"]


def test_migration_preserves_source_event_ids(v1_db_path, v2_datastore):
    # Event IDs are table-wide; callers address events by ID (get_event,
    # replace, delete), so the upgrade must not renumber them per bucket.
    with sqlite3.connect(v1_db_path) as source:
        source.execute(
            "INSERT INTO buckets SELECT 'other', 'other', type, client, hostname, "
            "created, datastr FROM buckets"
        )
        other = source.execute(
            "SELECT rowid FROM buckets WHERE id = 'other'"
        ).fetchone()[0]
        source.execute(
            "INSERT INTO events VALUES (7, ?, 0, 1000000, '{\"other\": true}')",
            (other,),
        )
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    assert [e.id for e in v2_datastore.get_events("test-bucket", -1)] == [3, 2, 1]
    assert v2_datastore.get_event("other", 7).data == {"other": True}


def test_recovery_reassigns_only_ids_taken_in_destination(v1_db_path, v2_datastore):
    with sqlite3.connect(v1_db_path) as source:
        bucket = source.execute("SELECT * FROM buckets").fetchone()
    v2_datastore.create_bucket(bucket[0], bucket[2], bucket[3], bucket[4], bucket[5])
    # An unrelated destination event already holds source event 2's ID.
    v2_datastore.conn.execute(
        "INSERT INTO events(id, bucketrow, starttime, endtime, datastr) "
        "VALUES (2, 1, 0, 1, '{\"unrelated\": true}')"
    )
    v2_datastore.commit()
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    ids = sorted(e.id for e in v2_datastore.get_events("test-bucket", -1))
    assert len(ids) == 4 and {1, 2, 3} <= set(ids)
    assert v2_datastore.get_event("test-bucket", 2).data == {"unrelated": True}
    # The replacement ID for source event 2 must not consume source event 3's ID.
    with sqlite3.connect(v1_db_path) as source:
        start3 = source.execute("SELECT starttime FROM events WHERE id = 3").fetchone()[
            0
        ]
    got3 = v2_datastore.conn.execute(
        "SELECT starttime FROM events WHERE id = 3"
    ).fetchone()[0]
    assert got3 == start3


def test_migration_keeps_source_event_id_high_water_mark(v1_db_path, v2_datastore):
    # v1 issued IDs up to 100 and later deleted 4..100: a new v2 event must not
    # reuse an ID a caller may still hold.
    with sqlite3.connect(v1_db_path) as source:
        source.execute("CREATE TABLE seq_probe (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        source.execute("INSERT INTO sqlite_sequence(name, seq) VALUES ('events', 100)")
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    from aw_core.models import Event

    new = v2_datastore.insert_one(
        "test-bucket", Event(timestamp=datetime.now(timezone.utc), data={})
    )
    assert new.id == 101


def test_replacement_ids_come_after_source_high_water_mark(v1_db_path, v2_datastore):
    with sqlite3.connect(v1_db_path) as source:
        bucket = source.execute("SELECT * FROM buckets").fetchone()
        source.execute("CREATE TABLE seq_probe (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        source.execute("INSERT INTO sqlite_sequence(name, seq) VALUES ('events', 100)")
    v2_datastore.create_bucket(bucket[0], bucket[2], bucket[3], bucket[4], bucket[5])
    v2_datastore.conn.execute(
        "INSERT INTO events(id, bucketrow, starttime, endtime, datastr) "
        "VALUES (2, 1, 0, 1, '{\"unrelated\": true}')"
    )
    v2_datastore.commit()
    sqlite_v1_to_v2(v2_datastore, v1_db_path)

    ids = sorted(e.id for e in v2_datastore.get_events("test-bucket", -1))
    assert ids == [1, 2, 3, 101]
