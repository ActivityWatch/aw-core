import glob
import os
import sqlite3
import stat
from datetime import datetime, timedelta, timezone

import pytest
from aw_core.models import Event
from aw_datastore.storages.peewee import PeeweeStorage, _db
from aw_datastore.storages.sqlite_recover import (
    AUTO_RECOVER_ENV,
    SqliteRecoverError,
    _assert_recovered_schema,
    _reconstruct_missing_buckets,
    is_sqlite_healthy,
    maybe_recover_malformed_sqlite,
    sanitize_dump_sql,
)


def _checkpoint_and_close(path: str) -> None:
    if not _db.is_closed():
        _db.close()
    con = sqlite3.connect(path)
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    for suffix in ("-wal", "-shm"):
        extra = path + suffix
        if os.path.exists(extra) and os.path.getsize(extra) == 0:
            os.remove(extra)


def _corrupt_sidecars(path: str):
    """Sidecar DB copies only — WAL/SHM use the same prefix on Windows."""
    skip = ("-wal", "-shm", "-journal")
    return [p for p in glob.glob(path + ".corrupt-*") if not p.endswith(skip)]


def _xor_corrupt(path: str, offset: int = 4096, length: int = 200) -> None:
    data = bytearray(open(path, "rb").read())
    end = min(offset + length, len(data))
    assert end > offset, "fixture too small to corrupt at offset"
    for i in range(offset, end):
        data[i] ^= 0xFF
    open(path, "wb").write(data)


def _seed_peewee_db(path: str, n_events: int = 30) -> None:
    if not _db.is_closed():
        _db.close()
    store = PeeweeStorage(testing=True, filepath=path)
    store.create_bucket(
        "aw-watcher-window",
        "currentwindow",
        "aw-watcher-window",
        "host",
        datetime.now(timezone.utc).isoformat(),
        name="window",
    )
    now = datetime.now(timezone.utc)
    events = [
        Event(
            timestamp=now + timedelta(seconds=i),
            duration=timedelta(seconds=1),
            data={"app": f"t{i}", "title": "x"},
        )
        for i in range(n_events)
    ]
    store.insert_many("aw-watcher-window", events)
    assert store.get_eventcount("aw-watcher-window") == n_events
    _checkpoint_and_close(path)


def test_sanitize_dump_sql_rewrites_rollback_and_drops_corruption_markers():
    sql = (
        "PRAGMA foreign_keys=OFF;\n"
        "BEGIN TRANSACTION;\n"
        "CREATE TABLE t(a);\n"
        "/****** CORRUPTION ERROR *******/\n"
        "INSERT INTO t VALUES(1);\n"
        "ROLLBACK; -- due to errors\n"
    )
    out = sanitize_dump_sql(sql)
    assert "CORRUPTION ERROR" not in out
    assert "ROLLBACK;" not in out
    assert "COMMIT;" in out
    assert "INSERT INTO t VALUES(1);" in out


def test_is_sqlite_healthy_missing_and_ok(tmp_path):
    missing = str(tmp_path / "nope.db")
    assert is_sqlite_healthy(missing)
    path = str(tmp_path / "ok.db")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE t(a)")
    con.commit()
    con.close()
    assert is_sqlite_healthy(path)


def test_maybe_recover_noop_on_healthy_db(tmp_path):
    path = str(tmp_path / "ok.db")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE t(a)")
    con.commit()
    con.close()
    assert maybe_recover_malformed_sqlite(path) is None
    assert _corrupt_sidecars(path) == []


def test_peewee_startup_recovers_xor_corrupt_db(tmp_path):
    path = str(tmp_path / "peewee-sqlite.v2.db")
    _seed_peewee_db(path, n_events=30)
    assert is_sqlite_healthy(path)
    _xor_corrupt(path)
    assert not is_sqlite_healthy(path)

    if not _db.is_closed():
        _db.close()
    store = PeeweeStorage(testing=True, filepath=path)
    try:
        sidecars = _corrupt_sidecars(path)
        assert len(sidecars) == 1
        assert os.path.exists(sidecars[0])
        assert is_sqlite_healthy(path)
        buckets = store.buckets()
        assert buckets, "recovered DB should expose at least one bucket"
        # Original string id survives when the bucket row is intact; otherwise
        # sqlite_recover reconstructs recovered-<key>.
        bucket_id = (
            "aw-watcher-window"
            if "aw-watcher-window" in buckets
            else next(iter(buckets))
        )
        assert store.get_eventcount(bucket_id) == 30
        events = store.get_events(bucket_id, limit=100)
        apps = {e.data["app"] for e in events}
        assert apps == {f"t{i}" for i in range(30)}
    finally:
        if not _db.is_closed():
            _db.close()


def test_python_fallback_without_sqlite_cli(tmp_path, monkeypatch):
    path = str(tmp_path / "peewee-sqlite.v2.db")
    _seed_peewee_db(path, n_events=30)
    _xor_corrupt(path)
    monkeypatch.setattr(
        "aw_datastore.storages.sqlite_recover._sqlite_bin", lambda: None
    )
    sidecar = maybe_recover_malformed_sqlite(path)
    assert sidecar
    assert os.path.exists(sidecar)
    assert is_sqlite_healthy(path)
    if not _db.is_closed():
        _db.close()
    store = PeeweeStorage(testing=True, filepath=path)
    try:
        buckets = store.buckets()
        assert buckets
        bucket_id = (
            "aw-watcher-window"
            if "aw-watcher-window" in buckets
            else next(iter(buckets))
        )
        assert store.get_eventcount(bucket_id) == 30
    finally:
        if not _db.is_closed():
            _db.close()


def test_auto_recover_disabled_raises(tmp_path, monkeypatch):
    path = str(tmp_path / "peewee-sqlite.v2.db")
    _seed_peewee_db(path, n_events=5)
    _xor_corrupt(path)
    monkeypatch.setenv(AUTO_RECOVER_ENV, "0")
    with pytest.raises(SqliteRecoverError, match="Auto-recovery disabled"):
        maybe_recover_malformed_sqlite(path)
    assert _corrupt_sidecars(path) == []


def _eventmodel_only_db(path: str, bucket_key: int = 7, n_events: int = 2) -> None:
    con = sqlite3.connect(path)
    con.execute(
        'CREATE TABLE "eventmodel" ('
        '"id" INTEGER NOT NULL PRIMARY KEY, '
        '"bucket_id" INTEGER NOT NULL, '
        '"timestamp" DATETIME NOT NULL, '
        '"duration" DECIMAL(10, 5) NOT NULL, '
        '"datastr" VARCHAR(255) NOT NULL)'
    )
    for i in range(n_events):
        con.execute(
            "INSERT INTO eventmodel (id, bucket_id, timestamp, duration, datastr) "
            "VALUES (?, ?, ?, ?, ?)",
            (i + 1, bucket_key, f"2020-01-01 00:00:0{i}+00:00", 1, "{}"),
        )
    con.commit()
    con.close()


def test_reconstruct_creates_bucketmodel_when_missing(tmp_path):
    path = str(tmp_path / "partial.db")
    _eventmodel_only_db(path, bucket_key=7, n_events=2)
    assert _reconstruct_missing_buckets(path) == 1
    con = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "bucketmodel" in tables
        row = con.execute('SELECT "key", id, type, client FROM bucketmodel').fetchone()
        assert row[0] == 7
        assert row[1] == "recovered-7"
        assert row[2] == "unknown"
        assert row[3] == "sqlite-recover"
        assert con.execute("SELECT count(*) FROM eventmodel").fetchone()[0] == 2
        missing = con.execute(
            """
            SELECT count(*) FROM eventmodel e
            LEFT JOIN bucketmodel b ON b."key" = e.bucket_id
            WHERE b."key" IS NULL
            """
        ).fetchone()[0]
        assert missing == 0
    finally:
        con.close()
    _assert_recovered_schema(path)


def test_assert_recovered_schema_rejects_events_without_buckets(tmp_path):
    path = str(tmp_path / "partial.db")
    _eventmodel_only_db(path)
    with pytest.raises(SqliteRecoverError, match="no bucketmodel"):
        _assert_recovered_schema(path)


@pytest.mark.skipif(
    os.name == "nt", reason="POSIX permission bits are not preserved on Windows"
)
def test_recovery_preserves_restrictive_mode(tmp_path):
    path = str(tmp_path / "peewee-sqlite.v2.db")
    _seed_peewee_db(path, n_events=5)
    os.chmod(path, 0o600)
    _xor_corrupt(path)
    sidecar = maybe_recover_malformed_sqlite(path)
    assert sidecar
    assert is_sqlite_healthy(path)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
