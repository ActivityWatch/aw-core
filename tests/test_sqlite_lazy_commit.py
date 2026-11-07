"""Lazy commits publish real writes at the time and statement boundaries."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from aw_core.models import Event
from aw_datastore.storages import sqlite as storage_module


@pytest.fixture
def database(tmp_path, monkeypatch):
    class Clock(datetime):
        current = datetime(2026, 1, 1, 12)

        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return cls.current
            return cls.current.replace(tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(storage_module, "datetime", Clock)
    path = tmp_path / "lazy.db"
    storage = storage_module.SqliteStorage(testing=True, filepath=str(path))
    storage.create_bucket(
        "test", "test-type", "test-client", "test-host", Clock.current.isoformat()
    )
    # Autocommit reads observe each new writer commit without keeping a snapshot.
    observer = sqlite3.connect(str(path), isolation_level=None)
    try:
        yield storage, observer, Clock
    finally:
        observer.close()
        storage.conn.close()


def _event():
    return Event(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration=timedelta(seconds=1),
        data={"app": "test"},
    )


def _visible_count(observer):
    # Storage read helpers may commit, which would hide the lazy-commit bug.
    return observer.execute("SELECT COUNT(*) FROM events").fetchone()[0]


@pytest.mark.parametrize(
    ("elapsed", "committed"),
    [
        (timedelta(seconds=1), False),
        (timedelta(seconds=10), False),
        (timedelta(seconds=10, microseconds=1), True),
        (timedelta(seconds=11), True),
    ],
    ids=["young", "exact-ten-seconds", "just-over-ten-seconds", "elapsed"],
)
def test_next_write_observes_elapsed_commit_boundary(database, elapsed, committed):
    storage, observer, clock = database
    storage.insert_one("test", _event())
    assert _visible_count(observer) == 0

    clock.current += elapsed
    # The policy runs on writes: advancing time alone does not publish anything.
    assert _visible_count(observer) == 0
    storage.insert_one("test", _event())

    assert _visible_count(observer) == (2 if committed else 0)
    assert storage.num_uncommitted_statements == (0 if committed else 2)
    if committed:
        assert storage.last_commit == clock.current


def test_insert_many_observes_elapsed_commit_boundary(database):
    storage, observer, clock = database
    storage.insert_one("test", _event())
    clock.current += timedelta(seconds=11)
    storage.insert_many("test", [_event(), _event()])

    assert _visible_count(observer) == 3
    assert storage.num_uncommitted_statements == 0
    assert storage.last_commit == clock.current


def test_statement_limit_and_commit_reset(database):
    storage, observer, clock = database
    clock.current += timedelta(seconds=1)
    for _ in range(50):
        storage.insert_one("test", _event())
    assert _visible_count(observer) == 0
    assert storage.num_uncommitted_statements == 50

    storage.insert_one("test", _event())
    assert _visible_count(observer) == 51
    assert storage.num_uncommitted_statements == 0
    assert storage.last_commit == clock.current

    # The next transaction gets a fresh statement count and ten-second window.
    clock.current += timedelta(seconds=10)
    storage.insert_one("test", _event())
    assert _visible_count(observer) == 51
    assert storage.num_uncommitted_statements == 1
    clock.current += timedelta(microseconds=1)
    storage.insert_one("test", _event())
    assert _visible_count(observer) == 53
    assert storage.num_uncommitted_statements == 0
    assert storage.last_commit == clock.current


def test_nonlazy_writes_are_visible_immediately(database):
    storage, observer, clock = database
    storage.enable_lazy_commit = False
    storage.insert_one("test", _event())
    assert _visible_count(observer) == 1
    storage.insert_many("test", [_event(), _event()])
    assert _visible_count(observer) == 3
    assert storage.num_uncommitted_statements == 0
    assert storage.last_commit == clock.current
