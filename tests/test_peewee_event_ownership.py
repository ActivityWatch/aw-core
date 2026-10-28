"""Explicit event IDs must not change bucket ownership in Peewee storage."""

import sqlite3
from contextlib import closing
from datetime import datetime, timezone

import pytest

from aw_core.models import Event
from aw_datastore.storages import PeeweeStorage


@pytest.fixture
def storage(tmp_path):
    storage = PeeweeStorage(testing=True, filepath=str(tmp_path / "events.db"))
    for bucket in ("source", "destination"):
        storage.create_bucket(
            bucket, "test", "pytest", "fixture", datetime.now(timezone.utc)
        )
    yield storage
    storage.db.close()


def event(label, event_id=None):
    return Event(
        id=event_id,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration=5,
        data={"label": label},
    )


def snapshot(storage):
    return {
        bucket: [e.to_json_dict() for e in storage.get_events(bucket, -1)]
        for bucket in ("source", "destination")
    }


@pytest.mark.parametrize("batch", [False, True])
def test_foreign_event_id_is_rejected(storage, batch):
    original = storage.insert_one("source", event("original"))
    before = snapshot(storage)
    foreign = event("replacement", original.id)
    with pytest.raises(ValueError, match="another bucket"):
        if batch:
            storage.insert_many("destination", [foreign])
        else:
            storage.insert_one("destination", foreign)
    assert snapshot(storage) == before


@pytest.mark.parametrize("foreign_first", [False, True])
def test_mixed_batch_rejection_does_not_apply_valid_updates_or_inserts(
    storage, foreign_first
):
    original = storage.insert_one("source", event("original"))
    existing = storage.insert_one("destination", event("existing"))
    before = snapshot(storage)
    valid = [event("new"), event("updated", existing.id)]
    foreign = event("foreign", original.id)
    events = [foreign, *valid] if foreign_first else [*valid, foreign]
    with pytest.raises(ValueError, match="another bucket"):
        storage.insert_many("destination", events)
    assert snapshot(storage) == before
    assert valid[0].id is None


@pytest.mark.parametrize("batch", [False, True])
def test_same_bucket_updates_and_idless_inserts_still_work(storage, batch):
    original = storage.insert_one("source", event("original"))
    replacement = event("updated", original.id)
    if batch:
        storage.insert_many("source", [replacement, event("new")])
        assert storage.get_eventcount("source") == 2
    else:
        assert storage.insert_one("source", replacement) is replacement
        assert storage.get_eventcount("source") == 1
    assert storage.get_event("source", original.id).data == {"label": "updated"}
    assert storage.get_eventcount("destination") == 0


@pytest.mark.parametrize("batch", [False, True])
def test_nonexistent_explicit_id_remains_a_noop(storage, batch):
    missing = event("missing", 123456)
    if batch:
        storage.insert_many("source", [missing, event("new")])
        assert storage.get_eventcount("source") == 1
    else:
        assert storage.insert_one("source", missing) is missing
        assert storage.get_eventcount("source") == 0
    assert storage.get_event("source", missing.id) is None


@pytest.mark.parametrize("batch", [False, True])
def test_same_bucket_update_with_competing_writer(storage, monkeypatch, batch):
    original = storage.insert_one("source", event("original"))
    check = storage._check_event_bucket
    attempted = False
    committed = False
    # A separate connection must not invalidate the ownership-check snapshot.
    with closing(sqlite3.connect(storage.db.database, timeout=0)) as competitor:

        def competing_insert():
            competitor.execute(
                "INSERT INTO eventmodel (bucket_id, timestamp, duration, datastr) "
                "SELECT bucket_id, timestamp, duration, datastr FROM eventmodel "
                "WHERE id = ?",
                (original.id,),
            )
            competitor.commit()

        def check_then_compete(bucket_id, candidate):
            nonlocal attempted, committed
            check(bucket_id, candidate)
            if not attempted:
                attempted = True
                try:
                    competing_insert()
                    committed = True
                except sqlite3.OperationalError as exc:
                    assert "locked" in str(exc)
                    competitor.rollback()

        monkeypatch.setattr(storage, "_check_event_bucket", check_then_compete)
        replacement = event("updated", original.id)
        if batch:
            storage.insert_many("source", [replacement, event("new")])
        else:
            storage.insert_one("source", replacement)
        assert attempted
        if not committed:
            competing_insert()

    assert storage.get_event("source", original.id).data == {"label": "updated"}
    assert storage.get_eventcount("source") == (3 if batch else 2)
    assert storage.get_eventcount("destination") == 0


def test_batch_write_failure_rolls_back_earlier_update(storage):
    existing = storage.insert_one("destination", event("existing"))
    before = snapshot(storage)
    invalid = event("invalid")
    invalid.data["unserializable"] = object()
    with pytest.raises(TypeError):
        storage.insert_many("destination", [event("updated", existing.id), invalid])
    assert snapshot(storage) == before
