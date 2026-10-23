from datetime import datetime, timedelta, timezone

import pytest

from aw_core.models import Event
from aw_datastore.storages import SqliteStorage


def test_scoped_storage_keeps_same_name_histories_separate(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "devices.db"))
    local = store.for_device("local")
    peer = store.for_device("peer")
    for scoped, source in [(local, "local"), (peer, "peer")]:
        scoped.create_bucket(
            "window", "test", "client", source, "2026-01-01T00:00:00+00:00"
        )
        event = scoped.insert_one(
            "window",
            Event(
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                duration=timedelta(seconds=1),
                data={"source": source},
            ),
        )
        assert scoped.get_metadata("window")["device_id"] == source
        assert scoped.get_event("window", event.id).data == {"source": source}
    assert local.get_events("window", -1)[0].data == {"source": "local"}
    assert peer.get_eventcount("window") == 1
    assert set(store.device_buckets()) == {"local", "peer"}
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        store.delete_bucket("window")
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        store.buckets()
    assert peer.get_eventcount("window") == 1
    local.delete_bucket("window")
    assert not local.has_bucket("window")
    assert peer.get_eventcount("window") == 1
    peer.commit()
    reopened = SqliteStorage(testing=True, filepath=str(tmp_path / "devices.db"))
    assert reopened.for_device("peer").get_events("window", -1)[0].data == {
        "source": "peer"
    }


def test_scoped_datastore_objects_do_not_reuse_other_device_cache(tmp_path):
    from aw_datastore import Datastore

    datastore = Datastore(
        SqliteStorage, testing=True, filepath=str(tmp_path / "cache.db")
    )
    local = datastore.for_device("local")
    peer = datastore.for_device("peer")
    for scoped, host in [(local, "host"), (peer, "peer-host")]:
        bucket = scoped.create_bucket(
            "window", type="test", client="test", hostname=host
        )
        bucket.insert(
            Event(
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                duration=timedelta(seconds=2),
                data={"host": host},
            )
        )
        assert bucket.metadata()["hostname"] == host
    assert local["window"] is not peer["window"]
    assert local["window"].get()[0].data == {"host": "host"}
    assert peer["window"].get()[0].data == {"host": "peer-host"}
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        datastore["window"]
    local.delete_bucket("window")
    assert peer["window"].get_eventcount() == 1


def test_scoped_event_mutations_never_cross_bucketrow(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "events.db"))
    local, peer = store.for_device("local"), store.for_device("peer")
    for scoped in [local, peer]:
        scoped.create_bucket(
            "window", "test", "client", "host", "2026-01-01T00:00:00+00:00"
        )
        scoped.insert_many(
            "window",
            [
                Event(
                    timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    duration=timedelta(seconds=1),
                    data={"original": True},
                )
            ],
        )
    local_event, peer_event = (
        local.get_events("window", -1)[0],
        peer.get_events("window", -1)[0],
    )
    assert not local.delete("window", peer_event.id)
    assert not local.replace("window", peer_event.id, local_event)
    changed = Event(
        timestamp=local_event.timestamp,
        duration=timedelta(seconds=4),
        data={"changed": True},
    )
    local.replace_last("window", changed)
    assert list(local.iter_events("window"))[0].duration == timedelta(seconds=4)
    assert peer.get_events("window", -1)[0].data == {"original": True}
    local.update_bucket("window", hostname="renamed")
    assert peer.get_metadata("window")["hostname"] == "host"
    for operation in [
        store.get_metadata,
        lambda b: store.insert_one(b, changed),
        lambda b: store.update_bucket(b, hostname="wrong"),
        lambda b: store.replace_last(b, changed),
    ]:
        with pytest.raises(ValueError, match="[Aa]mbiguous"):
            operation("window")
    assert local.delete("window", local_event.id)
    assert local.get_eventcount("window") == 0
    assert peer.get_eventcount("window") == 1


def test_empty_device_rejected_before_creation(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "empty.db"))
    with pytest.raises(ValueError, match="empty"):
        store.create_bucket(
            "bad", "test", "client", "host", "2026-01-01T00:00:00+00:00", device_id=""
        )
    assert not store.has_bucket("bad")


def test_missing_bucket_event_contracts_remain_unchanged(tmp_path):
    store = SqliteStorage(
        testing=True, filepath=str(tmp_path / "missing.db")
    ).for_device("peer")
    event = Event(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration=timedelta(seconds=1),
        data={},
    )
    assert store.get_event("missing", 1) is None
    assert store.get_events("missing", -1) == []
    assert store.get_eventcount("missing") == 0
    assert list(store.iter_events("missing")) == []
    assert not store.delete("missing", 1)
    assert not store.replace("missing", 1, event)
    store.insert_many("missing", [])


def test_cached_global_bucket_cannot_bypass_new_ambiguity(tmp_path):
    from aw_datastore import Datastore

    datastore = Datastore(
        SqliteStorage, testing=True, filepath=str(tmp_path / "cached-global.db")
    )
    datastore.create_bucket("window", type="test", client="test", hostname="host")
    datastore.for_device("peer").create_bucket(
        "window", type="test", client="test", hostname="other"
    )
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        datastore["window"]
