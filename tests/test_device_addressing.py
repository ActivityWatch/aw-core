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
        datastore.__getitem__("window")
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
    deleted = local.delete("window", peer_event.id)
    assert not deleted
    replaced = local.replace("window", peer_event.id, local_event)
    assert not replaced
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
    deleted = local.delete("window", local_event.id)
    assert deleted
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
    deleted = store.delete("missing", 1)
    assert not deleted
    replaced = store.replace("missing", 1, event)
    assert not replaced
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
        datastore.__getitem__("window")


def test_metadata_aliases_resolve_both_directions_without_renaming(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "aliases.db"))
    local = store.for_device("local")
    local.create_bucket(
        "window_host_with_underscores",
        "test",
        "client",
        "host_with_underscores",
        "2026-01-01T00:00:00+00:00",
    )
    event = Event(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration=1,
        data={"history": True},
    )
    local.insert_one("window", event)
    assert local.get_metadata("window")["id"] == "window_host_with_underscores"
    assert local.get_events("window", -1)[0].data == {"history": True}
    peer = store.for_device("peer")
    peer.create_bucket(
        "editor", "test", "client", "peer_host", "2026-01-01T00:00:00+00:00"
    )
    assert peer.get_metadata("editor_peer_host")["id"] == "editor"
    assert not peer.has_bucket("window")
    assert not store.has_bucket("window_host_with_underscores_unknown")


def test_alias_creation_cannot_split_history_or_acknowledge_peer_target(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "creation.db"))
    local = store.for_device("local")
    local.create_bucket(
        "window_host", "test", "client", "host", "2026-01-01T00:00:00+00:00"
    )
    with pytest.raises(ValueError, match="already exists"):
        local.create_bucket(
            "window", "test", "client", "host", "2026-01-01T00:00:00+00:00"
        )
    assert set(local.buckets()) == {"window_host"}
    # An old database can already contain the exact-first conflict. New
    # creation must not acknowledge a different scoped alias on that database.
    store.conn.execute(
        "INSERT INTO buckets (id, device_id, type, client, hostname, created, datastr) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "window",
            "peer",
            "test",
            "client",
            "other",
            "2026-01-01T00:00:00+00:00",
            "{}",
        ),
    )
    store.commit()
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        store.create_bucket(
            "window", "test", "client", "host", "2026-01-01T00:00:00+00:00"
        )
    assert store.get_metadata("window")["device_id"] == "peer"
    assert local.get_metadata("window")["id"] == "window_host"


def test_alias_resolution_is_exact_first_and_ambiguous_without_mutation(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "ambiguous.db"))
    local, peer = store.for_device("local"), store.for_device("peer")
    for scoped, bid, host in [
        (local, "window_host", "host"),
        (peer, "window_other", "other"),
    ]:
        scoped.create_bucket(bid, "test", "client", host, "2026-01-01T00:00:00+00:00")
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        store.delete_bucket("window")
    assert len(store.device_buckets()) == 2
    local.create_bucket("editor", "test", "client", "host", "2026-01-01T00:00:00+00:00")
    peer.create_bucket(
        "editor_peer", "test", "client", "peer", "2026-01-01T00:00:00+00:00"
    )
    assert store.get_metadata("editor")["device_id"] == "local"
    unknown = store.for_device("unknown-device")
    unknown.create_bucket(
        "browser_unknown", "test", "client", "unknown", "2026-01-01T00:00:00+00:00"
    )
    assert not unknown.has_bucket("browser")


def test_peer_exact_creation_cannot_redirect_existing_global_alias(tmp_path):
    store = SqliteStorage(testing=True, filepath=str(tmp_path / "shadow.db"))
    store.for_device("local").create_bucket(
        "window_host", "test", "client", "host", "2026-01-01T00:00:00+00:00"
    )
    with pytest.raises(ValueError, match="[Aa]mbiguous"):
        store.for_device("peer").create_bucket(
            "window", "test", "client", "other", "2026-01-01T00:00:00+00:00"
        )
    assert store.get_metadata("window")["id"] == "window_host"
    assert not store.for_device("peer").has_bucket("window")


def test_stream_iterator_can_preserve_caller_snapshot(tmp_path):
    import sqlite3

    store = SqliteStorage(testing=True, filepath=str(tmp_path / "snapshot.db"))
    store.conn.execute("PRAGMA journal_mode=WAL")
    store.create_bucket("window", "t", "c", "h", "2024-01-01T00:00:00Z")
    store.commit()
    store.conn.execute("BEGIN")
    assert store.has_bucket("window")
    peer = sqlite3.connect(str(tmp_path / "snapshot.db"))
    row = peer.execute("SELECT rowid FROM buckets").fetchone()[0]
    peer.execute(
        "INSERT INTO events(bucketrow,starttime,endtime,datastr) VALUES (?, ?, ?, ?)",
        (row, 1704110400000000, 1704110401000000, "{}"),
    )
    peer.commit()
    try:
        assert list(store.iter_events("window", flush=False)) == []
        assert store.conn.in_transaction
        store.conn.commit()
        assert len(list(store.iter_events("window"))) == 1
    finally:
        peer.close()
        store.conn.close()
