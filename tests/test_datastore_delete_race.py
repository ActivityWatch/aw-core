"""A bucket lookup that races a bucket deletion must not resurrect the bucket.

Datastore caches Bucket handles. delete_bucket used to drop the cached handle
*before* the storage delete finished, so a concurrent lookup (another request
thread in aw-server) still saw the bucket in storage and cached a fresh handle.
After the delete, that stale handle made every request for the bucket fail
against storage instead of raising KeyError (a 404 in aw-server).
"""

import threading
from datetime import datetime, timezone

import pytest

from aw_datastore import Datastore
from aw_datastore.storages.memory import MemoryStorage


class _SlowDeleteStorage(MemoryStorage):
    """Blocks inside delete_bucket until the test releases it."""

    def __init__(self, testing: bool) -> None:
        super().__init__(testing)
        self.delete_started = threading.Event()
        self.release_delete = threading.Event()

    def delete_bucket(self, bucket_id: str) -> None:
        self.delete_started.set()
        assert self.release_delete.wait(5)
        super().delete_bucket(bucket_id)


@pytest.mark.parametrize("cached", [True, False], ids=["cached", "uncached"])
def test_lookup_during_delete_does_not_leave_zombie_bucket(cached):
    ds = Datastore(storage_strategy=_SlowDeleteStorage, testing=True)
    storage = ds.storage_strategy
    ds.create_bucket("b", "test", "test", "test", created=datetime.now(timezone.utc))
    if not cached:
        ds.bucket_instances.clear()

    deleter = threading.Thread(target=ds.delete_bucket, args=("b",))
    deleter.start()
    assert storage.delete_started.wait(5)

    lookup_started = threading.Event()
    lookup_result = []

    def lookup():
        lookup_started.set()
        try:
            bucket = ds["b"]
            lookup_result.append(bucket.bucket_id)
        except KeyError:
            lookup_result.append("missing")

    looker = threading.Thread(target=lookup)
    looker.start()
    try:
        assert lookup_started.wait(5)
        if cached:
            looker.join(5)
            assert not looker.is_alive()
        else:
            # An uncached lookup must wait instead of returning a soon-stale handle.
            looker.join(0.2)
            assert looker.is_alive()
            assert lookup_result == []
    finally:
        storage.release_delete.set()
        deleter.join(5)
        looker.join(5)
    assert not deleter.is_alive() and not looker.is_alive()
    assert lookup_result == (["b"] if cached else ["missing"])

    assert "b" not in ds.buckets()
    with pytest.raises(KeyError):
        assert ds["b"] is not None
