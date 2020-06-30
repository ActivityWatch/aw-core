import logging
import random
from datetime import datetime, timezone

from aw_datastore import Datastore, get_storage_methods

logging.basicConfig(level=logging.DEBUG)

# Useful when you just want some placeholder time in your events, saves typing
now = datetime.now(timezone.utc)


class TempTestBucket:
    """Context manager for creating a test bucket"""

    def __init__(self, datastore):
        self.ds = datastore
        self.bucket_id = "test-{}".format(random.randint(0, 10 ** 4))

    def __enter__(self):
        self.ds.create_bucket(
            bucket_id=self.bucket_id,
            type="testtype",
            client="testclient",
            hostname="testhost",
            name="testname",
        )
        return self.ds[self.bucket_id]

    def __exit__(self, *_):
        self.ds.delete_bucket(bucket_id=self.bucket_id)

    def __repr__(self):
        return "<TempTestBucket using {}>".format(
            self.ds.storage_strategy.__class__.__name__
        )


_storage_methods = get_storage_methods()


def param_datastore_objects():
    return [
        Datastore(storage_strategy=strategy, testing=True)
        for name, strategy in _storage_methods.items()
    ]


def param_testing_buckets_cm():
    datastores = [
        Datastore(storage_strategy=strategy, testing=True)
        for name, strategy in _storage_methods.items()
    ]
    return [TempTestBucket(ds) for ds in datastores]
