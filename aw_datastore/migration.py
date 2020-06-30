from typing import Optional, List
import os
import re
import logging

from aw_core.dirs import get_data_dir
from .storages import AbstractStorage

logger = logging.getLogger(__name__)


def detect_db_files(
    data_dir: str, datastore_name: str = None, version=None
) -> List[str]:
    db_files = [filename for filename in os.listdir(data_dir)]
    if datastore_name:
        db_files = [
            filename
            for filename in db_files
            if filename.split(".")[0] == datastore_name
        ]
    if version:
        db_files = [
            filename
            for filename in db_files
            if filename.split(".")[1] == "v{}".format(version)
        ]
    return db_files


def check_for_migration(datastore: AbstractStorage):
    data_dir = get_data_dir("aw-server")

    if datastore.sid == "sqlite":
        peewee_type = "peewee-sqlite"
        peewee_name = peewee_type + ("-testing" if datastore.testing else "")
        # Migrate from peewee v2
        peewee_db_v2 = detect_db_files(data_dir, peewee_name, 2)
        if len(peewee_db_v2) > 0:
            peewee_v2_to_sqlite_v1(datastore)


def peewee_v2_to_sqlite_v1(datastore):
    logger.info("Migrating database from peewee v2 to sqlite v1")
    from .storages import PeeweeStorage

    pw_db = PeeweeStorage(datastore.testing)
    # Fetch buckets and events
    buckets = pw_db.buckets()
    # Insert buckets and events to new db
    for bucket_id in buckets:
        logger.info("Migrating bucket {}".format(bucket_id))
        bucket = buckets[bucket_id]
        datastore.create_bucket(
            bucket["id"],
            bucket["type"],
            bucket["client"],
            bucket["hostname"],
            bucket["created"],
            bucket["name"],
        )
        bucket_events = pw_db.get_events(bucket_id, -1)
        datastore.insert_many(bucket_id, bucket_events)
    logger.info("Migration of peewee v2 to sqlite v1 finished")
