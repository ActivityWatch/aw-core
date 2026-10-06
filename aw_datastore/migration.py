import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from aw_core.dirs import get_data_dir, legacy_testing_suffix

from .storages import AbstractStorage

logger = logging.getLogger(__name__)


def detect_db_files(
    data_dir: str, datastore_name: Optional[str] = None, version=None
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
            filename for filename in db_files if filename.split(".")[1] == f"v{version}"
        ]
    return db_files


def check_for_migration(datastore: AbstractStorage):
    data_dir = get_data_dir("aw-server")

    if datastore.sid == "sqlite":
        suffix = legacy_testing_suffix(datastore.testing)

        peewee_type = "peewee-sqlite"
        peewee_name = peewee_type + suffix
        # Migrate from peewee v2
        peewee_db_v2 = detect_db_files(data_dir, peewee_name, 2)
        if len(peewee_db_v2) > 0:
            peewee_v2_to_sqlite_v1(datastore)

        # Migrate from sqlite v1 to v2
        sqlite_name = "sqlite" + suffix
        sqlite_v1_files = detect_db_files(data_dir, sqlite_name, 1)
        if len(sqlite_v1_files) > 0:
            v1_path = os.path.join(data_dir, sqlite_v1_files[0])
            sqlite_v1_to_v2(datastore, v1_path)


def peewee_v2_to_sqlite_v1(datastore):
    logger.info("Migrating database from peewee v2 to sqlite v1")
    from .storages import PeeweeStorage

    pw_db = PeeweeStorage(datastore.testing)
    # Fetch buckets and events
    buckets = pw_db.buckets()
    # Insert buckets and events to new db
    for bucket_id in buckets:
        logger.info(f"Migrating bucket {bucket_id}")
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


def sqlite_v1_to_v2(datastore: AbstractStorage, v1_path: str) -> None:
    """Migrate SQLite v1 db (no device_id) to v2 (UNIQUE(device_id, id))."""
    from aw_core.models import Event

    logger.info(f"Migrating SQLite v1 → v2: {v1_path}")
    conn = sqlite3.connect(v1_path)
    try:
        buckets = conn.execute(
            "SELECT id, name, type, client, hostname, created, datastr FROM buckets"
        ).fetchall()
        for row in buckets:
            bucket_id, name, type_, client, hostname, created, datastr = row
            logger.info(f"Migrating bucket {bucket_id}")
            data = json.loads(datastr or "{}")
            datastore.create_bucket(
                bucket_id,
                type_,
                client,
                hostname,
                created,
                name=name,
                data=data,
                device_id="local",
            )
            event_rows = conn.execute(
                "SELECT id, starttime, endtime, datastr FROM events "
                "WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?)",
                [bucket_id],
            ).fetchall()
            events = []
            for erow in event_rows:
                eid, starttime_us, endtime_us, event_datastr = erow
                starttime = datetime.fromtimestamp(
                    starttime_us / 1_000_000, timezone.utc
                )
                endtime = datetime.fromtimestamp(endtime_us / 1_000_000, timezone.utc)
                duration = endtime - starttime
                events.append(
                    Event(
                        id=eid,
                        timestamp=starttime,
                        duration=duration,
                        data=json.loads(event_datastr),
                    )
                )
            if events:
                datastore.insert_many(bucket_id, events)
    finally:
        conn.close()
    logger.info("Migration SQLite v1 → v2 finished")
