import json
import logging
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import List, Optional

from aw_core.dirs import get_data_dir, legacy_testing_suffix

from .storages import AbstractStorage, SqliteStorage

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


def check_for_migration(datastore: AbstractStorage, migrate_peewee: bool = True):
    data_dir = get_data_dir("aw-server")

    if datastore.sid == "sqlite":
        suffix = legacy_testing_suffix(datastore.testing)

        # Prefer SQLite v1 (most recent); only fall back to Peewee if no SQLite v1.
        # Running both migrations into the same destination causes IntegrityError when
        # shared bucket IDs collide on UNIQUE(device_id, id).
        sqlite_name = "sqlite" + suffix
        sqlite_v1_files = detect_db_files(data_dir, sqlite_name, 1)
        if len(sqlite_v1_files) > 0:
            v1_path = os.path.join(data_dir, sqlite_v1_files[0])
            sqlite_v1_to_v2(datastore, v1_path)
        else:
            peewee_type = "peewee-sqlite"
            peewee_name = peewee_type + suffix
            peewee_db_v2 = detect_db_files(data_dir, peewee_name, 2)
            if len(peewee_db_v2) > 0 and migrate_peewee:
                peewee_v2_to_sqlite_v1(datastore)


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
    """Copy v1 atomically, retaining the source (including its WAL) untouched."""
    if not isinstance(datastore, SqliteStorage):
        raise TypeError("SQLite v1 migration requires SqliteStorage")
    destination = datastore.conn
    source_path = str(Path(v1_path).resolve())
    destination.execute(
        "CREATE TABLE IF NOT EXISTS migrated_sources (path TEXT PRIMARY KEY)"
    )
    if destination.execute(
        "SELECT 1 FROM migrated_sources WHERE path = ?", (source_path,)
    ).fetchone():
        return

    logger.info(f"Migrating SQLite v1 → v2: {v1_path}")
    source = sqlite3.connect(Path(source_path).as_uri() + "?mode=ro", uri=True)
    try:
        # One source snapshot and one destination transaction. Do not call storage
        # methods here: create_bucket/insert_many may commit midway through a copy.
        source.execute("BEGIN")
        with destination:
            buckets = source.execute(
                "SELECT rowid, id, name, type, client, hostname, created, datastr FROM buckets"
            ).fetchall()
            for row in buckets:
                (
                    source_rowid,
                    bucket_id,
                    name,
                    type_,
                    client,
                    hostname,
                    created,
                    datastr,
                ) = row
                destination.execute(
                    "INSERT OR IGNORE INTO buckets(id, device_id, name, type, client, hostname, created, datastr) "
                    "VALUES (?, 'local', ?, ?, ?, ?, ?, ?)",
                    (bucket_id, name, type_, client, hostname, created, datastr),
                )
                bucketrow = destination.execute(
                    "SELECT rowid FROM buckets WHERE device_id = 'local' AND id = ?",
                    (bucket_id,),
                ).fetchone()[0]

                # Recover destinations partially copied by older migration code.
                # Match multiplicities, not just a set: identical source events
                # are distinct rows. Keep unrelated destination events intact.
                def event_key(event):
                    start, end, data = event
                    return start, end, json.dumps(json.loads(data), sort_keys=True)

                existing = Counter(
                    event_key(event)
                    for event in destination.execute(
                        "SELECT starttime, endtime, datastr FROM events WHERE bucketrow = ?",
                        (bucketrow,),
                    )
                )
                for event in source.execute(
                    "SELECT starttime, endtime, datastr FROM events WHERE bucketrow = ? ORDER BY id",
                    (source_rowid,),
                ):
                    key = event_key(event)
                    if existing[key]:
                        existing[key] -= 1
                    else:
                        # Copy integer microseconds and JSON verbatim; new event
                        # IDs avoid collisions with pre-existing destination rows.
                        destination.execute(
                            "INSERT INTO events(bucketrow, starttime, endtime, datastr) VALUES (?, ?, ?, ?)",
                            (bucketrow, *event),
                        )
            # Completion commits alongside the data, never before it. Retaining
            # the source avoids unsafe main-file-only renames of WAL databases.
            destination.execute(
                "INSERT INTO migrated_sources(path) VALUES (?)", (source_path,)
            )
    finally:
        source.close()
    logger.info("Migration SQLite v1 → v2 finished")
