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
    # Only main database files: SQLite sidecars (-wal, -shm, -journal) share the
    # name and version components and must never be opened as a database.
    db_files = [
        filename for filename in os.listdir(data_dir) if filename.endswith(".db")
    ]
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
    # A destination upgrades from v1 once. The recorded path is provenance,
    # not source identity: moving both databases must not replay the copy and
    # resurrect events subsequently deleted in v2. This also recognizes
    # completion records written by earlier revisions of the migration.
    if destination.execute("SELECT 1 FROM migrated_sources LIMIT 1").fetchone():
        return

    logger.info(f"Migrating SQLite v1 → v2: {v1_path}")
    source = sqlite3.connect(Path(source_path).as_uri() + "?mode=ro", uri=True)
    try:
        # One source snapshot and one destination transaction. Do not call storage
        # methods here: create_bucket/insert_many may commit midway through a copy.
        source.execute("BEGIN")
        with destination:
            # Events whose source ID a recovered destination already holds. They
            # get fresh IDs only after every other source ID is placed, so a
            # replacement can never consume a later source event's ID.
            deferred = []
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
                for source_id, *event in source.execute(
                    "SELECT id, starttime, endtime, datastr FROM events WHERE bucketrow = ? ORDER BY id",
                    (source_rowid,),
                ):
                    key = event_key(event)
                    if existing[key]:
                        existing[key] -= 1
                    else:
                        # Copy ID, integer microseconds and JSON verbatim: callers
                        # address events by ID.
                        if destination.execute(
                            "SELECT 1 FROM events WHERE id = ?", (source_id,)
                        ).fetchone():
                            deferred.append((bucketrow, *event))
                            continue
                        destination.execute(
                            "INSERT INTO events(id, bucketrow, starttime, endtime, datastr) VALUES (?, ?, ?, ?, ?)",
                            (source_id, bucketrow, *event),
                        )
            # Carry the AUTOINCREMENT high-water mark: IDs of events deleted in v1
            # were already issued, so v2 must not hand them out again — including
            # to the deferred replacements below.
            try:
                row = source.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name = 'events'"
                ).fetchone()
            except sqlite3.OperationalError:  # source never used AUTOINCREMENT
                row = None
            if row:
                if not destination.execute(
                    "UPDATE sqlite_sequence SET seq = MAX(seq, ?) WHERE name = 'events'",
                    row,
                ).rowcount:
                    destination.execute(
                        "INSERT INTO sqlite_sequence(name, seq) VALUES ('events', ?)",
                        row,
                    )
            destination.executemany(
                "INSERT INTO events(bucketrow, starttime, endtime, datastr) VALUES (?, ?, ?, ?)",
                deferred,
            )
            # Completion commits alongside the data, never before it. Retaining
            # the source avoids unsafe main-file-only renames of WAL databases.
            destination.execute(
                "INSERT INTO migrated_sources(path) VALUES (?)", (source_path,)
            )
    finally:
        source.close()
    logger.info("Migration SQLite v1 → v2 finished")
