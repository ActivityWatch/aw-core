import copy
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

from aw_core.dirs import get_data_dir, legacy_testing_suffix
from aw_core.models import Event

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

LATEST_VERSION = 2

# The max integer value in SQLite is signed 8 Bytes / 64 bits
MAX_TIMESTAMP = 2**63 - 1

CREATE_BUCKETS_TABLE = """
    CREATE TABLE IF NOT EXISTS buckets (
        rowid INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL,
        device_id TEXT NOT NULL DEFAULT 'local',
        name TEXT,
        type TEXT NOT NULL,
        client TEXT NOT NULL,
        hostname TEXT NOT NULL,
        created TEXT NOT NULL,
        datastr TEXT NOT NULL,
        UNIQUE(device_id, id)
    )
"""

CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bucketrow INTEGER NOT NULL,
        starttime INTEGER NOT NULL,
        endtime INTEGER NOT NULL,
        datastr TEXT NOT NULL,
        FOREIGN KEY (bucketrow) REFERENCES buckets(rowid)
    )
"""

INDEX_BUCKETS_TABLE_ID = """
    CREATE INDEX IF NOT EXISTS event_index_id ON events(id);
"""

INDEX_EVENTS_TABLE_STARTTIME = """
    CREATE INDEX IF NOT EXISTS event_index_starttime ON events(bucketrow, starttime);
"""
INDEX_EVENTS_TABLE_ENDTIME = """
    CREATE INDEX IF NOT EXISTS event_index_endtime ON events(bucketrow, endtime);
"""


def _rows_to_events(rows: Iterable) -> List[Event]:
    events = []
    for row in rows:
        eid = row[0]
        starttime = datetime.fromtimestamp(row[1] / 1000000, timezone.utc)
        endtime = datetime.fromtimestamp(row[2] / 1000000, timezone.utc)
        duration = endtime - starttime
        data = json.loads(row[3])
        events.append(Event(id=eid, timestamp=starttime, duration=duration, data=data))
    return events


class SqliteStorage(AbstractStorage):
    sid = "sqlite"

    def __init__(
        self, testing, filepath: Optional[str] = None, enable_lazy_commit=True
    ) -> None:
        self.testing = testing
        self.enable_lazy_commit = enable_lazy_commit
        self.device_scope: Optional[str] = None
        self._owner: Optional[SqliteStorage] = None

        # Ignore the migration check if custom filepath is set
        ignore_migration_check = filepath is not None

        ds_name = self.sid + legacy_testing_suffix(testing)
        if not filepath:
            data_dir = get_data_dir("aw-server")
            filename = ds_name + f".v{LATEST_VERSION}" + ".db"
            filepath = os.path.join(data_dir, filename)

        new_db_file = not os.path.exists(filepath)
        self.conn = sqlite3.connect(filepath)
        logger.info(f"Using database file: {filepath}")

        # Create tables
        self.conn.execute(CREATE_BUCKETS_TABLE)
        self.conn.execute(CREATE_EVENTS_TABLE)
        self.conn.execute(INDEX_BUCKETS_TABLE_ID)
        self.conn.execute(INDEX_EVENTS_TABLE_STARTTIME)
        self.conn.execute(INDEX_EVENTS_TABLE_ENDTIME)
        self.conn.execute("PRAGMA journal_mode=WAL;")

        # Upgrade legacy schema if needed.  CREATE TABLE IF NOT EXISTS is a no-op
        # on existing tables, and ALTER TABLE can add a column but cannot replace a
        # constraint, so a full rebuild is required to change UNIQUE(id) →
        # UNIQUE(device_id, id).  Rowids are preserved so events.bucketrow FKs stay valid.
        existing_columns = {
            row[1] for row in self.conn.execute("PRAGMA table_info(buckets)")
        }
        has_device_id = "device_id" in existing_columns
        has_composite_unique = False
        for idx in self.conn.execute("PRAGMA index_list(buckets)").fetchall():
            if idx[2]:  # unique flag
                idx_cols = {
                    r[2] for r in self.conn.execute(f"PRAGMA index_info({idx[1]})")
                }
                if "device_id" in idx_cols and "id" in idx_cols:
                    has_composite_unique = True
                    break
        if not has_composite_unique:
            device_id_src = "device_id" if has_device_id else "'local'"
            self.conn.execute("PRAGMA foreign_keys = OFF;")
            try:
                self.conn.executescript(f"""
                BEGIN;
                CREATE TABLE buckets_v2 (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL,
                    device_id TEXT NOT NULL DEFAULT 'local',
                    name TEXT,
                    type TEXT NOT NULL,
                    client TEXT NOT NULL,
                    hostname TEXT NOT NULL,
                    created TEXT NOT NULL,
                    datastr TEXT NOT NULL,
                    UNIQUE(device_id, id)
                );
                INSERT INTO buckets_v2 (rowid, id, device_id, name, type, client, hostname, created, datastr)
                SELECT rowid, id, {device_id_src}, name, type, client, hostname, created, datastr FROM buckets;
                DROP TABLE buckets;
                ALTER TABLE buckets_v2 RENAME TO buckets;
                COMMIT;
                """)
            except sqlite3.Error:
                self.conn.rollback()
                raise
            finally:
                # PRAGMA foreign_keys cannot change inside a transaction, so
                # roll back a failed rebuild before restoring enforcement.
                self.conn.execute("PRAGMA foreign_keys = ON;")
            logger.info(
                "Schema upgraded: rebuilt buckets table with UNIQUE(device_id, id)"
            )

        self.commit()

        if not ignore_migration_check:
            if new_db_file:
                logger.info("Created new SQlite db file")

            from aw_datastore import check_for_migration  # fmt: skip

            check_for_migration(self, migrate_peewee=new_db_file)

        self.last_commit = datetime.now()
        self.num_uncommitted_statements = 0

    def commit(self):
        """
        Useful for debugging and trying to lower the amount of
        unnecessary commits
        """
        if self._owner is not None:
            return self._owner.commit()
        self.conn.commit()
        self.last_commit = datetime.now()
        self.num_uncommitted_statements = 0

    def conditional_commit(self, num_statements):
        """
        Only commit transactions if:
         - We have a lot of statements in our transaction
         - Was a while ago since last commit
        This is because sqlite is very slow with small inserts, this
        is a way to batch them together and lower CPU+disk usage
        """
        if self._owner is not None:
            return self._owner.conditional_commit(num_statements)
        if self.enable_lazy_commit:
            self.num_uncommitted_statements += num_statements
            if self.num_uncommitted_statements > 50:
                self.commit()
            if (self.last_commit - datetime.now()) > timedelta(seconds=10):
                self.commit()
        else:
            self.commit()

    def for_device(self, device_id: str) -> "SqliteStorage":
        """Return a handle with immutable routing scope and the same connection.

        Like the underlying sqlite connection, handles are single-thread-owned.
        Commit bookkeeping stays on the owning storage, not its scoped views.
        """
        if not device_id:
            raise ValueError("Device scope must not be empty")
        scoped = copy.copy(self)
        scoped.device_scope = device_id
        scoped._owner = self._owner or self
        return scoped

    def _bucket_row(self, bucket_id: str, required: bool = True) -> Optional[int]:
        sql = "SELECT rowid FROM buckets WHERE id = ?"
        args = [bucket_id]
        if self.device_scope is not None:
            sql += " AND device_id = ?"
            args.append(self.device_scope)
        rows = self.conn.execute(sql, args).fetchall()
        if len(rows) > 1:
            raise ValueError(f"Ambiguous bucket '{bucket_id}'; select a device")
        if not rows:
            if not required:
                return None
            raise ValueError(f"Bucket '{bucket_id}' did not exist")
        return rows[0][0]

    def device_buckets(self):
        """Lossless metadata view, without constructing qualified string IDs."""
        devices = {}
        for (device,) in self.conn.execute("SELECT DISTINCT device_id FROM buckets"):
            if self.device_scope is None or device == self.device_scope:
                devices[device] = self.for_device(device).buckets()
        return devices

    def buckets(self):
        buckets = {}
        c = self.conn.cursor()
        for row in c.execute(
            "SELECT id, device_id, name, type, client, hostname, created, datastr FROM buckets"
        ):
            if self.device_scope is not None and row[1] != self.device_scope:
                continue
            if row[0] in buckets:
                raise ValueError(f"Ambiguous bucket '{row[0]}'; select a device")
            buckets[row[0]] = {
                "id": row[0],
                "device_id": row[1],
                "name": row[2],
                "type": row[3],
                "client": row[4],
                "hostname": row[5],
                "created": row[6],
                "data": json.loads(row[7] or "{}"),
            }
        return buckets

    def has_bucket(self, bucket_id: str) -> bool:
        sql = "SELECT count(*) FROM buckets WHERE id = ?"
        args = [bucket_id]
        if self.device_scope is not None:
            sql += " AND device_id = ?"
            args.append(self.device_scope)
        count = self.conn.execute(sql, args).fetchone()[0]
        if count > 1:
            raise ValueError(f"Ambiguous bucket '{bucket_id}'; select a device")
        return count == 1

    def buckets_with_last_updated(self):
        # Match get_events' ordering by endtime for this backend.
        rows = self.conn.execute(
            "SELECT b.id, b.device_id, b.name, b.type, b.client, b.hostname, b.created, "
            "b.datastr, e.endtime FROM buckets b LEFT JOIN events e ON e.id = "
            "(SELECT id FROM events WHERE bucketrow = b.rowid "
            "AND endtime >= 0 AND starttime <= ? ORDER BY endtime DESC LIMIT 1)",
            (MAX_TIMESTAMP,),
        )
        try:
            buckets = {}
            for row in rows:
                if self.device_scope is not None and row[1] != self.device_scope:
                    continue
                if row[0] in buckets:
                    raise ValueError(f"Ambiguous bucket '{row[0]}'; select a device")
                metadata = dict(
                    zip(
                        (
                            "id",
                            "device_id",
                            "name",
                            "type",
                            "client",
                            "hostname",
                            "created",
                        ),
                        row[:7],
                    )
                )
                metadata["data"] = json.loads(row[7] or "{}")
                if row[8] is not None:
                    metadata["last_updated"] = datetime.fromtimestamp(
                        row[8] / 1000000, timezone.utc
                    ).isoformat()
                buckets[row[0]] = metadata
            return buckets
        finally:
            rows.close()

    def iter_events(self, bucket_id):
        self.commit()
        cursor = self.conn.execute(
            "SELECT id, starttime, endtime, datastr FROM events "
            "WHERE bucketrow = ? "
            "AND endtime >= 0 AND starttime <= ? ORDER BY endtime DESC",
            (self._bucket_row(bucket_id, required=False), MAX_TIMESTAMP),
        )
        try:
            for row in cursor:
                yield _rows_to_events([row])[0]
        finally:
            cursor.close()

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
        device_id: str = "local",
    ):
        if self.device_scope is not None:
            device_id = self.device_scope
        if not device_id:
            raise ValueError("Device scope must not be empty")
        self.conn.execute(
            "INSERT INTO buckets(id, device_id, name, type, client, hostname, created, datastr) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                bucket_id,
                device_id,
                name,
                type_id,
                client,
                hostname,
                created,
                json.dumps(data or {}),
            ],
        )
        self.commit()
        return self.for_device(device_id).get_metadata(bucket_id)

    def update_bucket(
        self,
        bucket_id: str,
        type_id: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        update_values = [
            ("type", type_id),
            ("client", client),
            ("hostname", hostname),
            ("name", name),
            ("datastr", json.dumps(data) if data is not None else None),
        ]
        updates, values = zip(*[(k, v) for k, v in update_values if v is not None])
        if not updates:
            raise ValueError("At least one field must be updated.")

        sql = (
            "UPDATE buckets SET "
            + ", ".join(f"{u} = ?" for u in updates)
            + " WHERE rowid = ?"
        )
        self.conn.execute(sql, (*values, self._bucket_row(bucket_id)))
        self.commit()
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        rowid = self._bucket_row(bucket_id)
        self.conn.execute("DELETE FROM events WHERE bucketrow = ?", [rowid])
        cursor = self.conn.execute("DELETE FROM buckets WHERE rowid = ?", [rowid])
        self.commit()
        if cursor.rowcount != 1:
            raise ValueError("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        c = self.conn.cursor()
        res = c.execute(
            "SELECT id, device_id, name, type, client, hostname, created, datastr FROM buckets WHERE rowid = ?",
            [self._bucket_row(bucket_id)],
        )
        row = res.fetchone()
        if row is not None:
            return {
                "id": row[0],
                "device_id": row[1],
                "name": row[2],
                "type": row[3],
                "client": row[4],
                "hostname": row[5],
                "created": row[6],
                "data": json.loads(row[7] or "{}"),
            }
        else:
            raise ValueError("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        c = self.conn.cursor()
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        c.execute(
            "INSERT INTO events(bucketrow, starttime, endtime, datastr) "
            + "VALUES (?, ?, ?, ?)",
            [self._bucket_row(bucket_id), starttime, endtime, datastr],
        )
        event.id = c.lastrowid
        self.conditional_commit(1)
        return event

    def insert_many(self, bucket_id, events: List[Event]) -> None:
        # FIXME: Is this true not only for peewee but sqlite aswell?
        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948

        # First, upsert events with id's set
        events_upsert = [e for e in events if e.id is not None]
        for e in events_upsert:
            self.replace(bucket_id, e.id, e)

        # Then insert events without id's set
        events_insert = [e for e in events if e.id is None]
        event_rows = []
        rowid = self._bucket_row(bucket_id, required=False)
        for event in events_insert:
            starttime = event.timestamp.timestamp() * 1000000
            endtime = starttime + (event.duration.total_seconds() * 1000000)
            datastr = json.dumps(event.data)
            event_rows.append((rowid, starttime, endtime, datastr))
        query = (
            "INSERT INTO events(bucketrow, starttime, endtime, datastr) "
            + "VALUES (?, ?, ?, ?)"
        )
        self.conn.executemany(query, event_rows)
        self.conditional_commit(len(event_rows))

    def replace_last(self, bucket_id, event):
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        query = """UPDATE events
                   SET starttime = ?, endtime = ?, datastr = ?
                   WHERE id = (
                        SELECT id FROM events WHERE bucketrow = ?
                        ORDER BY endtime DESC, id DESC LIMIT 1)"""
        self.conn.execute(
            query, [starttime, endtime, datastr, self._bucket_row(bucket_id)]
        )
        self.conditional_commit(1)
        return True

    def delete(self, bucket_id, event_id):
        query = "DELETE FROM events " + "WHERE id = ? AND bucketrow = ?"
        cursor = self.conn.execute(
            query, [event_id, self._bucket_row(bucket_id, required=False)]
        )
        return cursor.rowcount == 1

    def replace(self, bucket_id, event_id, event) -> bool:
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        query = """UPDATE events
                     SET starttime = ?,
                         endtime = ?,
                         datastr = ?
                     WHERE id = ? AND bucketrow = ?"""
        cursor = self.conn.execute(
            query,
            [
                starttime,
                endtime,
                datastr,
                event_id,
                self._bucket_row(bucket_id, required=False),
            ],
        )
        self.conditional_commit(1)
        if cursor.rowcount:
            event.id = event_id
        return bool(cursor.rowcount)

    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        self.commit()
        c = self.conn.cursor()
        query = """
            SELECT id, starttime, endtime, datastr
            FROM events
            WHERE bucketrow = ? AND id = ?
            LIMIT 1
        """
        rows = c.execute(query, [self._bucket_row(bucket_id, required=False), event_id])
        events = _rows_to_events(rows)
        if events:
            return events[0]
        else:
            return None

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        if limit == 0:
            return []
        elif limit < 0:
            limit = -1
        self.commit()
        c = self.conn.cursor()
        starttime_i = starttime.timestamp() * 1000000 if starttime else 0
        endtime_i = endtime.timestamp() * 1000000 if endtime else MAX_TIMESTAMP
        query = """
            SELECT id, starttime, endtime, datastr
            FROM events
            WHERE bucketrow = ?
            AND endtime >= ? AND starttime <= ?
            ORDER BY endtime DESC LIMIT ?
        """
        rows = c.execute(
            query,
            [
                self._bucket_row(bucket_id, required=False),
                starttime_i,
                endtime_i,
                limit,
            ],
        )
        events = _rows_to_events(rows)
        return events

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ):
        self.commit()
        c = self.conn.cursor()
        starttime_i = starttime.timestamp() * 1000000 if starttime else 0
        endtime_i = endtime.timestamp() * 1000000 if endtime else MAX_TIMESTAMP
        query = (
            "SELECT count(*) "
            + "FROM events "
            + "WHERE bucketrow = ? "
            + "AND endtime >= ? AND starttime <= ?"
        )
        rows = c.execute(
            query, [self._bucket_row(bucket_id, required=False), starttime_i, endtime_i]
        )
        row = rows.fetchone()
        eventcount = row[0]
        return eventcount
