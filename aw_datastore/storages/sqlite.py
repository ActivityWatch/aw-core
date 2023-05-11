import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

from aw_core.dirs import get_data_dir
from aw_core.models import Event

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

LATEST_VERSION = 1

# The max integer value in SQLite is signed 8 Bytes / 64 bits
MAX_TIMESTAMP = 2**63 - 1

CREATE_BUCKETS_TABLE = """
    CREATE TABLE IF NOT EXISTS buckets (
        rowid INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT UNIQUE NOT NULL,
        name TEXT,
        type TEXT NOT NULL,
        client TEXT NOT NULL,
        hostname TEXT NOT NULL,
        created TEXT NOT NULL,
        datastr TEXT NOT NULL
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

        # Ignore the migration check if custom filepath is set
        ignore_migration_check = filepath is not None

        ds_name = self.sid + ("-testing" if testing else "")
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
        self.commit()

        if new_db_file and not ignore_migration_check:
            logger.info("Created new SQlite db file")
            from aw_datastore import check_for_migration

            check_for_migration(self)

        self.last_commit = datetime.now()
        self.num_uncommitted_statements = 0

    def commit(self):
        """
        Useful for debugging and trying to lower the amount of
        unnecessary commits
        """
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
        if self.enable_lazy_commit:
            self.num_uncommitted_statements += num_statements
            if self.num_uncommitted_statements > 50:
                self.commit()
            if (self.last_commit - datetime.now()) > timedelta(seconds=10):
                self.commit()
        else:
            self.commit()

    def buckets(self):
        buckets = {}
        c = self.conn.cursor()
        for row in c.execute(
            "SELECT id, name, type, client, hostname, created, datastr FROM buckets"
        ):
            buckets[row[0]] = {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client": row[3],
                "hostname": row[4],
                "created": row[5],
                "data": json.loads(row[6] or "{}"),
            }
        return buckets

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        self.conn.execute(
            "INSERT INTO buckets(id, name, type, client, hostname, created, datastr) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                bucket_id,
                name,
                type_id,
                client,
                hostname,
                created,
                json.dumps(data or {}),
            ],
        )
        self.commit()
        return self.get_metadata(bucket_id)

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
            + " WHERE id = ?"
        )
        self.conn.execute(sql, (*values, bucket_id))
        self.commit()
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        self.conn.execute(
            "DELETE FROM events WHERE bucketrow IN (SELECT rowid FROM buckets WHERE id = ?)",
            [bucket_id],
        )
        cursor = self.conn.execute("DELETE FROM buckets WHERE id = ?", [bucket_id])
        self.commit()
        if cursor.rowcount != 1:
            raise Exception("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        c = self.conn.cursor()
        res = c.execute(
            "SELECT id, name, type, client, hostname, created, datastr FROM buckets WHERE id = ?",
            [bucket_id],
        )
        row = res.fetchone()
        if row is not None:
            return {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client": row[3],
                "hostname": row[4],
                "created": row[5],
                "data": json.loads(row[6] or "{}"),
            }
        else:
            raise Exception("Bucket did not exist, could not get metadata")

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        c = self.conn.cursor()
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        c.execute(
            "INSERT INTO events(bucketrow, starttime, endtime, datastr) "
            + "VALUES ((SELECT rowid FROM buckets WHERE id = ?), ?, ?, ?)",
            [bucket_id, starttime, endtime, datastr],
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
        for event in events_insert:
            starttime = event.timestamp.timestamp() * 1000000
            endtime = starttime + (event.duration.total_seconds() * 1000000)
            datastr = json.dumps(event.data)
            event_rows.append((bucket_id, starttime, endtime, datastr))
        query = (
            "INSERT INTO events(bucketrow, starttime, endtime, datastr) "
            + "VALUES ((SELECT rowid FROM buckets WHERE id = ?), ?, ?, ?)"
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
                        SELECT id FROM events WHERE endtime =
                            (SELECT max(endtime) FROM events WHERE bucketrow =
                                (SELECT rowid FROM buckets WHERE id = ?) LIMIT 1))"""
        self.conn.execute(query, [starttime, endtime, datastr, bucket_id])
        self.conditional_commit(1)
        return True

    def delete(self, bucket_id, event_id):
        query = (
            "DELETE FROM events "
            + "WHERE id = ? AND bucketrow = (SELECT b.rowid FROM buckets b WHERE b.id = ?)"
        )
        cursor = self.conn.execute(query, [event_id, bucket_id])
        return cursor.rowcount == 1

    def replace(self, bucket_id, event_id, event) -> bool:
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        query = """UPDATE events
                     SET bucketrow = (SELECT rowid FROM buckets WHERE id = ?),
                         starttime = ?,
                         endtime = ?,
                         datastr = ?
                     WHERE id = ?"""
        self.conn.execute(query, [bucket_id, starttime, endtime, datastr, event_id])
        self.conditional_commit(1)
        return True

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
            WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?) AND id = ?
            LIMIT 1
        """
        rows = c.execute(query, [bucket_id, event_id])
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
            WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?)
            AND endtime >= ? AND starttime <= ?
            ORDER BY endtime DESC LIMIT ?
        """
        rows = c.execute(query, [bucket_id, starttime_i, endtime_i, limit])
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
            + "WHERE bucketrow = (SELECT rowid FROM buckets WHERE id = ?) "
            + "AND endtime >= ? AND starttime <= ?"
        )
        rows = c.execute(query, [bucket_id, starttime_i, endtime_i])
        row = rows.fetchone()
        eventcount = row[0]
        return eventcount
