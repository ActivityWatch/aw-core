from typing import Optional, List
from datetime import datetime, timezone
import json
import os
import sys
import logging

import sqlite3

from aw_core.models import Event
from aw_core.dirs import get_data_dir

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

LATEST_VERSION=1

CREATE_BUCKETS_TABLE = """
    CREATE TABLE IF NOT EXISTS buckets (
        id TEXT PRIMARY KEY,
        name TEXT,
        type TEXT NOT NULL,
        client TEXT NOT NULL,
        hostname TEXT NOT NULL,
        created TEXT NOT NULL
    )
"""

CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bucket TEXT NOT NULL,
        starttime INTEGER NOT NULL,
        endtime REAL NOT NULL,
        datastr TEXT NOT NULL,
        FOREIGN KEY (bucket) REFERENCES buckets(id)
    )
"""

INDEX_EVENTS_TABLE = """
    CREATE INDEX IF NOT EXISTS event_index ON events(bucket, starttime, endtime);
"""


class SqliteStorage(AbstractStorage):
    sid = "sqlite"

    def __init__(self, testing):
        self.testing = testing
        data_dir = get_data_dir("aw-server")

        ds_name = self.sid + ('-testing' if testing else '')
        filename = ds_name + ".v{}".format(LATEST_VERSION) + '.db'
        filepath = os.path.join(data_dir, filename)
        new_db_file = not os.path.exists(filepath)
        self.conn = sqlite3.connect(filepath)
        logger.info("Using database file: {}".format(filepath))

        # Create tables
        self.conn.execute(CREATE_BUCKETS_TABLE)
        self.conn.execute(CREATE_EVENTS_TABLE)
        self.conn.execute(INDEX_EVENTS_TABLE)
        self.conn.execute("PRAGMA journal_mode = WAL;");
        self.commit()

        if new_db_file:
            logger.info("Created new SQlite db file")
            from aw_datastore import check_for_migration
            check_for_migration(self, ds_name, LATEST_VERSION)

    def commit(self):
        # Useful for debugging and trying to lower the amount of
        # unnecessary commits
        self.conn.commit()

    def buckets(self):
        buckets = {}
        c = self.conn.cursor()
        for row in c.execute("SELECT * FROM buckets"):
            buckets[row[0]] = {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client": row[3],
                "hostname": row[4],
                "created": row[5],
            }
        return buckets

    def create_bucket(self, bucket_id: str, type_id: str, client: str,
                      hostname: str, created: str, name: Optional[str] = None):
        self.conn.execute("INSERT INTO buckets VALUES (?, ?, ?, ?, ?, ?)",
            [bucket_id, name, type_id, client, hostname, created])
        self.commit();
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        self.conn.execute("DELETE FROM events WHERE bucket = ?", [bucket_id])
        self.conn.execute("DELETE FROM buckets WHERE id = ?", [bucket_id])
        self.commit()

    def get_metadata(self, bucket_id: str):
        c = self.conn.cursor()
        res = c.execute("SELECT * FROM buckets WHERE id = ?", [bucket_id])
        row = res.fetchone()
        bucket = {
            "id": row[0],
            "name": row[1],
            "type": row[2],
            "client": row[3],
            "hostname": row[4],
            "created": row[5],
        }
        return bucket

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        c = self.conn.cursor()
        starttime = event.timestamp.timestamp() * 1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        c.execute("INSERT INTO events(bucket, starttime, endtime, datastr) VALUES (?, ?, ?, ?)",
            [bucket_id, starttime, endtime, datastr])
        event.id = c.lastrowid
        return event

    def insert_many(self, bucket_id, events: List[Event], fast=False) -> None:
        # FIXME: Is this true not only for peewee but sqlite aswell?
        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948
        event_rows = []
        for event in events:
            starttime = event.timestamp.timestamp()*1000000
            endtime = starttime + (event.duration.total_seconds() * 1000000)
            datastr = json.dumps(event.data)
            event_rows.append((bucket_id, starttime, endtime, datastr))
        query = "INSERT INTO EVENTS(bucket, starttime, endtime, datastr) " + \
                "VALUES (?, ?, ?, ?)"
        self.conn.executemany(query, event_rows)
        if len(event_rows) > 50:
            self.commit();

    def replace_last(self, bucket_id, event):
        starttime = event.timestamp.timestamp()*1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        query = "UPDATE events " + \
                "SET starttime = ?, endtime = ?, datastr = ? " + \
                "WHERE endtime = (SELECT max(endtime) FROM events WHERE bucket = ?) AND bucket = ?"
        self.conn.execute(query, [starttime, endtime, datastr, bucket_id, bucket_id])
        return True

    def delete(self, bucket_id, event_id):
        query = "DELETE FROM events WHERE bucket = ? AND id = ?"
        self.conn.execute(query, [bucket_id, event_id])
        # TODO: Handle if event doesn't exist
        return True

    def replace(self, bucket_id, event_id, event) -> bool:
        starttime = event.timestamp.timestamp()*1000000
        endtime = starttime + (event.duration.total_seconds() * 1000000)
        datastr = json.dumps(event.data)
        query = "UPDATE events " + \
                "SET bucket = ?, starttime = ?, endtime = ?, datastr = ? " + \
                "WHERE id = ?"
        self.conn.execute(query, [bucket_id, starttime, endtime, datastr, event_id])
        return True

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        self.commit()
        c = self.conn.cursor()
        if limit <= 0:
            limit = -1
        starttime_i = starttime.timestamp()*1000000 if starttime else 0
        endtime_i = endtime.timestamp()*1000000 if endtime else sys.maxsize
        query = "SELECT id, starttime, endtime, datastr " + \
                "FROM events " + \
                "WHERE bucket = ? AND starttime >= ? AND endtime <= ? " + \
                "ORDER BY endtime DESC LIMIT ?"
        rows = c.execute(query, [bucket_id, starttime_i, endtime_i, limit])
        events = []
        for row in rows:
            eid = row[0]
            starttime = datetime.fromtimestamp(row[1]/1000000, timezone.utc)
            endtime = datetime.fromtimestamp(row[2]/1000000, timezone.utc)
            duration = endtime - starttime
            data = json.loads(row[3])
            events.append(Event(id=eid, timestamp=starttime, duration=duration, data=data))
        return events

    def get_eventcount(self, bucket_id: str,
                   starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        self.commit()
        c = self.conn.cursor()
        starttime_i = starttime.timestamp()*1000000 if starttime else 0
        endtime_i = endtime.timestamp()*1000000 if endtime else sys.maxsize
        query = "SELECT count(*) " + \
                "FROM events " + \
                "WHERE bucket = ? AND endtime >= ? AND starttime <= ?"
        rows = c.execute(query, [bucket_id, starttime_i, endtime_i])
        row = rows.fetchone()
        eventcount = row[0]
        return eventcount
