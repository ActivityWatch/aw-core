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

def detect_db_files(data_dir: str) -> List[str]:
    return [filename for filename in os.listdir(data_dir) if "sqlite" in filename]


def detect_db_version(data_dir: str, max_version: Optional[int] = None) -> Optional[int]:
    """Returns the most recent version number of any database file found (up to max_version)"""
    import re
    files = detect_db_files(data_dir)
    r = re.compile("v[0-9]+")
    re_matches = [r.search(filename) for filename in files]
    versions = [int(match.group(0)[1:]) for match in re_matches if match]
    if max_version:
        versions = list(filter(lambda v: v <= max_version, versions))
    return max(versions) if versions else None


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
        timestamp INTEGER NOT NULL,
        duration REAL NOT NULL,
        datastr TEXT NOT NULL,
        FOREIGN KEY (bucket) REFERENCES buckets(id)
    )
"""

INDEX_EVENTS_TABLE = """
    CREATE INDEX IF NOT EXISTS event_index ON events(bucket, timestamp);
"""


class SqliteStorage(AbstractStorage):
    sid = "sqlite"

    def __init__(self, testing):
        data_dir = get_data_dir("aw-server")
        current_db_version = detect_db_version(data_dir, max_version=LATEST_VERSION)

        if current_db_version is not None and current_db_version < LATEST_VERSION:
            # DB file found but was of an older version
            logger.info("Latest version database file found was of an older version")
            logger.info("Creating database file for new version {}".format(LATEST_VERSION))
            logger.warning("ActivityWatch does not currently support database migrations, new database file will be empty")

        filename = self.sid + ('-testing' if testing else '') + ".v{}".format(LATEST_VERSION) + '.db'
        filepath = os.path.join(data_dir, filename)
        self.conn = sqlite3.connect(filepath)
        logger.info("Using database file: {}".format(filepath))

        # Create tables
        c = self.conn.cursor()
        c.execute(CREATE_BUCKETS_TABLE)
        c.execute(CREATE_EVENTS_TABLE)
        c.execute(INDEX_EVENTS_TABLE)

        c.execute("PRAGMA syncronous = NORMAL;");
        c.execute("PRAGMA journal_mode = WAL;");

        self.conn.commit()

    def buckets(self):
        buckets = {}
        c = self.conn.cursor()
        for row in c.execute("SELECT id FROM buckets"):
            buckets[row[0]] = row[0]
        return buckets

    def create_bucket(self, bucket_id: str, type_id: str, client: str,
                      hostname: str, created: str, name: Optional[str] = None):
        c = self.conn.cursor()
        c.execute("INSERT INTO buckets VALUES (?, ?, ?, ?, ?, ?)",
            [bucket_id, name, type_id, client, hostname, created])
        self.conn.commit();
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        c = self.conn.cursor()
        c.execute("DELETE FROM events WHERE bucket = ?", [bucket_id])
        c.execute("DELETE FROM buckets WHERE id = ?", [bucket_id])
        self.conn.commit()

    def get_metadata(self, bucket_id: str):
        c = self.conn.cursor()
        res = c.execute("SELECT * FROM buckets")
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
        timestamp = event.timestamp.timestamp()*1000000
        duration = event.duration.total_seconds()
        datastr = json.dumps(event.data)
        c.execute("INSERT INTO events(bucket, timestamp, duration, datastr) VALUES (?, ?, ?, ?)",
            [bucket_id, timestamp, event.duration.total_seconds(), datastr])
        event.id = c.lastrowid
        return event

    def insert_many(self, bucket_id, events: List[Event], fast=False) -> None:
        # FIXME: Is this true not only for peewee but sqlite aswell?
        # Chunking into lists of length 100 is needed here due to SQLITE_MAX_COMPOUND_SELECT
        # and SQLITE_LIMIT_VARIABLE_NUMBER under Windows.
        # See: https://github.com/coleifer/peewee/issues/948
        event_rows = []
        for event in events:
            timestamp = event.timestamp.timestamp()*1000000
            duration = event.duration.total_seconds()
            datastr = json.dumps(event.data)
            event_rows.append((bucket_id, timestamp, duration, datastr))
        query = "INSERT INTO EVENTS(bucket, timestamp, duration, datastr) " + \
                "VALUES (?, ?, ?, ?)"
        self.conn.executemany(query, event_rows)
        if len(event_rows) > 50:
            self.conn.commit();

    def replace_last(self, bucket_id, event):
        c = self.conn.cursor()
        timestamp = event.timestamp.timestamp()*1000000
        duration = event.duration.total_seconds()
        datastr = json.dumps(event.data)
        query = "UPDATE events " + \
                "SET bucket = ?, timestamp = ?, duration = ?, datastr = ? " + \
                "WHERE timestamp = (SELECT max(timestamp) FROM events LIMIT 1)"
        c.execute(query, [bucket_id, timestamp, duration, datastr])
        return True

    def delete(self, bucket_id, event_id):
        c = self.conn.cursor()
        query = "DELETE FROM events WHERE bucket = ? AND id = ?"
        c.execute(query, [bucket_id, event_id])
        # TODO: Handle if event doesn't exist
        return True

    def replace(self, bucket_id, event_id, event):
        c = self.conn.cursor()
        timestamp = event.timestamp.timestamp()*1000000
        duration = event.duration.total_seconds()
        datastr = json.dumps(event.data)
        query = "UPDATE events " + \
                "SET bucket = ?, timestamp = ?, duration = ?, datastr = ? " + \
                "WHERE id = ?"
        c.execute(query, [bucket_id, timestamp, duration, datastr, event_id])
        return True

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        self.conn.commit()
        c = self.conn.cursor()
        if limit <= 0:
            limit = -1
        if not starttime:
            starttime = 0
        else:
            starttime = starttime.timestamp()*1000000
        if not endtime:
            endtime = sys.maxsize
        else:
            endtime = endtime.timestamp()*1000000
        query = "SELECT id, timestamp, duration, datastr " + \
                "FROM events " + \
                "WHERE bucket = ? AND timestamp >= ? AND timestamp <= ? " + \
                "ORDER BY timestamp DESC LIMIT ?"
        rows = c.execute(query, [bucket_id, starttime, endtime, limit])
        events = []
        for row in rows:
            eid = row[0]
            timestamp = datetime.fromtimestamp(row[1]/1000000, timezone.utc)
            duration = row[2]
            data = json.loads(row[3])
            events.append(Event(id=eid, timestamp=timestamp, duration=duration, data=data))
        return events

    def get_eventcount(self, bucket_id: str,
                   starttime: Optional[datetime] = None, endtime: Optional[datetime] = None):
        self.conn.commit()
        c = self.conn.cursor()
        if not starttime:
            starttime = 0
        else:
            starttime = starttime.timestamp()*1000000
        if not endtime:
            import sys
            endtime = sys.maxsize
        else:
            endtime = endtime.timestamp()*1000000
        query = "SELECT count(*) " + \
                "FROM events " + \
                "WHERE bucket = ? AND timestamp >= ? AND timestamp <= ?"
        rows = c.execute(query, [bucket_id, starttime, endtime])
        row = rows.fetchone()
        eventcount = row[0]
        return eventcount
