from typing import Optional, List, Tuple
from datetime import datetime, timedelta, timezone
import sqlite3

from aw_core.models import Event

from takethetime import ttt

from . import logger, AbstractStorage


def event_to_row(e: Event):
    return (e.timestamp.isoformat(), e.to_json_str())


def table_format_to_str(table_format: Tuple[Tuple[str]]):
    return "(" + ", ".join(tuple(" ".join(table_format[key]) for key in table_format)) + ")"


class SQLiteStorage(AbstractStorage):

    def __init__(self, testing):
        self.logger = logger.getChild("sqlite")
        self.conn = sqlite3.connect('example.db')
        self.c = self.conn.cursor()

        # TODO: Fix
        try:
            self._create_tables()
        except:
            print("Tables already created")

    def _create_table(self, table_format):
        table_str = table_format_to_str(table_format)
        print(table_str)
        self.c.execute('CREATE TABLE events ' + table_str)

    def _create_tables(self):
        # Create event table
        self._create_table((
            ("timestamp", "text"),
            ("bucket_id", "text"),
            ("jsonstr", "text"),
        ))

        # Create bucket table
        self._create_table((
            ("bucket_id", "text"),
            ("name", "text"),
            ("hostname", "text"),
            ("client", "text"),
            ("created", "text"),
        ))

    def buckets(self):

        raise NotImplementedError

    def create_bucket(self, bucket_id: str, type: str, client: str, hostname: str,
                      created: datetime, name: str):
        raise NotImplementedError

    def delete_bucket(self, bucket_id: str):
        raise NotImplementedError

    def get_metadata(self, bucket_id: str):
        raise NotImplementedError

    def insert_one(self, bucket_id: str, event: Event):
        row = event_to_row(event)
        self.c.execute("INSERT INTO events VALUES (?, '{}', ?)".format(bucket_id), row)

    def insert_many(self, bucket_id, events: List[Event]):
        rows = list(map(event_to_row, events))
        self.c.executemany("INSERT INTO events VALUES (?, '{}', ?)".format(bucket_id), rows)

    def replace_last(self, bucket_id, event):
        raise NotImplemented

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime]=None, endtime: Optional[datetime]=None):
        """Returns events in sorted order (latest first)"""
        q = "SELECT * FROM events WHERE bucket_id = '{}'".format(bucket_id)
        if starttime or endtime:
            q += " AND"
            if starttime:
                q += " timestamp > '" + starttime.isoformat() + "'"
                if endtime:
                    q += " AND"
            if endtime:
                q += " timestamp < '" + endtime.isoformat() + "'"
        q += " ORDER BY timestamp DESC"
        self.c.execute(q)

        rows = self.c.fetchall()
        print(rows[0])

        return events

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # Save (commit) the changes
        self.conn.commit()

        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        self.conn.close()


if __name__ == "__main__":
    now = datetime.now(timezone.utc)

    num_events = 100000
    events = [None] * num_events
    for i in range(num_events):
        events[i] = Event(label="asd", timestamp=now - i * timedelta(hours=1))

    bucket_id = "test_bucket"

    with ttt():
        with SQLiteStorage() as sql:

            with ttt("insert"):
                sql.insert_many(bucket_id, events)

            with ttt("get all"):
                events = sql.get_events(bucket_id)
                print("Total number of events: {}".format(len(events)))

            with ttt("get within time interval"):
                events = sql.get_events(bucket_id, begin=now - 100 * timedelta(hours=1), end=now)
                print("Events within time interval: {}".format(len(events)))

            assert events == sorted(events, reverse=True, key=lambda t: t[0])
            print(len(events))
