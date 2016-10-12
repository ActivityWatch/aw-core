from typing import Optional, List
from datetime import datetime, timedelta, timezone
import sqlite3

from aw_core.models import Event

from TTT import TTT

from . import logger, AbstractStorage


def event_to_row(e: Event):
    return (e.timestamp.isoformat(), e.to_json_str())


class SQLiteStorage(AbstractStorage):

    def __init__(self, testing):
        self.logger = logger.getChild("sqlite")
        self.conn = sqlite3.connect('example.db')
        self.c = self.conn.cursor()

        # TODO: Fix
        try:
            self._create_table()
        except:
            print("Table already created")

    def _create_table(self):
        # Create table
        table_format = (
            ("timestamp", "text"),
            ("bucket_id", "text"),
            ("jsonstr", "text"),
        )
        table_format_str = "(" + ", ".join(tuple(" ".join(table_format[key]) for key in table_format)) + ")"
        print(table_format_str)
        self.c.execute('CREATE TABLE events ' + table_format_str)

    def buckets(self, bucket_id: str):
        raise NotImplemented

    def create_bucket(self, bucket_id: str, name: str):
        raise NotImplemented

    def delete_bucket(self, bucket_id: str):
        raise NotImplemented

    def get_metadata(self, bucket_id: str):
        raise NotImplemented

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

    with TTT():
        with SQLiteStorage() as sql:

            with TTT("insert"):
                sql.insert_many(bucket_id, events)

            with TTT("get all"):
                events = sql.get_events(bucket_id)
                print("Total number of events: {}".format(len(events)))

            with TTT("get within time interval"):
                events = sql.get_events(bucket_id, begin=now - 100 * timedelta(hours=1), end=now)
                print("Events within time interval: {}".format(len(events)))

            assert events == sorted(events, reverse=True, key=lambda t: t[0])
            print(len(events))
