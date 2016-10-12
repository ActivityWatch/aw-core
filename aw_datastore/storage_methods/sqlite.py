from typing import Optional
from datetime import datetime, timedelta, timezone
import sqlite3

from aw_core.models import Event

from TTT import TTT


def event_to_row(e: Event):
    return (e.timestamp.isoformat(), e.to_json_str())


class SQLiteStorageMethod:

    def __init__(self):
        self.conn = sqlite3.connect('example.db')
        self.c = self.conn.cursor()

    def create_table(self):
        # Create table
        self.c.execute('''CREATE TABLE events
                     (timestamp text, bucket_id text, jsonstr text)''')

    def insert(self, bucket_id, event):
        rows = list(map(event_to_row, events))
        for row in rows:
            self.c.execute("INSERT INTO events VALUES (?, '{}', ?)".format(bucket_id), row)

    def insert_many(self, bucket_id, event):
        rows = list(map(event_to_row, events))
        self.c.executemany("INSERT INTO events VALUES (?, '{}', ?)".format(bucket_id), rows)

    def get_events(self, bucket_id: str, begin: Optional[datetime]=None, end: Optional[datetime]=None):
        """Returns events in sorted order (latest first)"""
        q = "SELECT * FROM events WHERE bucket_id = '{}'".format(bucket_id)
        if begin or end:
            q += " AND"
            if begin:
                q += " timestamp > '" + begin.isoformat() + "'"
                if end:
                    q += " AND"
            if end:
                q += " timestamp < '" + end.isoformat() + "'"
        q += " ORDER BY timestamp DESC"
        self.c.execute(q)
        return self.c.fetchall()

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
        with SQLiteStorageMethod() as sql:
            try:
                sql.create_table()
            except:
                pass

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
