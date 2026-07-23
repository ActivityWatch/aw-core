"""
Peewee-storage-specific tests, mainly for the schema/index migration in
auto_migrate() and for guarding against query-plan regressions.
"""

import logging

from aw_datastore.storages import PeeweeStorage
from aw_datastore.storages import peewee as peewee_storage

from . import context  # noqa: F401

logging.basicConfig(level=logging.DEBUG)


def _indexes(db) -> set:
    return {row[1] for row in db.execute_sql("PRAGMA index_list(eventmodel)")}


def test_index_migration():
    """auto_migrate() replaces the old single-column indexes with the composite one."""
    storage = PeeweeStorage(testing=True)
    db = storage.db
    path = db.database

    assert "eventmodel_bucket_id_timestamp" in _indexes(db)

    # Recreate the pre-migration state: single-column indexes, no composite
    db.execute_sql("DROP INDEX eventmodel_bucket_id_timestamp")
    db.execute_sql("CREATE INDEX eventmodel_bucket_id ON eventmodel (bucket_id)")
    db.execute_sql("CREATE INDEX eventmodel_timestamp ON eventmodel (timestamp)")

    peewee_storage.auto_migrate(path)

    indexes = _indexes(db)
    assert "eventmodel_bucket_id_timestamp" in indexes
    assert "eventmodel_bucket_id" not in indexes
    assert "eventmodel_timestamp" not in indexes


def test_query_plan_uses_composite_index():
    """
    The last-event query (issued by replace_last on every merged heartbeat) and
    range queries must be served by the composite index in a single pass.

    Without it, SQLite reads *all* of a bucket's rows and sorts them in a temp
    B-tree even for LIMIT 1, which on multi-million-event buckets meant seconds
    of CPU/IO per heartbeat (and terabytes read per day).
    """
    storage = PeeweeStorage(testing=True)
    db = storage.db

    queries = [
        # replace_last/_get_last
        "SELECT * FROM eventmodel WHERE bucket_id = 1"
        " ORDER BY timestamp DESC LIMIT 1",
        # get_events with a time range
        "SELECT * FROM eventmodel WHERE bucket_id = 1"
        " AND timestamp >= '2026-01-01' AND timestamp <= '2026-01-02'"
        " ORDER BY timestamp DESC LIMIT 100",
    ]
    for query in queries:
        rows = db.execute_sql(f"EXPLAIN QUERY PLAN {query}").fetchall()
        plan = "\n".join(str(row) for row in rows)
        assert "eventmodel_bucket_id_timestamp" in plan, plan
        assert "TEMP B-TREE" not in plan, plan
