from datetime import datetime, timedelta, timezone

import pytest

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_transform.query2 import QueryException, query, _parse_token
from aw_transform.query2 import Integer, Variable, String, Function


# TODO: space checking

def test_query2_test_token_parsing():
    ns = {}
    assert(type(_parse_token("123", ns)) is Integer)
    assert(type(_parse_token('"test"', ns)) is String)
    assert(type(_parse_token("test", ns)) is Variable)
    assert(type(_parse_token("test()", ns)) is Function)
    try:
        result = _parse_token(None, ns)
        assert(False)
    except QueryException:
        pass
    try:
        result = _parse_token("a123", ns)
        assert(False)
    except QueryException:
        pass
    try:
        result = _parse_token("#", ns)
        assert(False)
    except QueryException:
        pass

def test_query2_test_bogus_query():
    try: # No assignment
        example_query = "asd"
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass
    try: # Assign to non-variable
        example_query = "1=2"
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass
    try: # Function within a function
        example_query = "asd=asd(asd())"
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass

def test_query2_test_function_query():
    try: # Function with no args
        example_query = "asd=asd()"
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass

def test_query2_test_return_value():
    example_query = "RETURN=1"
    assert(query(example_query, None) == 1)
    example_query = 'RETURN="testing 123"'
    assert(query(example_query, None) == "testing 123")
    # TODO: test dict/array


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_test_basic_query(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    example_query = \
    """
    NAME="test_query"
    CACHE=False
    events=query_bucket("{bid1}")
    intersect_events=query_bucket("{bid2}")
    RETURN=filter_period_intersect(events, intersect_events)
    """.format(bid1=bid1, bid2=bid2)
    try:
        # Setup buckets
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        # Prepare buckets
        currtime = datetime.now(timezone.utc)
        e1 = Event(data={"label": "test1"},
                   timestamp=currtime,
                   duration=timedelta(seconds=1))
        e2 = Event(data={"label": "test2"},
                   timestamp=currtime + timedelta(seconds=2),
                   duration=timedelta(seconds=1))
        et = Event(data={"label": "intersect-label"},
                   timestamp=currtime,
                   duration=timedelta(seconds=1))
        bucket1.insert(e1)
        bucket1.insert(e2)
        bucket2.insert(et)
        # Query
        result = query(example_query, datastore)
        # Assert
        assert(len(result) == 1)
        assert(result[0]["data"]["label"] == "test1")
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)
