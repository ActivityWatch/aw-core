from datetime import datetime, timedelta, timezone
import iso8601

import pytest

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_transform.query2 import QueryException, query, _parse_token
from aw_transform.query2 import Integer, Variable, String, Function


# TODO: space checking

def test_query2_test_token_parsing():
    ns = {}
    (t, token), trash = _parse_token("123", ns)
    assert token == "123"
    assert t == Integer
    (t, token), trash = _parse_token('"test"', ns)
    assert token == '"test"'
    assert t == String
    (t, token), trash = _parse_token("test0xDEADBEEF", ns)
    assert token == "test0xDEADBEEF"
    assert t == Variable
    (t, token), trash = _parse_token("test1337()", ns)
    assert token == "test1337()"
    assert t == Function

    try:
        _parse_token(None, ns)
        assert(False)
    except QueryException:
        pass
    try:
        _parse_token('"', ns)
        assert(False)
    except QueryException:
        pass
    try:
        _parse_token("#", ns)
        assert(False)
    except QueryException:
        pass

def test_query2_test_bogus_query():
    try: # Assign to non-variable
        example_query = "1=2"
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass
    try: # Unclosed function
        query("a=unclosed_function(var1", None)
        assert(False)
    except QueryException:
        pass
    try: # Two tokens in assignment
        example_query = "asd nop()=2"
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass
    try: # Unvlosed string
        example_query = 'asd="something is wrong with me'
        result = query(example_query, None)
        assert(False)
    except QueryException:
        pass
    try: # Two tokens in value
        example_query = "asd=asd1 asd2"
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
    example_query = """
    NAME="asd"
    STARTTIME="2017"
    ENDTIME="2017"
    RETURN=1
    """
    assert(query(example_query, None) == 1)

    example_query = """
    NAME="asd2"
    STARTTIME="2017"
    ENDTIME="2017"
    RETURN="testing 123"
    """
    assert(query(example_query, None) == "testing 123")
    # TODO: test dict/array


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_test_basic_query(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    starttime = iso8601.parse_date("1970")
    endtime = starttime + timedelta(hours=1)
    example_query = \
    """
    NAME="test_query"
    CACHE=TRUE
    STARTTIME="{}"
    ENDTIME="{}"
    bid1="{}"
    bid2="{}"
    events=query_bucket(bid1)
    intersect_events=query_bucket(bid2)
    RETURN=filter_period_intersect(events, intersect_events)
    """.format(starttime, endtime, bid1, bid2)
    try:
        # Setup buckets
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        # Prepare buckets
        e1 = Event(data={"label": "test1"},
                   timestamp=starttime,
                   duration=timedelta(seconds=1))
        e2 = Event(data={"label": "test2"},
                   timestamp=starttime + timedelta(seconds=2),
                   duration=timedelta(seconds=1))
        et = Event(data={"label": "intersect-label"},
                   timestamp=starttime,
                   duration=timedelta(seconds=1))
        bucket1.insert(e1)
        bucket1.insert(e2)
        bucket2.insert(et)
        # Query
        result = query(example_query, datastore)
        # Query again for cache hit
        result = query(example_query, datastore)
        # Assert
        assert(len(result) == 1)
        assert(result[0]["data"]["label"] == "test1")
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)

@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_test_merged_keys(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    starttime = iso8601.parse_date("2080")
    endtime = starttime + timedelta(hours=1)
    example_query = \
    """
    NAME="test_query"
    CACHE=TRUE
    STARTTIME="{}"
    ENDTIME="{}"
    bid1="{}"
    events=query_bucket(bid1)
    events=merge_events_by_keys2(events, "label1", "label2")
    RETURN=sort_by_duration(events)
    """.format(starttime, endtime, bid1)
    try:
        # Setup buckets
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        # Prepare buckets
        e1 = Event(data={"label1": "test1", "label2": "test1"},
                   timestamp=starttime,
                   duration=timedelta(seconds=1))
        e2 = Event(data={"label1": "test1", "label2": "test1"},
                   timestamp=starttime + timedelta(seconds=1),
                   duration=timedelta(seconds=1))
        e3 = Event(data={"label1": "test1", "label2": "test2"},
                   timestamp=starttime + timedelta(seconds=2),
                   duration=timedelta(seconds=1))
        bucket1.insert(e3)
        bucket1.insert(e1)
        bucket1.insert(e2)
        # Query
        result = query(example_query, datastore)
        # Query again for cache miss (it's not year 2080 yet, I hope?)
        result = query(example_query, datastore)
        # Assert
        assert(len(result) == 2)
        assert(result[0]["data"]["label1"] == "test1")
        assert(result[0]["data"]["label2"] == "test1")
        assert(result[0]["duration"] == timedelta(seconds=2))
        assert(result[1]["data"]["label1"] == "test1")
        assert(result[1]["data"]["label2"] == "test2")
        assert(result[1]["duration"] == timedelta(seconds=1))
    finally:
        datastore.delete_bucket(bid1)
