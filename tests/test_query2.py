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

def test_query2_test_query_function_calling():
    try: # Function which doesn't exist
        example_query = """NAME="asd";
        STARTTIME="1970-01-01";
        ENDTIME="1970-01-02";
        RETURN=asd();"""
        result = query(example_query, None)
        assert False
    except QueryException as e:
        print(e)
    try: # Function which does exist with invalid arguments
        example_query = """NAME="asd";
        STARTTIME="1970-01-01";
        ENDTIME="1970-01-02";
        RETURN=nop(STARTTIME);"""
        result = query(example_query, None)
        assert False
    except QueryException as e:
        print(e)
    try: # Function which does exist with invalid arguments
        example_query = """NAME="asd";
        STARTTIME="1970-01-01";
        ENDTIME="1970-01-02";
        RETURN=nop();"""
        result = query(example_query, None)
    except Exception:
        assert False

def test_query2_test_return_value():
    example_query = """
    NAME="asd";
    STARTTIME="2017";
    ENDTIME="2017";
    RETURN=1;
    """
    assert(query(example_query, None) == 1)

    example_query = """
    NAME="asd2";
    STARTTIME="2017";
    ENDTIME="2017";
    RETURN="testing 123";
    """
    assert(query(example_query, None) == "testing 123")
    # TODO: test dict/array

@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_test_query_functions(datastore):
    """
    Just test calling all functions just to see something isn't completely broken
    In many cases the functions doesn't change the result at all, so it's not a test
    for testing the validity of the data the functions transform
    """
    bid = "test_bucket"
    starttime = iso8601.parse_date("1970")
    endtime = starttime + timedelta(hours=1)
    example_query = \
    """
    NAME="test";
    CACHE=FALSE;
    STARTTIME="{}";
    ENDTIME="{}";
    bid="{}";
    events=query_bucket(bid);
    events2=query_bucket(bid);
    events2=filter_keyval(events2, "label", "test1", FALSE);
    events=filter_period_intersect(events, events2);
    events=limit_events(events, 1);
    events=merge_events_by_keys(events, "label");
    events=split_url_events(events);
    events=sort_by_timestamp(events);
    events=sort_by_duration(events);
    asd=nop();
    RETURN=events;
    """.format(starttime, endtime, bid)
    try:
        bucket = datastore.create_bucket(bucket_id=bid, type="test", client="test", hostname="test", name="asd")
        e1 = Event(data={"label": "test1"},
                   timestamp=starttime,
                   duration=timedelta(seconds=1))
        bucket.insert(e1)
        result = query(example_query, datastore)
        print(result)
        assert result[0].data["label"] == "test1"
    finally:
        datastore.delete_bucket(bid)

@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_test_basic_query(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    starttime = iso8601.parse_date("1970")
    endtime = starttime + timedelta(hours=1)
    example_query = \
    """
    NAME="test_query_basic";
    CACHE=TRUE;
    STARTTIME="{}";
    ENDTIME="{}";
    bid1="{}";
    bid2="{}";
    events=query_bucket(bid1);
    intersect_events=query_bucket(bid2);
    RETURN=filter_period_intersect(events, intersect_events);
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
    NAME="test_query_merged_keys";
    CACHE=TRUE;
    STARTTIME="{}";
    ENDTIME="{}";
    bid1="{}";
    events=query_bucket(bid1);
    events=merge_events_by_keys(events, "label1", "label2");
    events=sort_by_duration(events);
    RETURN=events;
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
