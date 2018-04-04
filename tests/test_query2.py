from datetime import datetime, timedelta, timezone
import iso8601

import pytest

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_analysis.query2 import QueryException, query, _parse_token
from aw_analysis.query2 import QInteger, QVariable, QString, QFunction, QList, QDict


def test_query2_test_token_parsing():
    ns = {}
    (t, token), trash = _parse_token("123", ns)
    assert token == "123"
    assert t == QInteger
    (t, token), trash = _parse_token('"test"', ns)
    assert token == '"test"'
    assert t == QString
    (t, token), trash = _parse_token("'test'", ns)
    assert token == "'test'"
    assert t == QString
    (t, token), trash = _parse_token("test0xDEADBEEF", ns)
    assert token == "test0xDEADBEEF"
    assert t == QVariable
    (t, token), trash = _parse_token("test1337(')')", ns)
    assert token == "test1337(')')"
    assert t == QFunction
    (t, token), trash = _parse_token("[1, 'a', {}]", ns)
    assert token == "[1, 'a', {}]"
    assert t == QList
    (t, token), trash = _parse_token("{'a': 1, 'b}': 2}", ns)
    assert token == "{'a': 1, 'b}': 2}"
    assert t == QDict

    assert _parse_token('', ns) == ((None, ""), "")

    with pytest.raises(QueryException):
        _parse_token(None, ns)

    with pytest.raises(QueryException):
        _parse_token('"', ns)

    with pytest.raises(QueryException):
        _parse_token("#", ns)


def test_dict():
    ds = None
    ns = {}
    d_str = "{'a': {'a': {'a': 1}}, 'b': {'b': ':'}}"
    d = QDict.parse(d_str, ns)
    expected_res = {'a': {'a': {'a': 1}}, 'b': {'b': ':'}}
    assert expected_res == d.interpret(ds, ns)

    # Key in dict is not a string
    with pytest.raises(QueryException):
        d_str = "{b: 1}"
        d = Dict.parse(d_str, ns)

    # Char following key string is not a :
    with pytest.raises(QueryException):
        d_str = "{'test'p 1}"
        d = Dict.parse(d_str, ns)

    with pytest.raises(QueryException):
        d_str = "{'test': #}"
        d = Dict.parse(d_str, ns)

    # Semicolon without key
    with pytest.raises(QueryException):
        d_str = "{:}"
        d = Dict.parse(d_str, ns)

    # Trailing comma
    with pytest.raises(QueryException):
        d_str = "{'test':1,}"
        d = Dict.parse(d_str, ns)


def test_list():
    ds = None
    ns = {}
    l_str = "[1,2,[3,4],5]"
    l = QList.parse(l_str, ns)
    expected_res = [1,2,[3,4],5]
    assert expected_res == l.interpret(ds, ns)

    l_str = "[]"
    l = QList.parse(l_str, ns)
    expected_res = []
    assert expected_res == l.interpret(ds, ns)

    # Comma without pre/post value
    with pytest.raises(QueryException):
        l_str = "[,]"
        l = List.parse(l_str, ns)

    # Comma without post value
    with pytest.raises(QueryException):
        l_str = "[1,]"
        l = List.parse(l_str, ns)

    # Comma without pre value
    with pytest.raises(QueryException):
        l_str = "[,2]"
        l = List.parse(l_str, ns)


def test_query2_bogus_query():
    qname = "test"
    qstartdate = datetime.now(tz=timezone.utc)
    qenddate = qstartdate

    # Nothing to assign
    with pytest.raises(QueryException):
        example_query = "a="
        query(qname, example_query, qstartdate, qenddate, None)

    # Assign to non-variable
    with pytest.raises(QueryException):
        example_query = "1=2"
        query(qname, example_query, qstartdate, qenddate, None)

    # Unclosed function
    with pytest.raises(QueryException):
        example_query = "a=unclosed_function(var1"
        query(qname, example_query, qstartdate, qenddate, None)

    # Call a function which doesn't exist
    with pytest.raises(QueryException):
        example_query = "a=non_existing_function() "
        query(qname, example_query, qstartdate, qenddate, None)

    # Two tokens in assignment
    with pytest.raises(QueryException):
        example_query = "asd nop()=2"
        query(qname, example_query, qstartdate, qenddate, None)

    # Unclosed string
    with pytest.raises(QueryException):
        example_query = 'asd="something is wrong with me'
        query(qname, example_query, qstartdate, qenddate, None)

    # Two tokens in value
    with pytest.raises(QueryException):
        example_query = "asd=asd1 asd2"
        query(qname, example_query, qstartdate, qenddate, None)


def test_query2_query_function_calling():
    qname = "asd"
    starttime = iso8601.parse_date("1970-01-01")
    endtime = iso8601.parse_date("1970-01-02")

    # Function which doesn't exist
    with pytest.raises(QueryException):
        example_query = "RETURN=asd();"
        query(qname, example_query, starttime, endtime, None)

    # Function which does exist with invalid arguments
    with pytest.raises(QueryException):
        example_query = "RETURN=nop(badarg);"
        query(qname, example_query, starttime, endtime, None)

    # Function which does exist with valid arguments
    example_query = "RETURN=nop();"
    query(qname, example_query, starttime, endtime, None)


def test_query2_return_value():
    qname = "asd"
    starttime = iso8601.parse_date("1970-01-01")
    endtime = iso8601.parse_date("1970-01-02")
    example_query = "RETURN=1;"
    result = query(qname, example_query, starttime, endtime, None)
    assert(result == 1)

    example_query = "RETURN='testing 123'"
    result = query(qname, example_query, starttime, endtime, None)
    assert(result == "testing 123")

    example_query = "RETURN={'a': 1}"
    result = query(qname, example_query, starttime, endtime, None)
    assert(result == {'a': 1})

    try:  # Nothing to return
        example_query = "a=1"
        result = query(qname, example_query, starttime, endtime, None)
        assert False
    except QueryException:
        pass


def test_query2_multiline():
    qname = "asd"
    starttime = iso8601.parse_date("1970-01-01")
    endtime = iso8601.parse_date("1970-01-02")
    example_query = """
my_multiline_string="a
b";
RETURN=my_multiline_string;
    """
    result = query(qname, example_query, starttime, endtime, None)
    assert result == "a\nb"


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_query_functions(datastore):
    """
    Just test calling all functions just to see something isn't completely broken
    In many cases the functions doesn't change the result at all, so it's not a test
    for testing the validity of the data the functions transform
    """
    bid = "test_bucket"
    qname = "test"
    starttime = iso8601.parse_date("1970")
    endtime = starttime + timedelta(hours=1)

    example_query = """
    bid = "{bid}";
    events = query_bucket(bid);
    events2 = query_bucket(bid);
    events2 = filter_keyvals(events2, "label", "test1");
    events2 = exclude_keyvals(events2, "label", "test2");
    events = filter_period_intersect(events, events2);
    events = limit_events(events, 1);
    events = merge_events_by_keys(events, "label");
    events = split_url_events(events);
    events = sort_by_timestamp(events);
    events = sort_by_duration(events);
    eventcount = query_bucket_eventcount(bid);
    asd = nop();
    RETURN = {{"events": events, "eventcount": eventcount}};
    """.format(bid=bid)

    try:
        bucket = datastore.create_bucket(bucket_id=bid, type="test", client="test", hostname="test", name="asd")
        e1 = Event(data={"label": "test1"},
                   timestamp=starttime,
                   duration=timedelta(seconds=1))
        bucket.insert(e1)
        result = query(qname, example_query, starttime, endtime, datastore)
        assert result["eventcount"] == 1
        assert result["events"][0].data["label"] == "test1"
    finally:
        datastore.delete_bucket(bid)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_basic_query(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    qname = "test_query_basic"
    starttime = iso8601.parse_date("1970")
    endtime = starttime + timedelta(hours=1)

    example_query = """
    bid1 = "{bid1}";
    bid2 = "{bid2}";
    events = query_bucket(bid1);
    intersect_events = query_bucket(bid2);
    RETURN = filter_period_intersect(events, intersect_events);
    """.format(bid1=bid1, bid2=bid2)

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
        result = query(qname, example_query, starttime, endtime, datastore)
        # Assert
        assert(len(result) == 1)
        assert(result[0]["data"]["label"] == "test1")
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_test_merged_keys(datastore):
    name = "A label/name for a test bucket"
    bid = "bucket1"
    qname = "test_query_merged_keys"
    starttime = iso8601.parse_date("2080")
    endtime = starttime + timedelta(hours=1)

    example_query = """
    bid = "{bid}";
    events = query_bucket(bid);
    events = merge_events_by_keys(events, "label1", "label2");
    events = sort_by_duration(events);
    eventcount = query_bucket_eventcount(bid);
    RETURN = {{"events": events, "eventcount": eventcount}};
    """.format(bid=bid)

    try:
        # Setup buckets
        bucket1 = datastore.create_bucket(bucket_id=bid, type="test", client="test", hostname="test", name=name)
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
        result = query(qname, example_query, starttime, endtime, datastore)
        # Assert
        print(result)
        assert(len(result["events"]) == 2)
        assert(result["eventcount"] == 3)
        assert(result["events"][0]["data"]["label1"] == "test1")
        assert(result["events"][0]["data"]["label2"] == "test1")
        assert(result["events"][0]["duration"] == timedelta(seconds=2))
        assert(result["events"][1]["data"]["label1"] == "test1")
        assert(result["events"][1]["data"]["label2"] == "test2")
        assert(result["events"][1]["duration"] == timedelta(seconds=1))
    finally:
        datastore.delete_bucket(bid)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_fancy_query(datastore):
    """
    Tests:
     - find_bucket
     - simplify_window_titles
    """
    name = "A label/name for a test bucket"
    bid1 = "bucket-the-one"
    bid2 = "bucket-not-the-one"
    qname = "test_query_basic_fancy"
    starttime = iso8601.parse_date("1970")
    endtime = starttime + timedelta(hours=1)

    example_query = """
    bid = find_bucket("{}");
    events = query_bucket(bid);
    RETURN = simplify_window_titles(events, "title");
    """.format(bid1[:10])

    try:
        # Setup buckets
        bucket_main = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket_other = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        # Prepare buckets
        e1 = Event(data={"title": "(2) YouTube"},
                   timestamp=starttime,
                   duration=timedelta(seconds=1))
        bucket_main.insert(e1)
        # Query
        result = query(qname, example_query, starttime, endtime, datastore)
        # Assert
        assert(result[0]["data"]["title"] == "YouTube")
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)
