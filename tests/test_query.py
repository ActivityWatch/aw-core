from datetime import datetime, timedelta, timezone

import pytest

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_transform.query import QueryException, query


"""

    Bucket

"""


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_unspecified_bucket(datastore):
    """
        Asserts that a exception is raised when a query doesn't have a specified bucket
    """
    example_query = {
        'chunk': 'label',
        'transforms': [{}]
    }
    # Query and handle QueryException
    with pytest.raises(QueryException):
        result = query(example_query, datastore)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_invalid_bucket(datastore):
    """
        Asserts that a exception is raised when a query has specified a bucket that is not a string
    """
    example_query = {
        'chunk': 'label',
        'transforms': [{
            'bucket': 123,
        }]
    }
    # Query and handle QueryException
    with pytest.raises(QueryException):
        result = query(example_query, datastore)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_nonexisting_bucket(datastore):
    """
        Asserts that a exception is raised when a query has specified a bucket that does not exist
    """
    print(datastore)
    example_query = {
        'chunk': 'label',
        'transforms': [{
            'bucket': "There is no bucket with this name",
        }]
    }
    # Query and handle QueryException
    with pytest.raises(QueryException):
        result = query(example_query, datastore)

"""

    Filter

"""

@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_unspecified_filter(datastore):
    """
        Asserts that a exception is raised when a query has a filter where the filtername is not specified
    """
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        example_query = {
            'chunk': 'label',
            'transforms': [{
                'bucket': bid1,
                'filters': [{}],
            }]
        }
        # Query and handle QueryException
        with pytest.raises(QueryException):
            result = query(example_query, datastore)
    finally:
        datastore.delete_bucket(bid1)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_invalid_filter(datastore):
    """
        Asserts that a exception is raised when a query has a filter name that is not a string
    """
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        example_query = {
            'chunk': 'label',
            'transforms': [{
                'bucket': bid1,
                'filters': [{
                    'name': 123,
                }],
            }]
        }
        # Query and handle QueryException
        with pytest.raises(QueryException):
            result = query(example_query, datastore)
    finally:
        datastore.delete_bucket(bid1)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_nonexisting_filter(datastore):
    """
        Asserts that a exception is raised when a query tries to use a filter that doesn't exist
    """
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        example_query = {
            'chunk': 'label',
            'transforms': [{
                'bucket': bid1,
                'filters': [{
                    'name': 'There is no filter with this name',
                }],
            }]
        }
        # Query and handle QueryException
        with pytest.raises(QueryException):
            result = query(example_query, datastore)
    finally:
        datastore.delete_bucket(bid1)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_filter_labels_with_full_chunking(datastore):
    """
        Test include/exclude label filters as well as eventcount limit and start/end filtering
    """
    print(type(datastore.storage_strategy))
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    now = datetime.now(timezone.utc)
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        e1 = Event(data={"label": "test1", "key": "val"},
                   timestamp=now - timedelta(hours=100),
                   duration=timedelta(seconds=1))
        e2 = Event(data={"label": "test2"},
                   timestamp=now,
                   duration=timedelta(seconds=2))
        bucket1.insert(10 * [e1])
        bucket1.insert(5 * [e2])
        bucket2.insert(5 * [e1])
        bucket2.insert(10 * [e2])
        example_query = {
            'chunk': 'label',
            'transforms': [{
                'bucket': bid1,
                'filters': [{
                    'name': 'include_keyvals',
                    'key': 'label',
                    'vals': ['test1'],
                }]
            }, {
                'bucket': bid2,
                'filters': [{
                    'name': 'exclude_keyvals',
                    'key': 'label',
                    'vals': ['test1'],
                }],
            }]
        }
        # Test that output is correct
        result = query(example_query, datastore)
        print(result)
        assert result['chunks']['test1'] == {'duration': 10.0, 'data': {'key':{'duration': 10.0, 'values': {'val': {'duration': 10.0}}}}}
        assert result['chunks']['test2'] == {'duration': 20.0, 'data': {}}
        assert result['duration'] == 30.0
        # Test that limit works
        assert 1 == query(example_query, datastore, limit=1)["eventcount"]
        # Test that starttime works
        assert 10 == query(example_query, datastore, start=now - timedelta(hours=1))["eventcount"]
        # Test that endtime works
        assert 10 == query(example_query, datastore, end=now - timedelta(hours=1))["eventcount"]
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query_filter_labels(datastore):
    """
        Timeperiod intersect and eventlist
    """
    print(type(datastore.storage_strategy))
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
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
        example_query = {
            'transforms': [{
                'bucket': bid1,
                'filters': [{
                    'name': 'timeperiod_intersect',
                    'transforms': [{
                        'bucket': bid2,
                    }]
                }],
            }]
        }
        # Test that output is correct
        result = query(example_query, datastore)
        print(result)
        assert 1 == len(result['eventlist'])
        assert result['eventlist'][0]["data"] == e1.to_json_dict()["data"]
        assert result['duration'] == 1.0
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)
