from datetime import datetime, timedelta, timezone

import pytest
from pytest import raises

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_core.views import create_view, query_view, query_multiview, get_view, get_views, ViewException

bid1 = "bucket1"
bid2 = "bucket2"
example_view = {
    'name': 'exview',
    'created': '2016-09-03',
    'query': {
        'chunk': 'label',
        'cache': True,
        'transforms': [
            {
                'bucket': bid1,
                'filters': [{
                    'name': 'include_keyvals',
                    'key': 'label',
                    'vals': ['test1'],
                }],
            }, {
                'bucket': bid2,
                'filters': [{
                    'name': 'exclude_keyvals',
                    'key': 'label',
                    'vals': ['test1'],
                }],
            }
        ]
    }
}

def test_get_view():
    assert None == get_view("exview")
    create_view(example_view)
    assert None != get_view("exview")

@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_bad_query(datastore):
    try:
        name = "A test bucket"
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        # Check that it raises an exception when the view doesn't exist
        with raises(ViewException):
            query_view("nosuchview", datastore, [], [])
        with raises(ViewException):
            query_multiview("nosuchview", datastore, [], [])

        create_view(example_view)

        # Check that it raises an exception when there are more/less start points than end points
        with raises(ViewException):
            query_multiview("exview", datastore, [1], [])
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)

@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_view(datastore):
    name = "A test bucket"
    now = datetime.now(timezone.utc)
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        e1 = Event(data={"label": "test1"},
                   timestamp=now - timedelta(hours=100),
                   duration=timedelta(seconds=1))
        e2 = Event(data={"label": "test2"},
                   timestamp=now,
                   duration=timedelta(seconds=2))
        bucket1.insert(10 * [e1])
        bucket1.insert(5 * [e2])
        bucket2.insert(5 * [e1])
        bucket2.insert(10 * [e2])
        # Test creating view
        create_view(example_view)
        assert example_view == get_view('exview')
        assert ['exview'] == get_views()
        # Test that output is correct
        result = query_view('exview', datastore)
        assert result['eventcount'] == 20
        assert result['duration'] == 30
        assert result['chunks']['test1'] == {'data': {}, 'duration': 10.0}
        assert result['chunks']['test2'] == {'data': {}, 'duration': 20.0}
        # Test that starttime works
        assert 10 == query_view('exview', datastore, start=now - timedelta(hours=1))["eventcount"]
        # Test starttime cached which is past now isn't cached
        assert 10 == query_view('exview', datastore, start=now - timedelta(hours=1))["eventcount"]
        # Test that endtime works
        assert 10 == query_view('exview', datastore, start=now-timedelta(hours=200), end=now - timedelta(hours=1))["eventcount"]
        # Test endtime cached
        assert 10 == query_view('exview', datastore, start=now-timedelta(hours=200), end=now - timedelta(hours=1))["eventcount"]
        # Test that multiquery works
        starts = [now-timedelta(hours=1), now-timedelta(hours=100)]
        ends = [now+timedelta(hours=1),now+timedelta(hours=1)]
        assert 20 == query_multiview('exview', datastore, starts, ends)["eventcount"]
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)
