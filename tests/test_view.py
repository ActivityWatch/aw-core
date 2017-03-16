from datetime import datetime, timedelta, timezone

import pytest

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_core.views import create_view, query_view, get_view, get_views


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_view(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        e1 = Event(label="test1",
                   timestamp=datetime.now(timezone.utc) - timedelta(hours=100),
                   duration=timedelta(seconds=1))
        e2 = Event(label="test2",
                   timestamp=datetime.now(timezone.utc),
                   duration=timedelta(seconds=2))
        bucket1.insert(10 * [e1])
        bucket1.insert(5 * [e2])
        bucket2.insert(5 * [e1])
        bucket2.insert(10 * [e2])
        example_view = {
            'name': 'exview',
            'created': '2016-09-03',
            'query': {
                'chunk': True,
                'transforms': [
                    {
                        'bucket': bid1,
                        'filters': [{
                            'name': 'include_labels',
                            'labels': ['test1'],
                        }],
                    }, {
                        'bucket': bid2,
                        'filters': [{
                            'name': 'exclude_labels',
                            'labels': ['test1'],
                        }],
                    }
                ]
            }
        }
        # Test creating view
        create_view(example_view)
        assert get_view('exview') == example_view
        assert get_views() == ['exview']
        # Test that output is correct
        result = query_view('exview', datastore)
        assert result['chunks']['test1'] == {'duration': {'value': 10.0, 'unit': 's'}, "keyvals": {}}
        assert result['chunks']['test2'] == {'duration': {'value': 20.0, 'unit': 's'}, "keyvals": {}}
        # Test that limit works
        assert 1 == query_view('exview', datastore, limit=1)["eventcount"]
        # Test that starttime works
        assert 10 == query_view('exview', datastore, start=datetime.now(timezone.utc) - timedelta(hours=1))["eventcount"]
        # Test that endtime works
        assert 10 == query_view('exview', datastore, end=datetime.now(timezone.utc) - timedelta(hours=1))["eventcount"]
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)
