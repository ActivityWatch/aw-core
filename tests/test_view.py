from datetime import datetime, timedelta, timezone

import pytest

from .utils import param_datastore_objects

from aw_core.models import Event
from aw_core.views import create_view, query_view, query_multiview, get_view, get_views


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_view(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    now = datetime.now(timezone.utc)
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        e1 = Event(label="test1",
                   timestamp=now - timedelta(hours=100),
                   duration=timedelta(seconds=1))
        e2 = Event(label="test2",
                   timestamp=now,
                   duration=timedelta(seconds=2))
        bucket1.insert(10 * [e1])
        bucket1.insert(5 * [e2])
        bucket2.insert(5 * [e1])
        bucket2.insert(10 * [e2])
        example_view = {
            'name': 'exview',
            'created': '2016-09-03',
            'query': {
                'chunk': 'full',
                'cache': True,
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
        # Test that starttime works
        assert 10 == query_view('exview', datastore, start=now - timedelta(hours=1))["eventcount"]
        # Test that endtime works
        assert 10 == query_view('exview', datastore, end=now - timedelta(hours=1))["eventcount"]
        # Test that multiquery works
        starts = [now-timedelta(hours=1), now-timedelta(hours=100)]
        ends = [now+timedelta(hours=1),now+timedelta(hours=1)]
        assert 20 == query_multiview('exview', datastore, starts, ends)["eventcount"]
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)
