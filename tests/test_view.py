from datetime import datetime, timedelta, timezone
import json
import logging

from nose.tools import assert_equal, assert_dict_equal, assert_list_equal, assert_raises
from nose_parameterized import parameterized

from . import param_testing_buckets_cm, param_datastore_objects

from aw_core.models import Event
from aw_core.views import create_view, query_view, get_view, get_views

import unittest

@parameterized(param_datastore_objects())
def test_view(datastore):
    name = "A label/name for a test bucket"
    bid1 = "bucket1"
    bid2 = "bucket2"
    try:
        bucket1 = datastore.create_bucket(bucket_id=bid1, type="test", client="test", hostname="test", name=name)
        bucket2 = datastore.create_bucket(bucket_id=bid2, type="test", client="test", hostname="test", name=name)
        e1 = Event(label=["test1"],
                   timestamp=datetime.now(timezone.utc),
                   duration=timedelta(seconds=1))
        e2 = Event(label=["test2"],
                   timestamp=datetime.now(timezone.utc),
                   duration=timedelta(seconds=2))
        bucket1.insert(10*[e1]) 
        bucket1.insert(5*[e2]) 
        bucket2.insert(5*[e1]) 
        bucket2.insert(10*[e2]) 
        example_view = \
        {
            'name': 'exview',
            'created': '2016-09-03',
            'query':
            {
                'chunk': True,
                'transforms': 
                [
                {
                    'bucket': bid1,
                    'filters':
                    [
                        {
                            'name': 'include_labels',
                            'labels': ['test1'],
                        }
                    ],
                },
                {
                    'bucket': bid2,
                    'filters':
                    [
                        {
                            'name': 'exclude_labels',
                            'labels': ['test1'],
                        }
                    ],
                },
                ]
            }
        }
        create_view(example_view)
        assert_dict_equal(get_view('exview'), example_view)
        assert_list_equal(get_views(), ['exview'])
        result = query_view('exview', datastore)
        assert_dict_equal(result['chunks']['test1'], {'other_labels':[], 'duration': {'value': 10, 'unit': 's'}})
        assert_dict_equal(result['chunks']['test2'], {'other_labels':[], 'duration': {'value': 20, 'unit': 's'}})
        print(result)
    finally:
        datastore.delete_bucket(bid1)
        datastore.delete_bucket(bid2)

