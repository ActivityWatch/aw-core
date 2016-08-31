import logging
import unittest
from datetime import datetime, timedelta, timezone

from nose.tools import assert_equal, assert_dict_equal, assert_raises

from aw_core.models import Event
from aw_core.transform import chunk, filter_afk_events


def test_chunk():
    # Tests the chunk transform
    eventcount = 10
    events = []
    for i in range(eventcount):
        events.append(Event(label=["test", "test2"],
                            timestamp=datetime.now(timezone.utc) + timedelta(seconds=i),
                            duration={"value": 1, "unit": "s"}))
    res = chunk(events)
    logging.debug(res)
    assert_equal(res['eventcount'], eventcount)
    assert_equal(res['chunks']['test']['other_labels'], ["test2"])
    assert_equal(res['chunks']['test']['duration'], {"value": eventcount, "unit": "s"})
    assert_equal(res['chunks']['test2']['other_labels'], ["test"])
    assert_equal(res['chunks']['test2']['duration'], {"value": eventcount, "unit": "s"})


@unittest.skip
def test_filter_afk_events():
    filter_afk_events([])
