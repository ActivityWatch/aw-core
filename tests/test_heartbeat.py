from datetime import datetime, timedelta, timezone
import json
import logging

from aw_core.models import Event
from aw_transform import heartbeat_merge, heartbeat_reduce

import unittest


def test_heartbeat_merge():
    """Events should merge"""
    now = datetime.now()
    td_1s = timedelta(seconds=1)

    last_event, heartbeat = Event(timestamp=now), Event(timestamp=now + td_1s)
    merged = heartbeat_merge(last_event, heartbeat, pulsetime=2)
    assert merged is not None


def test_heartbeat_merge_fail():
    """Merge should not happen"""
    now = datetime.now()
    td_1s = timedelta(seconds=1)

    # timestamp of heartbeat more than pulsetime away
    last_event, heartbeat = (
        Event(timestamp=now, data={"label": "test"}),
        Event(timestamp=now + 3 * td_1s, data={"label": "test"}),
    )
    merged = heartbeat_merge(last_event, heartbeat, pulsetime=2)
    assert merged is None

    # labels not identical
    last_event, heartbeat = (
        Event(timestamp=now, data={"label": "test"}),
        Event(timestamp=now + td_1s, data={"label": "test2"}),
    )
    merged = heartbeat_merge(last_event, heartbeat, pulsetime=2)
    assert merged is None


def test_heartbeat_reduce():
    """Events should reduce"""
    now = datetime.now()
    td_1s = timedelta(seconds=1)

    # Check that empty list works
    assert not heartbeat_reduce([], pulsetime=1)

    events = [
        Event(timestamp=now, data={"label": "test"}),
        Event(timestamp=now + td_1s, data={"label": "test"}),
    ]
    reduced_events = heartbeat_reduce(events, pulsetime=2)
    assert len(reduced_events) == 1


def test_heartbeat_same_timestamp():
    now = datetime.now()
    td_1s = timedelta(seconds=1)
    td_0s = timedelta(seconds=0)

    e1 = Event(timestamp=now, duration=td_1s, data={"label": "test"})
    e2 = Event(timestamp=now, duration=td_0s, data={"label": "test"})

    # Should merge
    res = heartbeat_reduce(list([e1, e2]), pulsetime=5)
    assert len(res) == 1
    assert res[0].duration == td_1s

    # Order shouldn't matter, should merge
    res = heartbeat_reduce([e2, e1], pulsetime=5)
    assert len(res) == 1
    assert res[0].duration == td_1s


def test_heartbeat_reduce_fail():
    """Events should not reduce"""
    now = datetime.now()
    td_1s = timedelta(seconds=1)

    events = [
        Event(timestamp=now, data={"label": "test"}),
        Event(timestamp=now + 3 * td_1s, data={"label": "test"}),
    ]
    reduced_events = heartbeat_reduce(events, pulsetime=2)
    assert len(reduced_events) == 2
