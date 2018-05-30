from datetime import datetime, timedelta

from aw_core.models import Event

from aw_transform import flood


now = datetime.now()
td1s = timedelta(seconds=1)


def test_flood_forward():
    events = [
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 15 * td1s, duration=5, data={"b": 1}),
    ]
    flooded = flood(events)
    assert (flooded[0].timestamp + flooded[0].duration) - flooded[1].timestamp == timedelta(0)


def test_flood_forward_merge():
    events = [
        Event(timestamp=now, duration=10),
        Event(timestamp=now + 15 * td1s, duration=5),
    ]
    flooded = flood(events)
    assert len(flooded) == 1
    assert flooded[0].duration == timedelta(seconds=20)


def test_flood_backward():
    events = [
        Event(timestamp=now, duration=5, data={"a": 0}),
        Event(timestamp=now + 10 * td1s, duration=10, data={"b": 1}),
    ]
    flooded = flood(events)
    assert (flooded[0].timestamp + flooded[0].duration) - flooded[1].timestamp == timedelta(0)


def test_flood_backward_merge():
    events = [
        Event(timestamp=now, duration=5),
        Event(timestamp=now + 10 * td1s, duration=10),
    ]
    flooded = flood(events)
    assert len(flooded) == 1
    assert flooded[0].duration == timedelta(seconds=20)
