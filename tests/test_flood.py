from datetime import datetime, timedelta, timezone

from aw_core.models import Event
from aw_transform import flood


now = datetime.now(tz=timezone.utc)
td1s = timedelta(seconds=1)


def test_flood_forward():
    events = [
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 15 * td1s, duration=5, data={"b": 1}),
    ]
    flooded = flood(events)
    assert (flooded[0].timestamp + flooded[0].duration) - flooded[
        1
    ].timestamp == timedelta(0)


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
    assert (flooded[0].timestamp + flooded[0].duration) - flooded[
        1
    ].timestamp == timedelta(0)


def test_flood_backward_merge():
    events = [
        Event(timestamp=now, duration=5),
        Event(timestamp=now + 10 * td1s, duration=10),
    ]
    flooded = flood(events)
    assert len(flooded) == 1
    assert flooded[0].duration == timedelta(seconds=20)


def test_flood_negative_gap_same_data():
    events = [
        Event(timestamp=now, duration=100, data={"a": 0}),
        Event(timestamp=now, duration=5, data={"a": 0}),
    ]
    flooded = flood(events)
    total_duration = sum((e.duration for e in flooded), timedelta(0))
    assert len(flooded) == 1
    assert total_duration == timedelta(seconds=100)


def test_flood_negative_gap_differing_data():
    events = [
        Event(timestamp=now, duration=5, data={"a": 0}),
        Event(timestamp=now, duration=100, data={"b": 1}),
    ]
    flooded = flood(events)
    assert flooded == [events[1]]


def test_flood_negative_small_gap_differing_data():
    events = [
        Event(timestamp=now, duration=100, data={"b": 1}),
        Event(timestamp=now + 99.99 * td1s, duration=100, data={"a": 0}),
    ]
    flooded = flood(events)
    duration = sum((e.duration for e in flooded), timedelta(0))
    assert duration == timedelta(seconds=100 + 99.99)


def test_flood_idempotent():
    events = [
        # slight overlap, same data
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 9 * td1s, duration=5, data={"a": 0}),
        # different data, no overlap
        Event(timestamp=now + 15 * td1s, duration=5, data={"b": 0}),
    ]
    flood_first = flood(events, pulsetime=0)
    flooded = flood_first
    for i in range(2):
        flooded = flood(flooded, pulsetime=0)
        assert flood_first == flooded

    assert sum((e.duration for e in flooded), timedelta(0)) == 19 * td1s


def test_flood_unsafe_gap():
    events = [
        # slight overlap, different data
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 9 * td1s, duration=5, data={"b": 0}),
    ]
    flooded = flood(events, pulsetime=0)

    # The total duration should not exceed the range duration
    assert sum((e.duration for e in flooded), timedelta(0)) == 14 * td1s
