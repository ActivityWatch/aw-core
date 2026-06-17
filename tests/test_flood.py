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
    assert flooded == events


def test_flood_negative_small_gap_differing_data():
    events = [
        Event(timestamp=now, duration=100, data={"b": 1}),
        Event(timestamp=now + 99.99 * td1s, duration=100, data={"a": 0}),
    ]
    flooded = flood(events)
    duration = sum((e.duration for e in flooded), timedelta(0))
    assert duration == timedelta(seconds=100 + 99.99)


def test_flood_large_gap_not_filled_with_default_pulsetime():
    """A gap larger than the default pulsetime (5s) should not be filled."""
    events = [
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 25 * td1s, duration=10, data={"b": 1}),
    ]
    flooded = flood(events)
    # Gap is 25s - 10s = 15s, larger than default pulsetime=5; stays as a gap
    assert len(flooded) == 2
    gap = flooded[1].timestamp - (flooded[0].timestamp + flooded[0].duration)
    assert gap > timedelta(0)


def test_flood_large_gap_filled_with_custom_pulsetime():
    """A gap larger than default pulsetime should be filled when pulsetime is increased.

    This is the fix for ActivityWatch/activitywatch#1177: users with a high
    poll interval (e.g. 30s) need to call flood with pulsetime=poll_time+1.
    """
    events = [
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 25 * td1s, duration=10, data={"b": 1}),
    ]
    flooded = flood(events, pulsetime=20)
    # Gap is 15s, within pulsetime=20; should be filled
    assert len(flooded) == 2
    gap = flooded[1].timestamp - (flooded[0].timestamp + flooded[0].duration)
    assert gap == timedelta(0)
