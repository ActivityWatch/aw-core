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


def test_flood_zero_duration_chain_does_not_leave_overlaps():
    events = [
        Event(timestamp=now, duration=0, data={"title": "first"}),
        Event(timestamp=now, duration=1, data={"title": "first"}),
        Event(timestamp=now, duration=1, data={"title": "second"}),
    ]

    flooded = flood(events)

    assert flooded == [events[2]]
    assert all(
        previous.timestamp + previous.duration <= current.timestamp
        for previous, current in zip(flooded, flooded[1:])
    )


def test_flood_normalization_preserves_non_overlapping_tail():
    events = [
        Event(timestamp=now, duration=10, data={"title": "first"}),
        Event(timestamp=now + 5 * td1s, duration=2, data={"title": "second"}),
        Event(timestamp=now + 12 * td1s, duration=1, data={"title": "third"}),
    ]

    flooded = flood(events)

    assert flooded == [
        Event(timestamp=now, duration=5, data={"title": "first"}),
        Event(timestamp=now + 5 * td1s, duration=7, data={"title": "second"}),
        events[2],
    ]


def test_flood_normalization_merges_same_data_after_zero_duration_event():
    events = [
        Event(timestamp=now, duration=0, data={"title": "zero"}),
        Event(timestamp=now, duration=10, data={"title": "same"}),
        Event(timestamp=now + 5 * td1s, duration=10, data={"title": "same"}),
    ]

    flooded = flood(events)

    assert flooded == [Event(timestamp=now, duration=15, data={"title": "same"})]


def test_flood_negative_small_gap_differing_data():
    events = [
        Event(timestamp=now, duration=100, data={"b": 1}),
        Event(timestamp=now + 99.99 * td1s, duration=100, data={"a": 0}),
    ]
    flooded = flood(events)
    duration = sum((e.duration for e in flooded), timedelta(0))
    assert duration == timedelta(seconds=100 + 99.99)
