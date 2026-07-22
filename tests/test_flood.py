from datetime import datetime, timedelta, timezone

from aw_core.models import Event
from aw_transform import flood


now = datetime.now(tz=timezone.utc)
td1s = timedelta(seconds=1)


def test_flood_differing_data_meet_at_gap_midpoint():
    events = [
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 14 * td1s, duration=5, data={"b": 1}),
    ]

    flooded = flood(events)

    assert flooded == [
        Event(timestamp=now, duration=12, data={"a": 0}),
        Event(timestamp=now + 12 * td1s, duration=7, data={"b": 1}),
    ]


def test_flood_forward_merge():
    events = [
        Event(timestamp=now, duration=10),
        Event(timestamp=now + 15 * td1s, duration=5),
    ]
    flooded = flood(events)
    assert len(flooded) == 1
    assert flooded[0].duration == timedelta(seconds=20)


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
        Event(timestamp=now + 5 * td1s, duration=4.5, data={"title": "second"}),
        Event(timestamp=now + 9.5 * td1s, duration=3.5, data={"title": "third"}),
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


def test_flood_with_custom_pulsetime():
    # Events with a 30s gap between them (simulating 30s poll_time)
    events = [
        Event(timestamp=now, duration=5, data={"a": 0}),
        Event(timestamp=now + 35 * td1s, duration=5, data={"b": 1}),
    ]

    # Default pulsetime=5: gap (30s) > pulsetime, so no flooding
    flooded_default = flood(events)
    assert len(flooded_default) == 2
    total_duration_default = sum((e.duration for e in flooded_default), timedelta(0))
    assert total_duration_default == timedelta(seconds=10)

    # pulsetime=31: gap (30s) <= pulsetime, so both events extend to midpoint
    flooded_custom = flood(events, pulsetime=31)
    assert flooded_custom == [
        Event(timestamp=now, duration=20, data={"a": 0}),
        Event(timestamp=now + 20 * td1s, duration=20, data={"b": 1}),
    ]


def test_flood_idempotent():
    """flood() applied repeatedly should produce the same result."""
    events = [
        # slight overlap, same data
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 9 * td1s, duration=5, data={"a": 0}),
        # different data, no overlap
        Event(timestamp=now + 15 * td1s, duration=5, data={"b": 0}),
    ]
    flood_first = flood(events, pulsetime=0)
    flooded = flood_first
    for _ in range(2):
        flooded = flood(flooded, pulsetime=0)
        assert flood_first == flooded

    assert sum((e.duration for e in flooded), timedelta(0)) == 19 * td1s


def test_flood_unsafe_gap():
    """Overlapping events with differing data must not double-count time."""
    events = [
        Event(timestamp=now, duration=10, data={"a": 0}),
        Event(timestamp=now + 9 * td1s, duration=5, data={"b": 0}),
    ]
    flooded = flood(events, pulsetime=0)

    # Total duration must not exceed the span covered by the two events
    assert sum((e.duration for e in flooded), timedelta(0)) == 14 * td1s
