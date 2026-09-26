"""
Edge-case and randomized tests for union_no_overlap.

Ported from aw-server-rust (ActivityWatch/aw-server-rust#674, #744).
"""

import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pytest

from aw_core import Event
from aw_transform import union_no_overlap

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
SEC = timedelta(seconds=1)


def events_from(
    spans: List[Tuple[int, int]], source: str, unit: timedelta = SEC
) -> List[Event]:
    return [
        Event(
            timestamp=NOW + start * unit,
            duration=duration * unit,
            data={"source": source},
        )
        for start, duration in spans
    ]


def spans(events: List[Event]) -> List[Tuple[float, float, str]]:
    return [
        (
            (e.timestamp - NOW).total_seconds(),
            e.duration.total_seconds(),
            e.data["source"],
        )
        for e in events
    ]


def covers(events: List[Event], t: datetime) -> int:
    return sum(1 for e in events if e.timestamp <= t < e.timestamp + e.duration)


def assert_union_invariants(
    events1: List[Event], events2: List[Event], result: List[Event]
) -> None:
    """Sorted output, no overlap between events with a duration, every events1
    event preserved, and coverage equal to the union of both inputs' coverage."""
    assert all(a.timestamp <= b.timestamp for a, b in zip(result, result[1:])), (
        "result not sorted"
    )
    with_duration = [e for e in result if e.duration > timedelta(0)]
    assert all(
        a.timestamp + a.duration <= b.timestamp
        for a, b in zip(with_duration, with_duration[1:])
    ), "result has overlapping events"
    for e in events1:
        assert e in result, "events1 event missing from result"
    # Coverage is constant between consecutive boundaries, so one point per gap is exact.
    bounds = sorted(
        {
            t
            for e in events1 + events2 + result
            for t in (e.timestamp, e.timestamp + e.duration)
        }
    )
    for a, b in zip(bounds, bounds[1:]):
        t = a + (b - a) / 2
        expected = 1 if covers(events1, t) else min(covers(events2, t), 1)
        assert covers(result, t) == expected, f"wrong coverage at {t}"


EDGE_CASES = [
    # identical ranges
    ([(0, 10)], [(0, 10)]),
    # containment both ways
    ([(0, 10)], [(2, 3)]),
    ([(2, 3)], [(0, 10)]),
    # one events1 event covering several events2 events, followed by more events1
    ([(0, 10), (20, 5)], [(1, 2), (4, 2), (8, 5)]),
    # one events2 event spanning several events1 events
    ([(2, 1), (5, 1), (8, 1)], [(0, 12)]),
    # adjacent / touching boundaries
    ([(0, 5)], [(5, 5)]),
    ([(5, 5)], [(0, 5)]),
    ([(0, 5), (5, 5)], [(0, 10)]),
    # zero-duration events inside, at the start and at the end of the other list's events
    ([(5, 0), (7, 1)], [(0, 10)]),
    ([(0, 0), (2, 1)], [(0, 10)]),
    ([(10, 0)], [(0, 10)]),
    ([(0, 10)], [(5, 0), (12, 1)]),
    ([(3, 0), (3, 2)], [(0, 10)]),
    # empty inputs
    ([], [(0, 1)]),
    ([(0, 1)], []),
]


@pytest.mark.parametrize("a,b", EDGE_CASES)
def test_edge_cases_keep_union_free_of_overlap(a, b):
    events1 = events_from(a, "a")
    events2 = events_from(b, "b")
    result = union_no_overlap(events1, events2)
    assert_union_invariants(events1, events2, result)


def test_containment_of_several_events2():
    # Before the fix, events2 events after the first one inside a long events1
    # event were emitted whole on top of it.
    result = union_no_overlap(
        events_from([(0, 10), (20, 5)], "a"), events_from([(1, 2), (4, 2), (8, 5)], "b")
    )
    assert spans(result) == [(0, 10, "a"), (10, 3, "b"), (20, 5, "a")]


def test_zero_duration_event_splits_events2():
    result = union_no_overlap(
        events_from([(5, 0), (7, 1)], "a"), events_from([(0, 10)], "b")
    )
    assert spans(result) == [
        (0, 5, "b"),
        (5, 0, "a"),
        (5, 2, "b"),
        (7, 1, "a"),
        (8, 2, "b"),
    ]


@pytest.mark.parametrize(
    "point,expected",
    [
        (5, [(0, 10, "a")]),
        (0, [(0, 10, "a")]),
        (10, [(0, 10, "a"), (10, 0, "b")]),
    ],
)
def test_zero_duration_events2_points_yield_to_events1(point, expected):
    result = union_no_overlap(
        events_from([(0, 10)], "a"), events_from([(point, 0)], "b")
    )
    assert spans(result) == expected


def test_fractional_second_boundaries_split_exactly():
    ms = timedelta(milliseconds=1)
    result = union_no_overlap(
        events_from([(1250, 0), (1500, 250)], "a", ms),
        events_from([(1000, 1000)], "b", ms),
    )
    assert spans(result) == [
        (1.0, 0.25, "b"),
        (1.25, 0.0, "a"),
        (1.25, 0.25, "b"),
        (1.5, 0.25, "a"),
        (1.75, 0.25, "b"),
    ]


def test_chained_unions():
    # Mirrors how aw-webui combines devices: events = union_no_overlap(events, host_i)
    hosts = [
        events_from([(0, 4), (10, 0), (12, 6)], "a"),
        events_from([(2, 5), (9, 4), (20, 2)], "b"),
        events_from([(0, 30)], "c"),
    ]
    result: List[Event] = []
    for host in hosts:
        nxt = union_no_overlap(result, host)
        assert_union_invariants(result, host, nxt)
        result = nxt
    assert [s for s in spans(result) if s[1] > 0] == [
        (0, 4, "a"),
        (4, 3, "b"),
        (7, 2, "c"),
        (9, 1, "b"),
        (10, 2, "b"),
        (12, 6, "a"),
        (18, 2, "c"),
        (20, 2, "b"),
        (22, 8, "c"),
    ]


def test_inputs_not_mutated():
    events1 = events_from([(2, 1)], "a")
    events2 = events_from([(0, 10)], "b")
    before = [(e.timestamp, e.duration) for e in events1 + events2]
    union_no_overlap(events1, events2)
    assert [(e.timestamp, e.duration) for e in events1 + events2] == before


def test_random_inputs_keep_union_invariants():
    rng = random.Random(0x2545F491)
    ms = timedelta(milliseconds=1)
    for _ in range(2000):
        lists = []
        for source in ["a", "b"]:
            spans_: List[Tuple[int, int]] = []
            # Millisecond units so boundaries fall on fractional seconds.
            t = rng.randrange(3000)
            for _ in range(rng.randrange(6)):
                duration = 0 if rng.randrange(4) == 0 else 1 + rng.randrange(5000)
                spans_.append((t, duration))
                t += duration + rng.randrange(3000)
            lists.append(events_from(spans_, source, ms))
        result = union_no_overlap(lists[0], lists[1])
        assert_union_invariants(lists[0], lists[1], result)
