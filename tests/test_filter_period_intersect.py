"""Boundary cases for filter_period_intersect.

A filter covers [start, end), so a point at a filter's end isn't covered by it
and yields its own zero-duration piece. Zero-duration pieces at the event's
bounds are kept. aw-server-rust (with ActivityWatch/aw-server-rust#749) gives
the same results; the parity suite in ActivityWatch/activitywatch checks it.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from aw_core.models import Event
from aw_transform import filter_period_intersect

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _e(start: float, duration: float) -> Event:
    return Event(timestamp=T0 + timedelta(seconds=start), duration=duration)


def _spans(events: List[Event]) -> List[Tuple[float, float]]:
    return [
        ((e.timestamp - T0).total_seconds(), e.duration.total_seconds()) for e in events
    ]


def test_zero_duration_filter_at_end_of_another_filter():
    # 6 s isn't inside [0, 6), so the zero-duration filter at 6 s yields its
    # own (zero-duration) piece, just like a lone zero-duration filter at 6 s.
    result = filter_period_intersect([_e(0, 10)], [_e(0, 6), _e(6, 0)])
    assert _spans(result) == [(0, 6), (6, 0)]
    assert _spans(filter_period_intersect([_e(0, 10)], [_e(6, 0)])) == [(6, 0)]


def test_zero_duration_filter_at_end_of_event():
    # A lone zero-duration filter at the event's end yields a zero-duration
    # piece there, like one at the start.
    assert _spans(filter_period_intersect([_e(0, 10)], [_e(10, 0)])) == [(10, 0)]
    assert _spans(filter_period_intersect([_e(0, 10)], [_e(0, 0)])) == [(0, 0)]
    # After a filter that covers the whole event it doesn't: the walk has
    # already moved past the event. Unlike the (0,6),(6,0) case above, this
    # depends on walk order (the same in aw-server-rust); it only affects a
    # zero-duration piece, never durations.
    result = filter_period_intersect([_e(0, 10)], [_e(0, 10), _e(10, 0)])
    assert _spans(result) == [(0, 10)]


def test_adjacent_filters():
    result = filter_period_intersect([_e(0, 10)], [_e(0, 6), _e(6, 4)])
    assert _spans(result) == [(0, 6), (6, 4)]
    assert sum(e.duration.total_seconds() for e in result) == 10
