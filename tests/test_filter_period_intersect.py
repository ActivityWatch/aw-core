"""Boundary cases for filter_period_intersect.

Filter events are half-open intervals [start, end): a point at a filter's
end isn't covered by it. These pin the behaviour that aw-server-rust matches
(checked by the parity suite in ActivityWatch/activitywatch).
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
    # 10 s is outside the event's [0, 10), so the zero filter there adds nothing
    result = filter_period_intersect([_e(0, 10)], [_e(0, 10), _e(10, 0)])
    assert _spans(result) == [(0, 10)]


def test_adjacent_filters():
    result = filter_period_intersect([_e(0, 10)], [_e(0, 6), _e(6, 4)])
    assert _spans(result) == [(0, 6), (6, 4)]
    assert sum(e.duration.total_seconds() for e in result) == 10
