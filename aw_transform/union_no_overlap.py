"""
Originally from aw-research
"""

from copy import deepcopy
from typing import List, Tuple, Optional
from datetime import datetime, timedelta, timezone

from aw_core import Event


def _split_event(e: Event, dt: datetime) -> Tuple[Event, Optional[Event]]:
    if e.timestamp < dt < e.timestamp + e.duration:
        e1 = deepcopy(e)
        e2 = deepcopy(e)
        e1.duration = dt - e.timestamp
        e2.timestamp = dt
        e2.duration = (e.timestamp + e.duration) - dt
        return (e1, e2)
    else:
        return (e, None)


def test_split_event():
    now = datetime(2018, 1, 1, 0, 0).astimezone(timezone.utc)
    td1h = timedelta(hours=1)
    e = Event(timestamp=now, duration=2 * td1h, data={})
    e1, e2 = _split_event(e, now + td1h)
    assert e1.timestamp == now
    assert e1.duration == td1h
    assert e2
    assert e2.timestamp == now + td1h
    assert e2.duration == td1h


def union_no_overlap(events1: List[Event], events2: List[Event]) -> List[Event]:
    """Merges two eventlists and removes overlap, the first eventlist will have precedence

    Both lists are expected to be sorted by timestamp and free of internal overlap.

    Example:
      events1  | xxx    xx     xxx     |
      events1  |  ----     ------   -- |
      result   | xxx--  xx ----xxx  -- |
    """
    events1 = deepcopy(events1)
    events2 = deepcopy(events2)

    # Same algorithm as aw-server-rust (ActivityWatch/aw-server-rust#674, #744).
    # Positions are compared directly rather than via interval intersection,
    # which is false for zero-duration events: a zero-duration e1 inside e2 must
    # still split e2, or e2 is emitted whole and overlaps later e1 events.
    events_union: List[Event] = []
    e1_i = 0
    e2_i = 1
    pending: Optional[Event] = events2[0] if events2 else None

    def next_e2() -> Optional[Event]:
        nonlocal e2_i
        if e2_i < len(events2):
            e2_i += 1
            return events2[e2_i - 1]
        return None

    while e1_i < len(events1) and pending is not None:
        e1 = events1[e1_i]
        e2 = pending
        e1_end = e1.timestamp + e1.duration
        e2_end = e2.timestamp + e2.duration

        if e2.timestamp < e1.timestamp:
            # e2 starts first: emit the part before e1, keep the rest pending.
            prefix, remainder = _split_event(e2, e1.timestamp)
            events_union.append(prefix)
            pending = remainder if remainder else next_e2()
        elif e2.timestamp < e1_end:
            # e1 starts first (or together) and covers the start of e2.
            if e2_end <= e1_end:
                # e2 is fully covered. Keep e1, it may cover more events.
                pending = next_e2()
                continue
            e2.timestamp = e1_end
            e2.duration = e2_end - e1_end
            events_union.append(e1)
            e1_i += 1
        else:
            # e1 ends before (or where) e2 starts.
            events_union.append(e1)
            e1_i += 1

    events_union += events1[e1_i:]
    if pending is not None:
        events_union.append(pending)
    events_union += events2[e2_i:]
    return events_union
