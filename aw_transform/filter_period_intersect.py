import logging
from typing import List, Iterable, Tuple
from copy import deepcopy

from aw_core import Event
from timeslot import Timeslot

logger = logging.getLogger(__name__)


def _get_event_period(event: Event) -> Timeslot:
    start = event.timestamp
    end = start + event.duration
    return Timeslot(start, end)


def _replace_event_period(event: Event, period: Timeslot) -> Event:
    e = deepcopy(event)
    e.timestamp = period.start
    e.duration = period.duration
    return e


def _intersecting_eventpairs(
    events1: List[Event], events2: List[Event]
) -> Iterable[Tuple[Event, Event, Timeslot]]:
    """A generator that yields each overlapping pair of events from two eventlists along with a Timeslot of the intersection"""
    events1.sort(key=lambda e: e.timestamp)
    events2.sort(key=lambda e: e.timestamp)
    e1_i = 0
    e2_i = 0
    while e1_i < len(events1) and e2_i < len(events2):
        e1 = events1[e1_i]
        e2 = events2[e2_i]
        e1_p = _get_event_period(e1)
        e2_p = _get_event_period(e2)

        ip = e1_p.intersection(e2_p)
        if ip:
            # If events intersected, yield events
            yield (e1, e2, ip)
            if e1_p.end <= e2_p.end:
                e1_i += 1
            else:
                e2_i += 1
        else:
            # No intersection, check if event is before/after filterevent
            if e1_p.end <= e2_p.start:
                # Event ended before filter event started
                e1_i += 1
            elif e2_p.end <= e1_p.start:
                # Event started after filter event ended
                e2_i += 1
            else:
                logger.error("Should be unreachable, skipping period")
                e1_i += 1
                e2_i += 1


def filter_period_intersect(
    events: List[Event], filterevents: List[Event]
) -> List[Event]:
    """
    Filters away all events or time periods of events in which a
    filterevent does not have an intersecting time period.

    Useful for example when you want to filter away events or
    part of events during which a user was AFK.

    Usage:
      windowevents_notafk = filter_period_intersect(windowevents, notafkevents)

    Example:
      .. code-block:: none

        events1   |   =======        ======== |
        events2   | ------  ---  ---   ----   |
        result    |   ====  =          ====   |

    A JavaScript version used to exist in aw-webui but was removed in `this PR <https://github.com/ActivityWatch/aw-webui/pull/48>`_.
    """

    events = sorted(events)
    filterevents = sorted(filterevents)

    return [
        _replace_event_period(e1, ip)
        for (e1, _, ip) in _intersecting_eventpairs(events, filterevents)
    ]


def period_union(events1: List[Event], events2: List[Event]) -> List[Event]:
    """
    Takes a list of two events and returns a new list of events covering the union
    of the timeperiods contained in the eventlists with no overlapping events.

    .. warning:: This function strips all data from events as it cannot keep it consistent.

    Example:
      .. code-block:: none

        events1   |   -------       --------- |
        events2   | ------  ---  --    ----   |
        result    | -----------  -- --------- |
    """
    events = sorted(events1 + events2)
    merged_events = []
    if events:
        merged_events.append(events.pop(0))
    for e in events:
        last_event = merged_events[-1]

        e_p = _get_event_period(e)
        le_p = _get_event_period(last_event)

        if not e_p.gap(le_p):
            new_period = e_p.union(le_p)
            merged_events[-1] = _replace_event_period(last_event, new_period)
        else:
            merged_events.append(e)
    for event in merged_events:
        # Clear data
        event.data = {}
    return merged_events


def union(events1: List[Event], events2: List[Event]) -> List[Event]:
    """
    Concatenates and sorts union of 2 event lists and removes duplicates.

    Example:
      Merges events from a backup-bucket with events from a "living" bucket.

      .. code-block:: python

        events = union(events_backup, events_living)
    """

    events1 = sorted(events1, key=lambda e: (e.timestamp, e.duration))
    events2 = sorted(events2, key=lambda e: (e.timestamp, e.duration))
    events_union = []

    e1_i = 0
    e2_i = 0
    while e1_i < len(events1) and e2_i < len(events2):
        e1 = events1[e1_i]
        e2 = events2[e2_i]

        if e1 == e2:
            events_union.append(e1)
            e1_i += 1
            e2_i += 1
        else:
            if e1.timestamp < e2.timestamp:
                events_union.append(e1)
                e1_i += 1
            elif e1.timestamp > e2.timestamp:
                events_union.append(e2)
                e2_i += 1
            elif e1.duration < e2.duration:
                events_union.append(e1)
                e1_i += 1
            else:
                events_union.append(e2)
                e2_i += 1

    if e1_i < len(events1):
        events_union.extend(events1[e1_i:])

    if e2_i < len(events2):
        events_union.extend(events2[e2_i:])

    return events_union
