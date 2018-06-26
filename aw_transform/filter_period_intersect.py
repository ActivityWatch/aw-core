import logging
from typing import List, Iterable, Tuple
from copy import deepcopy

from aw_core.models import Event
from aw_core import TimePeriod

logger = logging.getLogger(__name__)


def _get_event_period(event: Event) -> TimePeriod:
    # TODO: Better parsing of event duration
    start = event.timestamp
    end = start + event.duration
    return TimePeriod(start, end)


def _replace_event_period(event: Event, period: TimePeriod) -> Event:
    e = deepcopy(event)
    e.timestamp = period.start
    e.duration = period.duration
    return e


def _concurrent_eventpairs(events1, events2) -> Iterable[Tuple[Event, Event]]:
    e1_i = 0
    e2_i = 0
    while e1_i < len(events1) and e2_i < len(events2):
        e1 = events1[e1_i]
        e2 = events2[e2_i]
        e1_p = _get_event_period(e1)
        e2_p = _get_event_period(e2)

        ip = e1_p.intersection(e2_p)
        if ip:
            # If events intersected, add event with intersected duration and try next event
            yield (e1, e2)
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


def filter_period_intersect(events: List[Event], filterevents: List[Event]) -> List[Event]:
    """
    Filters away all events or time periods of events in which a
    filterevent does not have an intersecting time period.

    Useful for example when you want to filter away events or
    part of events during which a user was AFK.

    Example:
      windowevents_notafk = filter_period_intersect(windowevents, notafkevents)

    A JavaScript version used to exist in aw-webui but was removed in `this PR <https://github.com/ActivityWatch/aw-webui/pull/48>`_.
    """

    events = sorted(events, key=lambda e: e.timestamp)
    filterevents = sorted(filterevents, key=lambda e: e.timestamp)
    filtered_events = []

    for (e1, e2) in _concurrent_eventpairs(events, filterevents):
        e1_p = _get_event_period(e1)
        e2_p = _get_event_period(e2)

        ip = e1_p.intersection(e2_p)
        if ip:
            # If events intersected, add event with intersected duration
            filtered_events.append(_replace_event_period(e1, ip))

    return filtered_events


def period_union(events1: List[Event], events2: List[Event]) -> List[Event]:
    events = sorted(events1 + events2, key=lambda e: e.timestamp)
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
    return merged_events
