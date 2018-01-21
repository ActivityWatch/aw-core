import logging
from typing import List
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

    e_i = 0
    f_i = 0
    while e_i < len(events) and f_i < len(filterevents):
        event = events[e_i]
        filterevent = filterevents[f_i]
        ep = _get_event_period(event)
        fp = _get_event_period(filterevent)

        ip = ep.intersection(fp)
        if ip:
            # If events itersected, add event with intersected duration and try next event
            filtered_events.append(_replace_event_period(event, ip))
            e_i += 1
        else:
            # No intersection, check if event is before/after filterevent
            if ep.end <= fp.start:
                # Event ended before filter event started
                e_i += 1
            elif fp.end <= ep.start:
                # Event started after filter event ended
                f_i += 1
            else:
                logger.warning("Unclear if/how this could be reachable, skipping period")
                e_i += 1
                f_i += 1

    return filtered_events
