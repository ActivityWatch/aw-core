import logging
from datetime import datetime, timedelta
from typing import List, Any
from copy import copy, deepcopy

from aw_core.models import Event
from aw_core import TimePeriod

logger = logging.getLogger("aw.core.transform")


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


def filter_period_intersect(events, filterevents):
    """
    Filters away all events or time periods of events in which a
    filterevent does not have an intersecting time period.

    Useful for example when you want to filter away events or
    part of events during which a user was AFK.

    Example:
      windowevents_notafk = filter_period_intersect(windowevents, notafkevents)
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
            if ep.end < fp.start:
                # Event ended before filter event started
                e_i += 1
            elif fp.end < ep.start:
                # Event started after filter event ended
                f_i += 1
            else:
                raise Exception("Should be unreachable")

    return filtered_events


def chunk(events: List[Event]) -> dict:
    eventcount = 0
    chunks = dict()  # type: Dict[str, Any]
    for event in events:
        if "label" in event:
            eventcount += 1
            for label in event["label"]:
                if label not in chunks:
                    chunks[label] = {"other_labels": []}
                for co_label in event["label"]:
                    if co_label != label and co_label not in chunks[label]["other_labels"]:
                        chunks[label]["other_labels"].append(co_label)
                if event.duration:
                    if "duration" not in chunks[label]:
                        chunks[label]["duration"] = copy(event.duration)
                    else:
                        chunks[label]["duration"] += event.duration
    # Turn all timedeltas into duration-dicts
    for label in chunks:
        if "duration" in chunks[label] and isinstance(chunks[label]["duration"], timedelta):
            chunks[label]["duration"] = {"value": chunks[label]["duration"].total_seconds(), "unit": "s"}
    payload = {
        "eventcount": eventcount,
        "chunks": chunks,
    }
    return payload
