import logging
from datetime import datetime, timedelta
from typing import List, Any

from aw_core.models import Event
from aw_core import TimePeriod

logger = logging.getLogger("aw.core.transform")


def _get_event_period(event: Event) -> TimePeriod:
    # TODO: Better parsing of event duration
    start = event["timestamp"][0]
    end = start + timedelta(seconds=event["duration"][0]["value"])
    return TimePeriod(start, end)


def _replace_event_period(event: Event, period: TimePeriod) -> Event:
    e = event.copy()
    e["timestamp"] = [period.start]
    e["duration"] = [period.duration]
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

    events = sorted(events, key=lambda e: e["timestamp"][0])
    filterevents = sorted(filterevents, key=lambda e: e["timestamp"][0])
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
    chunk = dict()  # type: Dict[str, Any]
    for event in events:
        if "label" in event:
            eventcount += 1
            for label in event["label"]:
                if label not in chunk:
                    chunk[label] = {"other_labels": []}
                for co_label in event["label"]:
                    if co_label != label and co_label not in chunk[label]["other_labels"]:
                        chunk[label]["other_labels"].append(co_label)
                if "duration" in event:
                    if "duration" not in chunk[label]:
                        chunk[label]["duration"] = event["duration"][0].copy()
                    else:
                        chunk[label]["duration"]["value"] += event["duration"][0]["value"]
    payload = {
        "eventcount": eventcount,
        "chunks": chunk,
    }
    return payload
