import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from copy import copy, deepcopy
import operator
from functools import reduce
from collections import defaultdict

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


def heartbeat_reduce(events: Event, pulsetime: float) -> List[Event]:
    """Merges consecutive events together according to the rules of `heartbeat_merge`."""
    reduced = []
    if len(events) > 0:
        reduced.append(events.pop(0))
    for heartbeat in events:
        merged = heartbeat_merge(reduced[-1], heartbeat, pulsetime)
        if merged is not None:
            # Heartbeat was merged
            reduced[-1] = merged
        else:
            # Heartbeat was not merged
            reduced.append(heartbeat)
    return reduced


def heartbeat_merge(last_event: Event, heartbeat: Event, pulsetime: float) -> Optional[Event]:
    """Merges two events together if they have identical labels and are separated by a time smaller than pulsetime."""
    if last_event.data == heartbeat.data:
        last_event_end = last_event.timestamp + last_event.duration
        pulsewindow_end = last_event_end + timedelta(seconds=pulsetime)

        if heartbeat.timestamp <= pulsewindow_end:
            # Heartbeat was within pulsetime window, set duration of last event appropriately
            last_event.duration = heartbeat.timestamp - last_event.timestamp
            return last_event

    return None


def filter_period_intersect(events: List[Event], filterevents: List[Event]) -> List[Event]:
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


def filter_keyvals(events, key, vals, exclude=False) -> List[Event]:
    def predicate(event):
        for val in vals:
            if key in event.data and val == event.data[key]:
                return True
        return False

    if exclude:
        return list(filter(lambda e: not predicate(e), events))
    else:
        return list(filter(lambda e: predicate(e), events))


def merge_events_by_keys(events, keys):
    # The result will be a list of events without timestamp since they are merged
    # Call recursively until all keys are consumed
    if len(keys) < 1:
        return events
    merged_events = {}
    for event in events:
        summed_key = ""
        for key in keys:
            if key in event.data:
                summed_key = summed_key + "." + event["data"][key]
        if summed_key not in merged_events:
            merged_events[summed_key] = deepcopy(event)
            merged_events[summed_key].data = {}
            for key in keys:
                if key in event.data:
                    merged_events[summed_key].data[key] = event.data[key]
        else:
            merged_events[summed_key].duration += event.duration
    result = []
    for key in merged_events:
        result.append(Event(**merged_events[key]))
    return result

def sort_by_timestamp(events):
    return sorted(events, key=lambda e: e.timestamp)

def sort_by_duration(events):
    return sorted(events, key=lambda e: e.duration, reverse=True)

def limit_events(events, count):
    return events[:count]

"""
    Watcher specific transforms
"""

def split_url_events(events):
    for event in events:
        if "url" in event.data:
            url = event.data["url"]
            protocol_end = url.find('://')
            #print("Protocol: 0->{}".format(protocol_end))
            domain_start = protocol_end+3
            domain_end = domain_start+url[domain_start:].find('/')
            #print("Domain: {}->{}".format(domain_start, domain_end))
            path_start = domain_end+1
            path_end = path_start+url[path_start:].find('?')
            if path_end < path_start:
                path_end = len(url)
            #print("Path: {}->{}".format(path_start, path_end))
            event.data["protocol"] = url[:protocol_end]
            event.data["domain"] = url[domain_start:domain_end]
            event.data["path"] = url[domain_end:path_end]
            event.data["options"] = url[path_end+1:]
    return events
