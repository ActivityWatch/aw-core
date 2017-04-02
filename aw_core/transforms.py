import logging
import json
from datetime import datetime, timedelta
from typing import List, Any, Dict, Optional
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


def heartbeat_reduce(events: Event, pulsetime: float) -> List[Event]:
    """Merges consequtive events together according to the rules of `heartbeat_merge`."""
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
    if last_event.label == heartbeat.label \
       and last_event.keyvals == heartbeat.keyvals \
       and last_event.count == heartbeat.count:
        # print("Passed labels check")

        # Diff between timestamps in seconds, takes into account the duration of the last event
        ts_diff_seconds = (heartbeat.timestamp - last_event.timestamp).total_seconds()
        last_duration_seconds = last_event.duration.total_seconds() if last_event.duration else 0

        if ts_diff_seconds < pulsetime + last_duration_seconds:
            # print("Passed ts_diff check")
            last_event.duration = timedelta(seconds=ts_diff_seconds)
            return last_event

    return None


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

def include_labels(events, labels):
    filtered_events = []
    for event in events:
        match = False
        for label in labels:
            if label in event["label"]:
                match = True
        if match:
            filtered_events.append(event)
    return filtered_events

def exclude_labels(events, labels):
    filtered_events = []
    for event in events:
        match = False
        for label in labels:
            if label in event["label"]:
                match = True
        if not match:
            filtered_events.append(event)
    return filtered_events


def full_chunk(events: List[Event]) -> dict:
    eventcount = 0
    chunks = dict()  # type: Dict[str, Any]
    totduration = timedelta();
    for event in events:
        eventcount += 1
        if event.duration:
            totduration += event.duration
        if event.label:
            if event.label not in chunks:
                chunks[event.label] = {"keyvals": {}}
                if event.duration:
                    chunks[event.label]["duration"] = copy(event.duration)
            else:
                if event.duration:
                    if "duration" not in chunks[event.label]:
                        chunks[event.label]["duration"] = copy(event.duration)
                    else:
                        chunks[event.label]["duration"] += event.duration
            for k, v in event.keyvals.items():
                if k not in chunks[event.label]["keyvals"]:
                    kv_info = {"values": {}} # type: dict
                    if event.duration:
                        kv_info["duration"] = copy(event.duration)
                    chunks[event.label]["keyvals"][k] = kv_info
                else:
                    if event.duration:
                        if "duration" not in chunks[event.label]["keyvals"][k]:
                            chunks[event.label]["keyvals"][k]["duration"] = copy(event.duration)
                        else:
                            chunks[event.label]["keyvals"][k]["duration"] += event.duration
                if v not in chunks[event.label]["keyvals"][k]["values"]:
                    chunks[event.label]["keyvals"][k]["values"][v] = {}
                    if event.duration:
                        chunks[event.label]["keyvals"][k]["values"][v]["duration"] = event.duration
                else:
                    if event.duration:
                        if "duration" not in chunks[event.label]["keyvals"][k]["values"][v]:
                            chunks[event.label]["keyvals"][k]["values"][v]["duration"] = event.duration
                        else:
                            chunks[event.label]["keyvals"][k]["values"][v]["duration"] += event.duration
    # Package response
    payload = {
        "eventcount": eventcount,
        "duration": totduration,
        "chunks": chunks,
    }
    return payload


def label_chunk(events: List[Event]) -> dict:
    eventcount = 0
    chunks = dict()  # type: Dict[str, Any]
    totduration = timedelta();
    for event in events:
        eventcount += 1
        if event.duration:
            totduration += event.duration
        if event.label:
            if event.label not in chunks:
                chunks[event.label] = {}
                if event.duration:
                    chunks[event.label]["duration"] = copy(event.duration)
            else:
                if event.duration:
                    if "duration" not in chunks[event.label]:
                        chunks[event.label]["duration"] = copy(event.duration)
                    else:
                        chunks[event.label]["duration"] += event.duration
    # Package response
    payload = {
        "eventcount": eventcount,
        "duration": totduration,
        "chunks": chunks,
    }
    return payload


def merge_chunks(chunk1, chunk2):
    result = {}
    for label in set(chunk1.keys()).union(set(chunk2.keys())):
        if label in chunk1 and label in chunk2:
            result[label] = {"keyvals":{}}
            c1kv = chunk1[label]["keyvals"]
            c2kv = chunk2[label]["keyvals"]
            for key in set(c1kv.keys()).union(set(c2kv.keys())):
                if key in c1kv and key in c2kv:
                    result[label]["keyvals"][key] = {"values":{}}
                    c1k = c1kv[key]["values"]
                    c2k = c2kv[key]["values"]
                    for val in set(c1k.keys()).union(set(c2k.keys())):
                        if val in c1k and val in c2k:
                            c1v = c1k[val]
                            c2v = c2k[val]
                            result[label]["keyvals"][key]["values"][val] = {
                                "duration":
                                    c1v["duration"] +
                                    c2v["duration"]
                            }
                        elif val in c1v:
                            result[label]["keyvals"][key]["values"][val] = c1k[val]
                        elif val in c2v:
                            result[label]["keyvals"][key]["values"][val] = c2k[val]
                elif key in c1kv:
                    result[label]["keyvals"][key] = c1kv[key]
                elif key in c2kv:
                    result[label]["keyvals"][key] = c2kv[key]
        elif label in chunk1:
            result[label] = chunk1[label]
        elif label in chunk2:
            result[label] = chunk2[label]


    return result


def merge_queries(q1, q2):
    result = {}
    # Eventcount
    result["eventcount"] = q1["eventcount"] + q2["eventcount"]
    # Duration
    d1 = q1["duration"]
    if type(d1) is dict:
        d1 = timedelta(seconds=d1["value"])
    d2 = q2["duration"]
    if type(d2) is dict:
        d2 = timedelta(seconds=d2["value"])
    result["duration"] = d1 + d2
    # Data (eventlist/chunks)
    if "chunks" in q1 and "chunks" in q2:
        result["chunks"] = merge_chunks(q1["chunks"], q2["chunks"])
    if "eventlist" in q1 and "eventlist" in q2:
        result["eventlist"] = q1["eventlist"] + q2["eventlist"]
    return result


