import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from copy import copy, deepcopy

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
        # print("Passed labels check")

        # Diff between timestamps in seconds, takes into account the duration of the last event
        ts_diff_seconds = (heartbeat.timestamp - last_event.timestamp).total_seconds()
        last_duration_seconds = last_event.duration.total_seconds() if last_event.duration else 0

        if ts_diff_seconds < pulsetime + last_duration_seconds:
            # print("Passed ts_diff check")
            last_event.duration = timedelta(seconds=ts_diff_seconds)
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
            if ep.end < fp.start:
                # Event ended before filter event started
                e_i += 1
            elif fp.end < ep.start:
                # Event started after filter event ended
                f_i += 1
            else:
                raise Exception("Should be unreachable")

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


class ChunkResult(dict):
    def __init__(self, chunks: dict, duration: timedelta, eventcount: int) -> None:
        dict.__init__(self)
        self["chunks"] = chunks
        self["duration"] = duration
        self["eventcount"] = eventcount

    @property
    def chunks(self) -> dict:
        return self["chunks"]

    @property
    def duration(self) -> timedelta:
        return self["duration"]

    @property
    def eventcount(self) -> int:
        return self["eventcount"]


def full_chunk(events: List[Event], chunk_key: str) -> ChunkResult:
    from collections import defaultdict
    eventcount = 0
    chunks = defaultdict(lambda: {
        "data": {},
        "duration": timedelta(0)
    })  # type: Dict[str, dict]
    totduration = timedelta()
    for event in events:
        eventcount += 1
        totduration += event.duration
        if chunk_key in event.data:
            chunks[event.data[chunk_key]]["duration"] += event.duration

            for k, v in event.data.items():
                if k != chunk_key:
                    if k not in chunks[event.data[chunk_key]]["data"]:
                        kv_info = {"values": {}}  # type: dict
                        kv_info["duration"] = copy(event.duration)
                        chunks[event.data[chunk_key]]["data"][k] = kv_info
                    else:
                        chunks[event.data[chunk_key]]["data"][k]["duration"] += event.duration
                    if v not in chunks[event.data[chunk_key]]["data"][k]["values"]:
                        chunks[event.data[chunk_key]]["data"][k]["values"][v] = {}
                        chunks[event.data[chunk_key]]["data"][k]["values"][v]["duration"] = copy(event.duration)
                    else:
                        chunks[event.data[chunk_key]]["data"][k]["values"][v]["duration"] += event.duration
    # Package response
    return ChunkResult(chunks, totduration, eventcount)


def merge_chunks(chunk1, chunk2):
    result = {}
    for label in set(chunk1.keys()).union(set(chunk2.keys())):
        if label in chunk1 and label in chunk2:
            result[label] = {"data":{}}
            c1kv = chunk1[label]["data"]
            c2kv = chunk2[label]["data"]
            for key in set(c1kv.keys()).union(set(c2kv.keys())):
                if key in c1kv and key in c2kv:
                    result[label]["data"][key] = {"values":{}}
                    c1k = c1kv[key]["values"]
                    c2k = c2kv[key]["values"]
                    for val in set(c1k.keys()).union(set(c2k.keys())):
                        if val in c1k and val in c2k:
                            c1v = c1k[val]
                            c2v = c2k[val]
                            result[label]["data"][key]["values"][val] = {
                                "duration":
                                    c1v["duration"] +
                                    c2v["duration"]
                            }
                        elif val in c1v:
                            result[label]["data"][key]["values"][val] = c1k[val]
                        elif val in c2v:
                            result[label]["data"][key]["values"][val] = c2k[val]
                elif key in c1kv:
                    result[label]["data"][key] = c1kv[key]
                elif key in c2kv:
                    result[label]["data"][key] = c2kv[key]
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
    d2 = q2["duration"]
    result["duration"] = d1 + d2
    # Data (eventlist/chunks)
    if "chunks" in q1 and "chunks" in q2:
        result["chunks"] = merge_chunks(q1["chunks"], q2["chunks"])
    if "eventlist" in q1 and "eventlist" in q2:
        result["eventlist"] = q1["eventlist"] + q2["eventlist"]
    return result
