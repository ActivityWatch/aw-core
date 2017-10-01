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
    """
    Merges two events if they have identical labels and are
    separated by a time smaller than :code:`pulsetime` seconds.
    """
    if last_event.data == heartbeat.data:
        gap = heartbeat.timestamp - (last_event.timestamp + last_event.duration)

        if gap <= timedelta(seconds=pulsetime):
            # Heartbeat was within pulsetime window, set duration of last event appropriately
            last_event.duration = (heartbeat.timestamp - last_event.timestamp) + heartbeat.duration
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


def full_chunk(events: List[Event], chunk_key: str, include_subchunks=True) -> ChunkResult:
    """
    Takes a list of events, returns them chunked by key.

    Example Input:
        full_chunk([
            {ts: now,       duration: 120, data: {"app": "chrome", "title": "ActivityWatch"}},
            {ts: now + 120, duration: 120, data: {"app": "chrome", "title": "GitHub"}},
        ], "app")

    Example Output:
        {"chrome": {
           "duration": 240,
           "data": {"title": {
              "values": {
                 "ActivityWatch": { "duration": 120 },
                 "GitHub": { "duration": 120 },
              }
           }}
        }}
    """
    # TODO: Rename the function to just `chunk`.
    # TODO: Rename the "data" key to the more descriptive "subchunks".
    # TODO: Move everything in ["data"][key]["values"] to just ["data"][key]
    #       This makes the dict expressable as a recursive datatype.
    #       Approximate Haskell equivalent:
    #           data Chunk = Chunk String Duration [Chunk]
    # New proposed output:
    #   {"chrome": {
    #      "duration": 240,
    #      "subchunks": {"title": {
    #         "ActivityWatch": { duration: 120 },
    #         "GitHub": { duration: 120 },
    #      }}
    #   }}

    if include_subchunks:
        def default_dict_constructor():
            return {"duration": timedelta(0),
                    "data": defaultdict(lambda: {
                        "values": defaultdict(lambda: defaultdict(lambda: timedelta(0))),
                        "duration": timedelta(0)
                    })}
    else:
        def default_dict_constructor():
            return {"duration": timedelta(0)}

    chunks = defaultdict(default_dict_constructor)  # type: Dict[str, dict]

    for event in filter(lambda e: chunk_key in list(e.data.keys()), events):
        chunk = chunks[event.data[chunk_key]]
        chunk["duration"] += event.duration

        if include_subchunks:
            # Merge all the data keys that are not chunk_key
            for k, v in event.data.items():
                if k != chunk_key:
                    chunk["data"][k]["values"][v]["duration"] += event.duration
                    # TODO: This probably lacks any meaningful uses, should be removed for simplicity
                    #       Would also make the datastructure simpler to express recursively.
                    chunk["data"][k]["duration"] += event.duration

    # Convert all the defaultdicts into normal dicts
    chunks = dict(chunks)
    if include_subchunks:
        for chunk in chunks.values():
            chunk["data"] = dict(chunk["data"])
            for subchunk in chunk["data"].values():
                subchunk["values"] = dict(subchunk["values"])

    # Package response
    total_duration = reduce(operator.add, (event.duration for event in events), timedelta(0))
    return ChunkResult(chunks, total_duration, len(events))


def merge_chunks(chunk1: ChunkResult, chunk2: ChunkResult):
    """What exactly is chunk1 and chunk2?"""
    result = defaultdict(lambda: {"data": {}})  # type: Dict[str, dict]

    keys_intersection = set(chunk1.keys()).intersection(set(chunk2.keys()))
    for key in keys_intersection:
        c1_subchunks = chunk1[key]["data"]
        c2_subchunks = chunk2[key]["data"]
        subchunk_keys_union = set(c1_subchunks.keys()).union(set(c2_subchunks.keys()))

        subchunks = result[key]["data"]
        for subkey in subchunk_keys_union:
            result[key]["data"][subkey] = {"values": {}}

            if subkey in c1_subchunks and subkey in c2_subchunks:
                c1_subchunk_values = c1_subchunks[subkey]["values"]
                c2_subchunk_values = c2_subchunks[subkey]["values"]
                subchunk_values_union = set(c1_subchunk_values.keys()).union(set(c2_subchunk_values.keys()))

                subchunk_values = subchunks[subkey]["values"]
                for val in subchunk_values_union:
                    if val in c1_subchunk_values and val in c2_subchunk_values:
                        v1_duration = c1_subchunk_values[val]["duration"]
                        v2_duration = c2_subchunk_values[val]["duration"]
                        subchunk_values[val] = {
                            "duration": v1_duration + v2_duration
                        }
                    else:
                        source = c1_subchunk_values if val in c1_subchunk_values else c2_subchunk_values
                        subchunk_values[val] = source[val]
            else:
                source = c1_subchunks if subkey in c1_subchunks else c2_subchunks
                result[key]["data"][subkey] = source[subkey]

    keys_xor = set(chunk1.keys()).symmetric_difference(set(chunk2.keys()))
    for key in keys_xor:
        source_chunk = chunk1 if key in chunk1 else chunk2
        result[key] = source_chunk[key]

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
        print(summed_key)
        if summed_key not in merged_events:
            merged_events[summed_key] = deepcopy(event)
            merged_events[summed_key].data = {}
            for key in keys:
                if key in event.data:
                    merged_events[summed_key].data[key] = event.data[key]
        else:
            merged_events[summed_key].duration += event.duration
    print(merged_events)
    result = []
    for key in merged_events:
        result.append(merged_events[key])
    return result

def sort_by_timestamp(events):
    return sorted(events, key=lambda e: e.timestamp)

def sort_by_duration(events):
    return sorted(events, key=lambda e: e.duration)
