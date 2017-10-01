import logging

from aw_core.models import Event
from aw_datastore import Datastore

from .transforms import filter_period_intersect, filter_keyvals, merge_events_by_keys, sort_by_timestamp, sort_by_duration

class QueryFunctionException(Exception):
    pass

# TODO: proper type checking

def q2_query_bucket(datastore: Datastore, namespace: dict, bucketname: str):
    if type(bucketname) != str:
        raise QueryFunctionException("Invalid argument to query_bucket")
    return datastore[bucketname].get()

def q2_filter_keyvals(datastore: Datastore, namespace: dict, events: list, key: str, vals: list, exclude: bool):
    # TODO: Implement
    raise NotImplementedError

def q2_filter_period_intersect(datastore: Datastore, namespace: dict, events: list, filterevents: list):
    return filter_period_intersect(events, filterevents)

def q2_merge_events_by_key(datastore: Datastore, namespace: dict, events: list, key1: str):
    return merge_events_by_keys(events, [key1])

def q2_merge_events_by_keys2(datastore: Datastore, namespace: dict, events: list, key1: str, key2: str):
    return merge_events_by_keys(events, [key1, key2])

def q2_merge_events_by_keys3(datastore: Datastore, namespace: dict, events: list, key1: str, key2: str, key3: str):
    return merge_events_by_keys(events, [key1, key2, key3])

def q2_sort_by_timestamp(datastore: Datastore, namespace: dict, events: list):
    return sort_by_timestamp(events)

def q2_sort_by_duration(datastore: Datastore, namespace: dict, events: list):
    return sort_by_duration(events)

def q2_nop(datastore: Datastore, namespace: dict):
    """
    No operation function for unittesting
    """
    return 1

query2_functions = {
    "filter_period_intersect": q2_filter_period_intersect,
    "filter_keyvals": q2_filter_keyvals,
    "query_bucket": q2_query_bucket,
    "merge_events_by_key": q2_merge_events_by_key,
    "merge_events_by_keys2": q2_merge_events_by_keys2,
    "merge_events_by_keys3": q2_merge_events_by_keys3,
    "sort_by_timestamp": q2_sort_by_timestamp,
    "sort_by_duration": q2_sort_by_duration,
    "nop": q2_nop,
}
