import logging
import iso8601

from aw_core.models import Event
from aw_datastore import Datastore

from aw_transform import filter_period_intersect, filter_keyvals, merge_events_by_keys, sort_by_timestamp, sort_by_duration, limit_events, split_url_events

class QueryFunctionException(Exception):
    pass

def _verify_bucket_exists(datastore, bucketname):
    if bucketname in datastore.buckets():
        return
    else:
        raise QueryFunctionException("There's no bucket named '{}'".format(bucketname))

# TODO: proper type checking (typecheck-decorator in pypi?)

"""
    Data gathering functions
"""
def q2_query_bucket(datastore: Datastore, namespace: dict, bucketname: str):
    _verify_bucket_exists(datastore, bucketname)
    starttime = iso8601.parse_date(namespace["STARTTIME"])
    endtime = iso8601.parse_date(namespace["ENDTIME"])
    return datastore[bucketname].get(starttime=starttime, endtime=endtime)

def q2_query_bucket_eventcount(datastore: Datastore, namespace: dict, bucketname: str):
    _verify_bucket_exists(datastore, bucketname)
    starttime = iso8601.parse_date(namespace["STARTTIME"])
    endtime = iso8601.parse_date(namespace["ENDTIME"])
    return datastore[bucketname].get_eventcount(starttime=starttime, endtime=endtime)

"""
    Filtering functions
"""
def q2_filter_keyvals(datastore: Datastore, namespace: dict, events: list, key: str, *vals):
    return filter_keyvals(events, key, list(vals), False)

def q2_exclude_keyvals(datastore: Datastore, namespace: dict, events: list, key: str, *vals):
    return filter_keyvals(events, key, list(vals), True)

def q2_filter_period_intersect(datastore: Datastore, namespace: dict, events: list, filterevents: list):
    return filter_period_intersect(events, filterevents)

def q2_limit_events(datastore: Datastore, namespace: dict, events: list, count: int):
    return limit_events(events, count)


"""
    Merge functions
"""
def q2_merge_events_by_keys(datastore: Datastore, namespace: dict, events: list, *keys):
    return merge_events_by_keys(events, keys)

"""
    Sort functions
"""
def q2_sort_by_timestamp(datastore: Datastore, namespace: dict, events: list):
    return sort_by_timestamp(events)

def q2_sort_by_duration(datastore: Datastore, namespace: dict, events: list):
    return sort_by_duration(events)


"""
    Watcher specific functions
"""

def q2_split_url_events(datastore: Datastore, namespace: dict, events: list):
    return split_url_events(events)

"""
    Test functions
"""
def q2_nop(datastore: Datastore, namespace: dict):
    """
    No operation function for unittesting
    """
    return 1


"""
    Declarations
"""
query2_functions = {
    "filter_period_intersect": q2_filter_period_intersect,
    "filter_keyvals": q2_filter_keyvals,
    "exclude_keyvals": q2_exclude_keyvals,
    "query_bucket": q2_query_bucket,
    "query_bucket_eventcount": q2_query_bucket_eventcount,
    "limit_events": q2_limit_events,
    "merge_events_by_keys": q2_merge_events_by_keys,
    "sort_by_timestamp": q2_sort_by_timestamp,
    "sort_by_duration": q2_sort_by_duration,
    "split_url_events": q2_split_url_events,
    "nop": q2_nop,
}
