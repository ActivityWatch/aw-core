import iso8601
from typing import Callable, Dict, Any, List
from inspect import signature
from functools import wraps
from datetime import timedelta

from aw_core.models import Event
from aw_datastore import Datastore

from aw_transform import (
    filter_period_intersect,
    filter_keyvals,
    filter_keyvals_regex,
    period_union,
    merge_events_by_keys,
    chunk_events_by_key,
    sort_by_timestamp,
    sort_by_duration,
    sum_durations,
    sum_event_lists,
    split_url_events,
    simplify_string,
    flood
)

from .query2_error import QueryFunctionException


def _verify_bucket_exists(datastore, bucketname):
    if bucketname in datastore.buckets():
        return
    else:
        raise QueryFunctionException("There's no bucket named '{}'".format(bucketname))


def _verify_variable_is_type(variable, t):
    if type(variable) != t:
        raise QueryFunctionException("Variable '{}' passed to function call is of invalid type".format(variable))

# TODO: proper type checking (typecheck-decorator in pypi?)


TNamespace = Dict[str, Any]
TQueryFunction = Callable[..., Any]


"""
    Declarations
"""
query2_functions = {}  # type: Dict[str, TQueryFunction]


def q2_function(f):
    """
    Decorator used to register query functions.

    Automatically adds mock arguments for Datastore and TNamespace
    if not in function signature.
    """
    sig = signature(f)

    @wraps(f)
    def g(datastore: Datastore, namespace: TNamespace, *args, **kwargs):
        args = (datastore, namespace, *args)
        if TNamespace not in (sig.parameters[p].annotation for p in sig.parameters):
            args = (args[0], *args[2:])
        if Datastore not in (sig.parameters[p].annotation for p in sig.parameters):
            args = (args[1:])
        return f(*args, **kwargs)

    fname = f.__name__
    if fname[:3] == "q2_":
        fname = fname[3:]
    query2_functions[fname] = g
    return g


"""
    Getting buckets
"""


@q2_function
def q2_find_bucket(datastore: Datastore, filter_str: str, hostname: str = None):
    """Find bucket by using a filter_str (to avoid hardcoding bucket names)"""
    for bucket in datastore.buckets():
        if filter_str in bucket:
            bucket_metadata = datastore[bucket].metadata()
            if hostname:
                if bucket_metadata["hostname"] == hostname:
                    return bucket
            else:
                return bucket
    raise QueryFunctionException("Unable to find bucket matching '{}' (hostname filter set to '{}')".format(filter_str, hostname))


"""
    Data gathering functions
"""


@q2_function
def q2_query_bucket(datastore: Datastore, namespace: TNamespace, bucketname: str) -> List[Event]:
    _verify_variable_is_type(bucketname, str)
    _verify_bucket_exists(datastore, bucketname)
    try:
        starttime = iso8601.parse_date(namespace["STARTTIME"])
        endtime = iso8601.parse_date(namespace["ENDTIME"])
    except iso8601.ParseError:
        raise QueryFunctionException("Unable to parse starttime/endtime for query_bucket")
    return datastore[bucketname].get(starttime=starttime, endtime=endtime)


@q2_function
def q2_query_bucket_eventcount(datastore: Datastore, namespace: TNamespace, bucketname: str) -> int:
    _verify_variable_is_type(bucketname, str)
    _verify_bucket_exists(datastore, bucketname)
    starttime = iso8601.parse_date(namespace["STARTTIME"])
    endtime = iso8601.parse_date(namespace["ENDTIME"])
    return datastore[bucketname].get_eventcount(starttime=starttime, endtime=endtime)


"""
    Filtering functions
"""


@q2_function
def q2_filter_keyvals(events: list, key: str, vals: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    _verify_variable_is_type(vals, list)
    return filter_keyvals(events, key, vals, False)


@q2_function
def q2_exclude_keyvals(events: list, key: str, vals: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    _verify_variable_is_type(vals, list)
    return filter_keyvals(events, key, vals, True)


@q2_function
def q2_filter_keyvals_regex(events: list, key: str, regex: str) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    return filter_keyvals_regex(events, key, regex)


@q2_function
def q2_filter_period_intersect(events: list, filterevents: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(filterevents, list)
    return filter_period_intersect(events, filterevents)


@q2_function
def q2_period_union(events1: list, events2: list) -> List[Event]:
    _verify_variable_is_type(events1, list)
    _verify_variable_is_type(events2, list)
    return period_union(events1, events2)


@q2_function
def q2_limit_events(events: list, count: int) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(count, int)
    return events[:count]


"""
    Merge functions
"""


@q2_function
def q2_merge_events_by_keys(events: list, keys: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(keys, list)
    return merge_events_by_keys(events, keys)


@q2_function
def q2_chunk_events_by_key(events: list, key: str) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    return chunk_events_by_key(events, key)


"""
    Sort functions
"""


@q2_function
def q2_sort_by_timestamp(events: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    return sort_by_timestamp(events)


@q2_function
def q2_sort_by_duration(events: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    return sort_by_duration(events)


"""
    Summarizing functions
"""


@q2_function
def q2_sum_durations(events: list) -> timedelta:
    _verify_variable_is_type(events, list)
    return sum_durations(events)

@q2_function
def q2_sum_event_lists(events1: list, events2: list) -> List[Event]:
    _verify_variable_is_type(events1, list)
    _verify_variable_is_type(events2, list)
    return sum_event_lists(events1, events2)


"""
    Flood functions
"""


@q2_function
def q2_flood(events: list) -> List[Event]:
    return flood(events)


"""
    Watcher specific functions
"""


@q2_function
def q2_split_url_events(events: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    return split_url_events(events)


@q2_function
def q2_simplify_window_titles(events: list, key: str) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    return simplify_string(events, key=key)


"""
    Test functions
"""


@q2_function
def q2_nop():
    """No operation function for unittesting"""
    return 1
