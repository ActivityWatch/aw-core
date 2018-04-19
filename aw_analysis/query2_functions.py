import iso8601
from typing import Callable, Dict, Any, List

from aw_core.models import Event
from aw_datastore import Datastore

from aw_transform import filter_period_intersect, filter_keyvals, filter_keyvals_regex, merge_events_by_keys, sort_by_timestamp, sort_by_duration, limit_events, split_url_events, simplify_string

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
    """Decorator used to register query functions"""
    fname = f.__name__
    if fname[:3] == "q2_":
        fname = fname[3:]
    query2_functions[fname] = f
    return f


"""
Getting buckets
"""


@q2_function
def q2_find_bucket(datastore: Datastore, namespace: TNamespace, filter_str: str, hostname: str = None):
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
        raise QueryFunctionException("Unable to parse starttime/endtime to query_bucket_date")
    return datastore[bucketname].get(starttime=starttime, endtime=endtime)

def q2_query_bucket_period(datastore: Datastore, namespace: dict, bucketname: str, starttime_str: str, endtime_str: str):
    # Uses hardcoded timeperiods instead of the STARTTIME/ENDTIME variables
    _verify_bucket_exists(datastore, bucketname)
    try:
        starttime = iso8601.parse_date(starttime_str)
        endtime = iso8601.parse_date(endtime_str)
    except iso8601.ParseError:
        raise QueryFunctionException("Unable to parse starttime/endtime to query_bucket_date")
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
def q2_filter_keyvals(datastore: Datastore, namespace: TNamespace, events: list, key: str, vals: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    _verify_variable_is_type(vals, list)
    return filter_keyvals(events, key, vals, False)


@q2_function
def q2_exclude_keyvals(datastore: Datastore, namespace: dict, events: list, key: str, vals: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    _verify_variable_is_type(vals, list)
    return filter_keyvals(events, key, vals, True)


@q2_function
def q2_filter_keyvals_regex(datastore: Datastore, namespace: TNamespace, events: list, key: str, regex: str) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    return filter_keyvals_regex(events, key, regex)


@q2_function
def q2_filter_period_intersect(datastore: Datastore, namespace: TNamespace, events: list, filterevents: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(filterevents, list)
    return filter_period_intersect(events, filterevents)


@q2_function
def q2_limit_events(datastore: Datastore, namespace: TNamespace, events: list, count: int) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(count, int)
    return events[:count]


"""
    Merge functions
"""


@q2_function
def q2_merge_events_by_keys(datastore: Datastore, namespace: dict, events: list, keys: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(keys, list)
    return merge_events_by_keys(events, keys)


"""
    Sort functions
"""


@q2_function
def q2_sort_by_timestamp(datastore: Datastore, namespace: TNamespace, events: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    return sort_by_timestamp(events)


@q2_function
def q2_sort_by_duration(datastore: Datastore, namespace: TNamespace, events: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    return sort_by_duration(events)


"""
    Watcher specific functions
"""


@q2_function
def q2_split_url_events(datastore: Datastore, namespace: TNamespace, events: list) -> List[Event]:
    _verify_variable_is_type(events, list)
    return split_url_events(events)


@q2_function
def q2_simplify_window_titles(datastore: Datastore, namespace: TNamespace, events: list, key: str) -> List[Event]:
    _verify_variable_is_type(events, list)
    _verify_variable_is_type(key, str)
    return simplify_string(events, key=key)


"""
    Test functions
"""


@q2_function
def q2_nop(datastore: Datastore, namespace: TNamespace):
    """
    No operation function for unittesting
    """
    return 1
