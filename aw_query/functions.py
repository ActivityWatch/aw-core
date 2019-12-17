import re
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
    categorize,
    assign_productivity,
    tag,
    Rule,
    merge_events_by_keys,
    chunk_events_by_key,
    sort_by_timestamp,
    sort_by_duration,
    sum_durations,
    concat,
    split_url_events,
    simplify_string,
    flood,
    limit_events,
)

from .exceptions import QueryFunctionException


def _verify_bucket_exists(datastore, bucketname):
    if bucketname in datastore.buckets():
        return
    else:
        raise QueryFunctionException("There's no bucket named '{}'".format(bucketname))


def _verify_variable_is_type(variable, t):
    if not isinstance(variable, t):
        raise QueryFunctionException("Variable '{}' passed to function call is of invalid type. Expected {} but was {}".format(variable, t, type(variable)))

# TODO: proper type checking (typecheck-decorator in pypi?)


TNamespace = Dict[str, Any]
TQueryFunction = Callable[..., Any]


"""
    Declarations
"""
functions: Dict[str, TQueryFunction] = {}


def q2_function(transform_func=None):
    """
    Decorator used to register query functions.

    Automatically adds mock arguments for Datastore and TNamespace
    if not in function signature.
    """

    def h(f):
        sig = signature(f)
        # If function lacks docstring, use docstring from underlying function in aw_transform
        if transform_func and transform_func.__doc__ and not f.__doc__:
            f.__doc__ = ".. note:: Documentation automatically copied from underlying function `aw_transform.{func_name}`\n\n{func_doc}".format(func_name=transform_func.__name__, func_doc=transform_func.__doc__)

        @wraps(f)
        def g(datastore: Datastore, namespace: TNamespace, *args, **kwargs):
            # Remove datastore and namespace argument for functions that don't need it
            args = (datastore, namespace, *args)
            if TNamespace not in (sig.parameters[p].annotation for p in sig.parameters):
                args = (args[0], *args[2:])
            if Datastore not in (sig.parameters[p].annotation for p in sig.parameters):
                args = (args[1:])
            return f(*args, **kwargs)

        fname = f.__name__
        if fname[:3] == "q2_":
            fname = fname[3:]
        functions[fname] = g
        return g

    return h


def q2_typecheck(f):
    """Decorator that typechecks using `_verify_variable_is_type`"""
    sig = signature(f)

    @wraps(f)
    def g(*args, **kwargs):
        # FIXME: If the first argument passed to a query2 function is a straight [] then the second argument disappears from the argument list for unknown reasons, which breaks things
        for i, p in enumerate(sig.parameters):
            param = sig.parameters[p]

            # print(f"Checking that param ({param}) was {param.annotation}, value: {args[i]}")
            # FIXME: Won't check keyword arguments
            if param.annotation in [list, str, int, float] and param.default == param.empty:
                _verify_variable_is_type(args[i], param.annotation)

        return f(*args, **kwargs)
    return g


"""
    Getting buckets
"""


@q2_function()
@q2_typecheck
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


@q2_function()
@q2_typecheck
def q2_query_bucket(datastore: Datastore, namespace: TNamespace, bucketname: str) -> List[Event]:
    _verify_bucket_exists(datastore, bucketname)
    try:
        starttime = iso8601.parse_date(namespace["STARTTIME"])
        endtime = iso8601.parse_date(namespace["ENDTIME"])
    except iso8601.ParseError:
        raise QueryFunctionException("Unable to parse starttime/endtime for query_bucket")
    return datastore[bucketname].get(starttime=starttime, endtime=endtime)


@q2_function()
@q2_typecheck
def q2_query_bucket_eventcount(datastore: Datastore, namespace: TNamespace, bucketname: str) -> int:
    _verify_bucket_exists(datastore, bucketname)
    starttime = iso8601.parse_date(namespace["STARTTIME"])
    endtime = iso8601.parse_date(namespace["ENDTIME"])
    return datastore[bucketname].get_eventcount(starttime=starttime, endtime=endtime)


"""
    Filtering functions
"""


@q2_function(filter_keyvals)
@q2_typecheck
def q2_filter_keyvals(events: list, key: str, vals: list) -> List[Event]:
    return filter_keyvals(events, key, vals, False)


@q2_function(filter_keyvals)
@q2_typecheck
def q2_exclude_keyvals(events: list, key: str, vals: list) -> List[Event]:
    return filter_keyvals(events, key, vals, True)


@q2_function(filter_keyvals_regex)
@q2_typecheck
def q2_filter_keyvals_regex(events: list, key: str, regex: str) -> List[Event]:
    return filter_keyvals_regex(events, key, regex)


@q2_function(filter_period_intersect)
@q2_typecheck
def q2_filter_period_intersect(events: list, filterevents: list) -> List[Event]:
    return filter_period_intersect(events, filterevents)


@q2_function(period_union)
@q2_typecheck
def q2_period_union(events1: list, events2: list) -> List[Event]:
    return period_union(events1, events2)


@q2_function(limit_events)
@q2_typecheck
def q2_limit_events(events: list, count: int) -> List[Event]:
    return limit_events(events, count)


"""
    Merge functions
"""


@q2_function(merge_events_by_keys)
@q2_typecheck
def q2_merge_events_by_keys(events: list, keys: list) -> List[Event]:
    return merge_events_by_keys(events, keys)


@q2_function(chunk_events_by_key)
@q2_typecheck
def q2_chunk_events_by_key(events: list, key: str) -> List[Event]:
    return chunk_events_by_key(events, key)


"""
    Sort functions
"""


@q2_function(sort_by_timestamp)
@q2_typecheck
def q2_sort_by_timestamp(events: list) -> List[Event]:
    return sort_by_timestamp(events)


@q2_function(sort_by_duration)
@q2_typecheck
def q2_sort_by_duration(events: list) -> List[Event]:
    return sort_by_duration(events)


"""
    Summarizing functions
"""


@q2_function(sum_durations)
@q2_typecheck
def q2_sum_durations(events: list) -> timedelta:
    return sum_durations(events)


@q2_function(concat)
@q2_typecheck
def q2_concat(events1: list, events2: list) -> List[Event]:
    return concat(events1, events2)


"""
    Flood functions
"""


@q2_function(flood)
@q2_typecheck
def q2_flood(events: list) -> List[Event]:
    return flood(events)


"""
    Watcher specific functions
"""


@q2_function(split_url_events)
@q2_typecheck
def q2_split_url_events(events: list) -> List[Event]:
    return split_url_events(events)


@q2_function(simplify_string)
@q2_typecheck
def q2_simplify_window_titles(events: list, key: str) -> List[Event]:
    return simplify_string(events, key=key)


"""
    Test functions
"""


@q2_function()
@q2_typecheck
def q2_nop():
    """No operation function for unittesting"""
    return 1


"""
    Classify
"""


@q2_function(categorize)
@q2_typecheck
def q2_categorize(events: list, classes: list):
    classes = [(_cls, Rule(rule_dict)) for _cls, rule_dict in classes]
    return categorize(events, classes)


@q2_function(tag)
@q2_typecheck
def q2_tag(events: list, classes: list):
    classes = [(_cls, Rule(rule_dict)) for _cls, rule_dict in classes]
    return tag(events, classes)

@q2_function(assign_productivity)
@q2_typecheck
def q2_assign_productivity(events:list, classes: list):
    classes = [(_cls, productivity_score) for _cls, productivity_score in classes]
    return assign_productivity(events, classes)
