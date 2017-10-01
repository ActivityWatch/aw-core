import logging

from aw_core.models import Event
from aw_datastore import Datastore

from .transforms import filter_period_intersect, filter_keyvals

class QueryFunctionException(Exception):
    pass

# TODO: proper type checking

def q2_query_bucket(datastore: Datastore, namespace: dict, bucketname: str):
    if type(bucketname) != str:
        raise QueryFunctionException("Invalid argument to query_bucket")
    return datastore[bucketname].get()

def q2_filter_keyvals(datastore: Datastore, namespace: dict, events: list, key: str, vals: list, exclude: bool):
    # TODO: Implement
    pass

def q2_filter_period_intersect(datastore: Datastore, namespace: dict, events: list, filterevents: list):
    if type(events) != list:
        logging.debug(events)
        raise QueryFunctionException("Invalid argument for filter_period_intersect")
    if type(filterevents) != list:
        logging.debug(filterevents)
        raise QueryFunctionException("Invalid argument for filter_period_intersect")
    return filter_period_intersect(events, filterevents)


query2_functions = {
    "filter_period_intersect": q2_filter_period_intersect,
    "filter_keyvals": q2_filter_keyvals,
    "query_bucket": q2_query_bucket,
}
