from typing import Union, List

from aw_datastore import Datastore

from .models import Event

from . import transforms

from datetime import datetime, timedelta


class QueryException(Exception):
    pass


class Query:
    def __init__(self, transforms: List[dict], chunk=False, cache=False) -> None:
        self.transforms = transforms
        self.chunk = chunk
        self.cache = cache


class BucketTransform:
    def __init__(self, bucket_id: str, filters: dict) -> None:
        self.bucket_id = bucket_id
        self.filters = filters


# TODO: Get rid of union here?
def bucket_transform(btransform: Union[BucketTransform, dict], ds: Datastore,
                     limit: int=-1, start: datetime=None, end: datetime=None):
    if isinstance(btransform, dict):
        # Run-time typechecks
        if "bucket" not in btransform:
            raise QueryException("No bucket specified in transform: {}".format(btransform))
        if not isinstance(btransform["bucket"], str):
            raise QueryException("Invalid bucket name in transform: '{}'".format(btransform["bucket"]))

        # Fetch the bucket, ensure the bucket exists
        bucket_id = btransform["bucket"]  # type: str
        if not btransform["bucket"] in ds.buckets():
            raise QueryException("Cannot query bucket that doesn't exist in transform: '{}'".format(btransform["bucket"]))

        btransform = BucketTransform(bucket_id, filters=btransform["filters"] if "filters" in btransform else [])

    # Get events
    events = ds[btransform.bucket_id].get(limit, start, end)

    # Apply filters
    for vfilter in btransform.filters:
        if "name" not in vfilter:
            raise QueryException("No filter specified in transform: {}".format(vfilter))
        filtername = vfilter["name"]
        if not isinstance(filtername, str):
            raise QueryException("Invalid filter name in transform: '{}'".format(filtername))
        if filtername not in filters:
            raise QueryException("No such filter in transform: '{}'".format(filtername))
        events = filters[filtername](vfilter, events, ds, limit, start, end)

    return events


# TODO: Get rid of union here?
def query(query: Union[Query, dict], ds: Datastore,
          limit: int=-1, start: datetime=None, end: datetime=None):
    if isinstance(query, dict):
        if "transforms" not in query:
            raise QueryException("Query does not contain a transform: {}".format(query))
        query = Query(query["transforms"], chunk=query["chunk"] if "chunk" in query else False)

    events = []  # type: List[Event]
    for transform in query.transforms:
        events += bucket_transform(transform, ds, limit, start, end)

    if query.chunk:
        result = chunk(events, query.chunk)
    else:
        result = {}
        result["eventcount"] = len(events)
        result["eventlist"] = []
        result["duration"] = timedelta()
        for event in events:
            result["duration"] += event.duration
            result["eventlist"].append(event.to_json_dict())
        result["duration"] = result["duration"].total_seconds()
    return result


"""
CHUNKERS
========
"""


def chunk(events: List[Event], chunk_key: str):
    result = transforms.full_chunk(events, chunk_key)
    # Turn all timedeltas into duration-dicts
    for label, lv in result["chunks"].items():
        if "duration" in lv:
            result["chunks"][label]["duration"] = lv["duration"].total_seconds()
        for key, kv in lv["data"].items():
            if "duration" in kv:
                result["chunks"][label]["data"][key]["duration"] = kv["duration"].total_seconds()
            for value, vv in kv["values"].items():
                if "duration" in vv:
                    result["chunks"][label]["data"][key]["values"][value]["duration"] = vv["duration"].total_seconds()
    result["duration"] = result["duration"].total_seconds()
    return result


"""
FILTERS
=======
"""


def filter_keyvals(tfilter, events: List[Event], ds: Datastore, limit=-1, start: datetime=None, end: datetime=None, exclude: bool=False):
    if "key" not in tfilter:
        raise QueryException("filter_keyvals filter misses key field: {}".format(tfilter))
    elif "vals" not in tfilter:
        raise QueryException("filter_keyvals filter misses vals field: {}".format(tfilter))
    else:
        key = tfilter["key"] # type: str
        vals = tfilter["vals"]  # type: list
        return transforms.filter_keyvals(events, key, vals, exclude=exclude)


# @deprecated
def include_keyvals(tfilter, events, ds, limit=-1, start=None, end=None):
    return filter_keyvals(tfilter, events, ds, limit=limit, start=start, end=end)


# @deprecated
def exclude_keyvals(tfilter, events, ds, limit=-1, start=None, end=None):
    return filter_keyvals(tfilter, events, ds, limit=limit, start=start, end=end, exclude=True)


def timeperiod_intersect(tfilter, events, ds, limit=-1, start=None, end=None):
    filterevents = []
    for btransform in tfilter["transforms"]:
        filterevents += bucket_transform(btransform, ds, limit, start, end)
    events = transforms.filter_period_intersect(events, filterevents)
    return events


filters = {
    'exclude_keyvals': exclude_keyvals,
    'include_keyvals': include_keyvals,
    'timeperiod_intersect': timeperiod_intersect,
}
