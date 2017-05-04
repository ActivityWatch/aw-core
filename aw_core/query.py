from . import transforms

from datetime import datetime, timedelta


class QueryException(Exception):
    pass


def bucket_transform(btransform, ds, limit=-1, start=None, end=None):
    if "bucket" not in btransform:
        raise QueryException("No bucket specified in transform: {}".format(btransform))
    if not isinstance(btransform["bucket"], str):
        raise QueryException("Invalid bucket name in transform: '{}'".format(btransform["bucket"]))
    if not btransform["bucket"] in ds.buckets():
        raise QueryException("Cannot query bucket that doesn't exist in transform: '{}'".format(btransform["bucket"]))
    # Get events
    events = ds[btransform["bucket"]].get(limit, start, end)
    # Apply filters
    if "filters" in btransform:
        for vfilter in btransform["filters"]:
            if "name" not in vfilter:
                raise QueryException("No filter specified in transform: {}".format(vfilter))
            filtername = vfilter["name"]
            if not isinstance(filtername, str):
                raise QueryException("Invalid filter name in transform: '{}'".format(filtername))
            if filtername not in filters:
                raise QueryException("No such filter in transform: '{}'".format(filtername))
            events = filters[filtername](vfilter, events, ds, limit, start, end)
    return events


def query(query, ds, limit=-1, start=None, end=None):
    events = []
    if "transforms" not in query:
        raise QueryException("Query does not contain a transform: {}".format(query))
    for transform in query["transforms"]:
        events += bucket_transform(transform, ds, limit, start, end)

    if "chunk" in query and query["chunk"]:
        result = chunk(events, query["chunk"])
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

"""

def chunk(events, chunk_key):
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

"""


def include_keyvals(tfilter, events, ds, limit=-1, start=None, end=None):
    if "key" not in tfilter:
        raise QueryException("include_keyvals filter misses key field: {}".format(tfilter))
    elif "vals" not in tfilter:
        raise QueryException("include_keyvals filter misses vals field: {}".format(tfilter))
    else:
        key = tfilter["key"] # type: str
        vals = tfilter["vals"]  # type: list
        return transforms.include_keyvals(events, key, vals)


def exclude_keyvals(tfilter, events, ds, limit=-1, start=None, end=None):
    if "key" not in tfilter:
        raise QueryException("exclude_keyvals filter misses key field: {}".format(tfilter))
    elif "vals" not in tfilter:
        raise QueryException("exclude_keyvals filter misses vals field: {}".format(tfilter))
    else:
        key = tfilter["key"] # type: str
        vals = tfilter["vals"]  # type: list
        return transforms.exclude_keyvals(events, key, vals)


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
