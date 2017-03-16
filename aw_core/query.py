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


def _timedelta_to_dict(td: timedelta):
    return {"value": td.total_seconds(), "unit": "s"}


def query(query, ds, limit=-1, start=None, end=None):
    events = []
    if "transforms" not in query:
        raise QueryException("Query does not contain a transform: {}".format(query))
    for transform in query["transforms"]:
        events += bucket_transform(transform, ds, limit, start, end)

    if "chunk" in query and query["chunk"]:
        result = transforms.chunk(events)
        # Turn all timedeltas into duration-dicts
        for label, lv in result["chunks"].items():
            if "duration" in lv:
                result["chunks"][label]["duration"] = _timedelta_to_dict(lv["duration"])
            for key, kv in lv["keyvals"].items():
                if "duration" in kv:
                    result["chunks"][label]["keyvals"][key]["duration"] = _timedelta_to_dict(kv["duration"])
                for value, vv in kv["values"].items():
                    if "duration" in vv:
                        result["chunks"][label]["keyvals"][key]["values"][value]["duration"] = _timedelta_to_dict(vv["duration"])
        result["duration"] = _timedelta_to_dict(result["duration"])
    else:
        result = {}
        result["eventcount"] = len(events)
        result["eventlist"] = []
        result["duration"] = timedelta()
        for event in events:
            result["duration"] += event.duration
            result["eventlist"].append(event.to_json_dict())
        result["duration"] = _timedelta_to_dict(result["duration"])
    return result


"""

FILTERS

"""


def include_labels(tfilter, events, ds, limit=-1, start=None, end=None):
    if "labels" not in tfilter:
        return []
    else:
        labels = tfilter["labels"]  # type: list
        return transforms.include_labels(events, labels)


def exclude_labels(tfilter, events, ds, limit=-1, start=None, end=None):
    if "labels" not in tfilter:
        return events
    else:
        labels = tfilter["labels"]  # type: list
        return transforms.exclude_labels(events, labels)


def timeperiod_intersect(tfilter, events, ds, limit=-1, start=None, end=None):
    filterevents = []
    for btransform in tfilter["transforms"]:
        filterevents += bucket_transform(btransform, ds, limit, start, end)
    events = transforms.filter_period_intersect(events, filterevents)
    return events

filters = {
    'exclude_labels': exclude_labels,
    'include_labels': include_labels,
    'timeperiod_intersect': timeperiod_intersect,
}
