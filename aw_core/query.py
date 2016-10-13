from . import transforms

def bucket_transform(btransform, ds, limit=-1, start=None, end=None):
    if not "bucket" in btransform:
        # TODO: Better handling
        raise "No bucket specified!"
    if not btransform["bucket"] in ds.buckets():
        # TODO: Better handling
        raise "Cannot query bucket that doesn't exist!"
    # Get events
    events = ds[btransform["bucket"]].get(limit, start, end)
    # Apply filters
    if "filters" in btransform:
        for vfilter in btransform["filters"]:
            filtername = vfilter["name"]
            if filtername not in filters:
                # TODO: Handle better
                raise "No such filter"
            events = filters[filtername](vfilter, events, ds, limit, start, end)
    return events


def query(query, ds, limit=0, start=None, end=None):
    events = []
    for transform in query["transforms"]:
        events += bucket_transform(transform, ds, limit, start, end)
    
    if "chunk" in query and query["chunk"]:
        result = transforms.chunk(events)
    else:
        result = {}
        result["eventcount"] = len(events)
        result["eventlist"] = []
        for event in events:
            result["eventlist"].append(event.to_json_dict())
    return result


"""

FILTERS

"""

def include_labels(tfilter, events, ds, limit=-1, start=None, end=None):
    labels = tfilter["labels"] # list
    return transforms.include_labels(events, labels)

def exclude_labels(tfilter, events, ds, limit=-1, start=None, end=None):
    labels = tfilter["labels"] # list
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
