from . import transforms

def bucket_transform(btransform, ds):
    if not "bucket" in btransform:
        # TODO: Better handling
        raise "No such bucket"
    events = ds[btransform["bucket"]].get()
    for vfilter in btransform["filters"]:
        filtername = vfilter["name"]
        if filtername not in filters:
            # TODO: Handle better
            raise "No such filter"
        events = filters[filtername](vfilter, events, ds)
    return events


def query(query, ds):
    events = []
    for transform in query["transforms"]:
        events += bucket_transform(transform, ds)
    
    if "chunk" in query and query["chunk"]:
        result = transforms.chunk(events)
    else:
        result = []
        for event in events:
            result.append(event.to_json_dict())
    return result


"""

FILTERS

"""

def include_labels(tfilter, events, db):
    labels = tfilter["labels"] # list
    return transforms.include_labels(events, labels)

def exclude_labels(tfilter, events, db):
    labels = tfilter["labels"] # list
    return transforms.exclude_labels(events, labels)

def timeperiod_intersect(tfilter, events, db):
    filterevents = []
    for btransform in tfilter["transforms"]:
        filterevents += bucket_transform(btransform, db)
    events = transforms.filter_period_intersect(events, filterevents)
    return events

filters = {
    'exclude_labels': exclude_labels,
    'include_labels': include_labels,
    'timeperiod_intersect': timeperiod_intersect,
}
