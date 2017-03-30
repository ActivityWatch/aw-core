import os
import json
import logging
from datetime import datetime, timezone

from .query import query
from aw_core import dirs

logger = logging.getLogger("aw.core.views")

views = {}

def create_view(view):
    """
    filter: {
        'name': 'filtername'
        **** PARAMETERS ****
    }
    'label_include':
        {'labels': [labels]}
    'label_exclude':
        {'labels': [labels]}
    'timeperiod_intersect':
        {'transforms': [btransform]}

    buckettransform: {
        'bucket': 'bucketname',
        'filters': [filter],
    }

    viewquery{
        "limit": -1,
        "start": None,
        "end": None,
    }

    viewtransform: {
        view: 'viewname',
        queries: [
            viewquery
        ]
    }

    "transforms": [
        btransform/viewtransform
    ]

    view: {
        "name": 'viewname',
        "cache": true/false,
        "chunk": full/label/false,
        "type": 'view'/'bucket',
        "query": transforms,
    }
    """
    views[view["name"]] = view

class ViewException(Exception):
    pass

@dirs.ensure_returned_path_exists
def get_view_cache_directory(viewname, dsname):
    cache_dir = dirs.get_cache_dir("view_cache")
    cache_dir = os.path.join(cache_dir, dsname)
    cache_dir = os.path.join(cache_dir, viewname)
    return cache_dir


def get_view_cache_file(viewname, ds, start, end):
    cache_filename = "{}_to_{}".format(start, end)
    cache_dir = get_view_cache_directory(viewname, ds.storage_strategy.sid)
    cache_file = os.path.join(cache_dir, cache_filename)
    return cache_file


def get_cached_query(viewname, ds, start, end):
    if end and end < datetime.now(timezone.utc):
        cache_file = get_view_cache_file(viewname, ds, start, end)
        if os.path.isfile(cache_file):
            logger.debug("Retrieving cached query {}".format(cache_file))
            with open(cache_file, 'r') as f:
                return json.load(f)
    return None


def cache_query(data, viewname, ds, start, end):
    cache_file = get_view_cache_file(viewname, ds, start, end)
    logger.debug("Caching query {}".format(cache_file))
    with open(cache_file, 'w') as f:
        json.dump(data, f)

def query_view(viewname, ds, limit=-1, start=None, end=None):
    view = views[viewname]
    # Check if query should be cached
    cache = False
    if "cache" in view["query"] and view["query"]["cache"]:
        cache = True
    # Check if query already is cached
    if cache:
        cached_result = get_cached_query(viewname, ds, start, end)
        if cached_result:
            return cached_result
    # Do query
    if "type" not in view:
        raise ViewException("View type unspecified")
    if view["type"] == "bucket":
        result = query(view["query"], ds, limit, start, end)
    elif view["type"] == "view":
        query_multiview(viewname, ds, limit, start, end)
    else:
        raise ViewException("View type invalid")
    # Cache result
    if cache:
        if end and end < datetime.now(timezone.utc):
            cache_query(result, viewname, ds, start, end)
    return result


def query_multiview(viewname, ds, limit, start, end):
    viewname = view["query"]["viewname"]
    for q in view["query"]["queries"]:
        limit = query["limit"] if "limit" in q else -1
        start = query["start"] if "start" in q else None
        end   = query["end"] if "end" in q else None
        result = query_view(viewname, ds, limit, start, end)


def get_views():
    return [view for view in views]


def get_view(viewname):
    if viewname in views:
        return views[viewname]
    else:
        return None
