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
        {'labels': [labels]},
    'label_exclude':
        {'labels': [labels]},
    'timeperiod_intersect':
        {'transforms': [btransform]},

    btransform: {
        'bucket': 'bucketname',
        'filters': [filter],
    }
    view: {
        "name": 'viewname'
        "transforms": [btransform],
        "cache": true/false,
        "chunk": true/false,
        "created": date,
    }
    """
    views[view["name"]] = view


@dirs.ensure_returned_path_exists
def get_view_cache_directory(viewname, dsname):
    cache_dir = dirs.get_cache_dir("view_cache")
    cache_dir = os.path.join(cache_dir, dsname)
    cache_dir = os.path.join(cache_dir, viewname)
    return cache_dir


def get_view_cache_file(viewname, ds, start, end):
    cache_filename = "{} to {}".format(start, end)
    cache_dir = get_view_cache_directory(viewname, ds.storage_strategy.sid)
    cache_file = os.path.join(cache_dir, cache_filename)
    return cache_file


def get_cached_query(viewname, ds, start, end):
    cache_file = get_view_cache_file(viewname, ds, start, end)
    if os.path.isfile(cache_file):
        logger.debug("Retrieving cached query {}".format(cache_file))
        with open(cache_file, 'r') as f:
            return json.load(f)
    else:
        return None


def cache_query(data, viewname, ds, start, end):
    cache_file = get_view_cache_file(viewname, ds, start, end)
    logger.debug("Caching query {}".format(cache_file))
    with open(cache_file, 'w') as f:
        json.dump(data, f)


def query_view(viewname, ds, limit=-1, start=None, end=None):
    if views[viewname]["query"]["cache"]:
        if end < datetime.now(timezone.utc):
            cached_result = get_cached_query(viewname, ds, start, end)
            if cached_result:
                return cached_result
    result = query(views[viewname]["query"], ds, limit, start, end)
    if views[viewname]["query"]["cache"]:
        if end < datetime.now(timezone.utc):
            cache_query(result, viewname, ds, start, end)
    return result


def get_views():
    return [view for view in views]


def get_view(viewname):
    if viewname in views:
        return views[viewname]
    else:
        return None
