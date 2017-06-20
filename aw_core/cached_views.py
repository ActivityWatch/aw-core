import os
import json
import logging
from datetime import datetime, timezone

from . import dirs

logger = logging.getLogger(__name__)


def get_view_cache_directory(viewname, dsname):
    cache_dir = dirs.get_cache_dir("view_cache")
    cache_dir = os.path.join(cache_dir, dsname)
    cache_dir = os.path.join(cache_dir, viewname)
    dirs.ensure_path_exists(cache_dir)
    return cache_dir


def get_view_cache_file(viewname, ds, start, end):
    if start:
        start = start.astimezone(timezone.utc).strftime("%Y%m%dT%H%m%SZ")
    if end:
        end = end.astimezone(timezone.utc).strftime("%Y%m%dT%H%m%SZ")
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
