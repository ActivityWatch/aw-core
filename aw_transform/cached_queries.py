import os
import json
import logging
from datetime import datetime, timezone

from aw_core import dirs
from aw_core.models import Event

logger = logging.getLogger(__name__)


def get_query_cache_directory(queryname, dsname):
    cache_dir = dirs.get_cache_dir("query_cache")
    cache_dir = os.path.join(cache_dir, dsname)
    cache_dir = os.path.join(cache_dir, queryname)
    dirs.ensure_path_exists(cache_dir)
    return cache_dir


def get_query_cache_file(queryname, ds, start, end):
    if start:
        start = start.astimezone(timezone.utc).strftime("%Y%m%dT%H%m%SZ")
    if end:
        end = end.astimezone(timezone.utc).strftime("%Y%m%dT%H%m%SZ")
    cache_filename = "{}_to_{}".format(start, end)
    cache_dir = get_query_cache_directory(queryname, ds.storage_strategy.sid)
    cache_file = os.path.join(cache_dir, cache_filename)
    return cache_file


def get_cached_query(queryname, ds, start, end):
    if end and end < datetime.now(timezone.utc):
        cache_file = get_query_cache_file(queryname, ds, start, end)
        if os.path.isfile(cache_file):
            logger.debug("Retrieving cached query {}".format(cache_file))
            with open(cache_file, 'r') as f:
                result = json.load(f)
                if isinstance(result, list):
                    result = [Event(**e) for e in result]
                return result
    return None


def cache_query(result, queryname, ds, start, end):
    cache_file = get_query_cache_file(queryname, ds, start, end)
    logger.debug("Caching query {}".format(cache_file))
    if isinstance(result, list):
        result = [e.to_json_dict() for e in result]
    with open(cache_file, 'w') as f:
        json.dump(result, f)
