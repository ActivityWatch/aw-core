import logging
from datetime import datetime, timezone

from .query import query
from .transforms import merge_queries
from .cached_views import get_cached_query, cache_query

from aw_datastore import Datastore

logger = logging.getLogger("aw.core.views")

_views = {}


# TODO: Needs better type annotation
def create_view(view: dict):
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
        "start": None,
        "end": None,
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
    _views[view["name"]] = view


class ViewException(Exception):
    pass


def query_view(viewname: str, ds: Datastore, start: datetime=None, end: datetime=None):
    if viewname not in _views:
        raise ViewException("Tried to query non-existing view named {}".format(viewname))

    view = _views[viewname]

    # Check if query should be cached
    cache = "cache" in view["query"] and view["query"]["cache"]

    # Check if query already is cached
    if cache:
        cached_result = get_cached_query(viewname, ds, start, end)
        if cached_result:
            return cached_result

    # Do query
    result = query(view["query"], ds, -1, start, end)

    # Cache result
    if cache:
        if end and end < datetime.now(timezone.utc):
            cache_query(result, viewname, ds, start, end)

    return result


def query_multiview(viewname, ds, starts=[], ends=[]):
    if viewname not in _views:
        raise ViewException("Tried to query non-existing view named {}".format(viewname))

    if len(starts) != len(ends):
        raise ViewException("query_multiview call has more start points than endpoints")

    result = None
    for i in range(len(starts)):
        next_result = query_view(viewname, ds, starts[i], ends[i])
        if not result:
            result = next_result
        else:
            result = merge_queries(result, next_result)

    return result


def get_views():
    return [view for view in _views]


def get_view(viewname):
    if viewname in _views:
        return _views[viewname]
    else:
        return None
