from .query import query

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
        "chunk": true/false,
        "created": date,
    }
    """
    views[view["name"]] = view


def query_view(viewname, ds):
    print(viewname)
    return query(views[viewname]["query"], ds)

def get_views():
    return [view for view in views]

def get_view(viewname):
    if viewname in views:
        return views[viewname]
    else:
        return None
