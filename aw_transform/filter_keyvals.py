import logging
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def filter_keyvals(events, key, vals, exclude=False) -> List[Event]:
    def predicate(event):
        for val in vals:
            if key in event.data and val == event.data[key]:
                return True
        return False

    if exclude:
        return list(filter(lambda e: not predicate(e), events))
    else:
        return list(filter(lambda e: predicate(e), events))
