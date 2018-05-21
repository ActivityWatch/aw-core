import logging
from typing import List
import re

from aw_core.models import Event

logger = logging.getLogger(__name__)


def filter_keyvals(events: List[Event], key: str, vals: List[str], exclude=False) -> List[Event]:
    def predicate(event):
        for val in vals:
            if key in event.data and val == event.data[key]:
                return True
        return False

    if exclude:
        return list(filter(lambda e: not predicate(e), events))
    else:
        return list(filter(lambda e: predicate(e), events))


def filter_keyvals_regex(events: List[Event], key: str, regex: str) -> List[Event]:
    r = re.compile(regex)

    def predicate(event):
        return bool(r.findall(event.data[key]))

    return list(filter(lambda e: predicate(e), events))
