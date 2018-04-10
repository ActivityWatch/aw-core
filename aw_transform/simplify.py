import re
from copy import deepcopy
from typing import List

from aw_core.models import Event


def simplify_string(events, key="title") -> List[Event]:
    events = deepcopy(events)
    for e in events:
        # Remove prefixes that are numbers within parenthesis
        # Example: "(2) Facebook" -> "Facebook"
        # Example: "(1) YouTube" -> "YouTube"
        e.data[key] = re.sub(r"^\([0-9]+\)\s*", "", e.data[key])

        # Remove FPS display in window title
        # Example: "Cemu - FPS: 59.2 - ..." -> "Cemu - FPS: ... - ..."
        e.data[key] = re.sub(r"FPS:\s+[0-9\.]+", "FPS: ...", e.data[key])
    return events
