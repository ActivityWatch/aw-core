import re
from copy import deepcopy
from typing import List

from aw_core import Event


def simplify_string(events: List[Event], key: str = "title") -> List[Event]:
    events = deepcopy(events)

    re_leadingdot = re.compile(r"^(●|\*)\s*")
    re_parensprefix = re.compile(r"^\([0-9]+\)\s*")
    re_fps = re.compile(r"FPS:\s+[0-9\.]+")

    for e in events:
        # Remove prefixes that are numbers within parenthesis
        # Example: "(2) Facebook" -> "Facebook"
        # Example: "(1) YouTube" -> "YouTube"
        e.data[key] = re_parensprefix.sub("", e.data[key])

        # Things generally specific to window events with the "app" key
        if key == "title" and "app" in e["data"]:
            # Remove FPS display in window title
            # Example: "Cemu - FPS: 59.2 - ..." -> "Cemu - FPS: ... - ..."
            e.data[key] = re_fps.sub("FPS: ...", e.data[key])

            # For VSCode (uses ●), gedit (uses *), et al
            # See: https://github.com/ActivityWatch/aw-watcher-window/issues/32
            e.data[key] = re_leadingdot.sub("", e.data[key])
    return events
