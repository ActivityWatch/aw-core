import logging
from typing import List, Dict

from aw_core.models import Event

logger = logging.getLogger(__name__)


def chunk_events_by_key(events, key) -> List[Event]:
    chunked_events = [] # type: List[Event]
    for event in events:
        if key not in event.data:
            pass
        elif len(chunked_events) == 0 or chunked_events[-1].data[key] != event.data[key]:
            data = {key: event.data[key], "subevents": [event]}
            chunked_event = Event(timestamp=event.timestamp, duration=event.duration, data=data)
            chunked_events.append(chunked_event)
        else:
            chunked_event = chunked_events[-1]
            chunked_event.duration += event.duration
            chunked_event.data["subevents"].append(event)

    return chunked_events
