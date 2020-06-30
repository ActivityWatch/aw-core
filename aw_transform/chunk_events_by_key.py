import logging
from typing import List, Dict
from datetime import timedelta

from aw_core.models import Event

logger = logging.getLogger(__name__)


def chunk_events_by_key(
    events: List[Event], key: str, pulsetime: float = 5.0
) -> List[Event]:
    chunked_events: List[Event] = []
    for event in events:
        if key not in event.data:
            break
        timediff = timedelta(seconds=999999999)  # FIXME: ugly but works
        if len(chunked_events) > 0:
            timediff = event.timestamp - (events[-1].timestamp + events[-1].duration)
        if (
            len(chunked_events) > 0
            and chunked_events[-1].data[key] == event.data[key]
            and timediff < timedelta(seconds=pulsetime)
        ):
            chunked_event = chunked_events[-1]
            chunked_event.duration += event.duration
            chunked_event.data["subevents"].append(event)
        else:
            data = {key: event.data[key], "subevents": [event]}
            chunked_event = Event(
                timestamp=event.timestamp, duration=event.duration, data=data
            )
            chunked_events.append(chunked_event)

    return chunked_events
