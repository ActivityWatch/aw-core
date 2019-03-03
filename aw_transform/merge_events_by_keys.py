import logging
from typing import List, Dict

from aw_core.models import Event

logger = logging.getLogger(__name__)


def merge_events_by_keys(events, keys) -> List[Event]:
    # The result will be a list of events without timestamp since they are merged
    # Call recursively until all keys are consumed
    if len(keys) < 1:
        return events
    merged_events = {}  # type: Dict[str, Event]
    for event in events:
        summed_key = "/".join(event['data'][key] for key in keys)
        if summed_key not in merged_events:
            merged_events[summed_key] = Event(
                timestamp=event.timestamp,
                duration=event.duration,
                data=event.data
            )
        else:
            merged_events[summed_key].duration += event.duration
    result = [Event(**event) for event in merged_events.values()]
    return result
