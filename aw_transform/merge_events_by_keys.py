import logging
from typing import Dict

from aw_core.models import Event

logger = logging.getLogger(__name__)


def merge_events_by_keys(events, keys) -> Dict:
    # The result will be a list of events without timestamp since they are merged
    # Call recursively until all keys are consumed
    if len(keys) < 1:
        return events
    merged_events = {}
    for event in events:
        summed_key = ""
        for key in keys:
            if key in event.data:
                summed_key = summed_key + "." + event["data"][key]
        if summed_key not in merged_events:
            merged_events[summed_key] = Event(
                timestamp=event.timestamp,
                duration=event.duration,
                data={}
            )
            for key in keys:
                if key in event.data:
                    merged_events[summed_key].data[key] = event.data[key]
        else:
            merged_events[summed_key].duration += event.duration
    result = []
    for key in merged_events:
        result.append(Event(**merged_events[key]))
    return result
