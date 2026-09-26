import copy
import json
import logging
from typing import Dict, List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def merge_events_by_keys(events: List[Event], keys: List[str]) -> List[Event]:
    """
    Merges all events that share the same values for all of ``keys``, whether
    they are adjacent or not, summing their durations.

    Each merged event keeps the timestamp and the whole ``data`` of the first
    event in its group (not only the merge keys), so fields that are the same
    across the group, like ``$category`` for ``["app", "title"]``, stay
    available. Events missing any of the keys are dropped, and an empty key list
    returns no events. This matches aw-server-rust (ActivityWatch/activitywatch#1466).
    """
    if not keys:
        return []
    merged_events: Dict[str, Event] = {}
    for event in events:
        try:
            values = [event.data[key] for key in keys]
        except KeyError:
            continue
        # Group by the JSON values, like aw-server-rust (so 1 and 1.0 differ,
        # and list values such as categories work).
        composite_key = json.dumps(values, sort_keys=True, default=str)
        merged = merged_events.get(composite_key)
        if merged is None:
            merged_events[composite_key] = Event(
                timestamp=event.timestamp,
                duration=event.duration,
                data=copy.deepcopy(event.data),
            )
        else:
            merged.duration += event.duration
    return list(merged_events.values())
