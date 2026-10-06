import copy
import json
import logging
from typing import Any, Dict, List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def _non_json_key(value: Any) -> Dict[str, str]:
    # Event data from the datastore is always JSON, like in aw-server-rust.
    # Other values (only possible through the Python API) are tagged with their
    # type, so e.g. a datetime can't merge with a string that looks the same.
    return {
        "$non-json": f"{type(value).__module__}.{type(value).__qualname__}:{value!r}"
    }


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
        composite_key = json.dumps(values, sort_keys=True, default=_non_json_key)
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
