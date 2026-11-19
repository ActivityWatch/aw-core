import logging
import warnings
from datetime import datetime, timedelta
from typing import List, Optional

from aw_core.models import Event

logger = logging.getLogger(__name__)


CHUNK_DEPRECATION = (
    "chunk_events_by_key is deprecated and will be removed. There is no drop-in "
    "replacement: merge_events_by_keys merges all events with the same value "
    "(also across gaps) and doesn't produce subevents."
)


def chunk_events_by_key(
    events: List[Event], key: str, pulsetime: float = 5.0
) -> List[Event]:
    """
    "Chunks" adjacent events together which have the same value for a key, and stores the
    original events in the :code:`subevents` key of the new event.
    Events are processed in timestamp order without modifying the input list.
    Chunk durations sum event durations, excluding gaps; the pulse window is
    measured from the actual end of the chunk, not that summed duration.

    .. deprecated::
        Will be removed (ActivityWatch/activitywatch#1466). There is no
        drop-in replacement: aw-server-rust never supported ``subevents``, and
        :func:`merge_events_by_keys` merges all events with the same value,
        also across gaps, instead of adjacent runs.
    """
    warnings.warn(CHUNK_DEPRECATION, DeprecationWarning, stacklevel=2)
    chunked_events: List[Event] = []
    chunk_end: Optional[datetime] = None
    for event in sorted(events, key=lambda e: e.timestamp):
        if key not in event.data:
            break
        event_end = event.timestamp + event.duration
        if (
            chunk_end is not None
            and chunked_events[-1].data[key] == event.data[key]
            and event.timestamp - chunk_end < timedelta(seconds=pulsetime)
        ):
            chunked_event = chunked_events[-1]
            chunked_event.duration += event.duration
            chunked_event.data["subevents"].append(event)
            chunk_end = max(chunk_end, event_end)
        else:
            data = {key: event.data[key], "subevents": [event]}
            chunked_event = Event(
                timestamp=event.timestamp, duration=event.duration, data=data
            )
            chunked_events.append(chunked_event)
            chunk_end = event_end

    return chunked_events
