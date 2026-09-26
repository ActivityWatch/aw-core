import logging
import warnings
from datetime import timedelta
from typing import List

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

    .. deprecated::
        Will be removed (ActivityWatch/activitywatch#1466). There is no
        drop-in replacement: aw-server-rust never supported ``subevents``, and
        :func:`merge_events_by_keys` merges all events with the same value,
        also across gaps, instead of adjacent runs.
    """
    warnings.warn(CHUNK_DEPRECATION, DeprecationWarning, stacklevel=2)
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
