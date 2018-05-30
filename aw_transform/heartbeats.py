import logging
from datetime import timedelta
from typing import List, Optional

from aw_core.models import Event

logger = logging.getLogger(__name__)


def heartbeat_reduce(events: List[Event], pulsetime: float) -> List[Event]:
    """Merges consecutive events together according to the rules of `heartbeat_merge`."""
    reduced = []
    if len(events) > 0:
        reduced.append(events.pop(0))
    for heartbeat in events:
        merged = heartbeat_merge(reduced[-1], heartbeat, pulsetime)
        if merged is not None:
            # Heartbeat was merged
            reduced[-1] = merged
        else:
            # Heartbeat was not merged
            reduced.append(heartbeat)
    return reduced


def heartbeat_merge(last_event: Event, heartbeat: Event, pulsetime: float) -> Optional[Event]:
    """
    Merges two events if they have identical labels and are
    separated by a time smaller than :code:`pulsetime` seconds.
    """
    if last_event.data == heartbeat.data:
        gap = heartbeat.timestamp - (last_event.timestamp + last_event.duration)

        if gap <= timedelta(seconds=pulsetime):
            # Heartbeat was within pulsetime window, set duration of last event appropriately
            last_event.duration = (heartbeat.timestamp - last_event.timestamp) + heartbeat.duration
            if last_event.duration < timedelta(0):
                logger.warning("Heartbeat got a negative duration, forcing to zero.")
                last_event.duration = timedelta(0)
            return last_event

    return None
