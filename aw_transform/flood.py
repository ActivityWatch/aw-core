import logging
from datetime import timedelta
from copy import deepcopy
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def flood(events: List[Event], pulsetime: float=5) -> List[Event]:
    """
    See details on flooding here:
     - https://github.com/ActivityWatch/activitywatch/issues/124

    Copied here from: https://github.com/ActivityWatch/aw-analysis/blob/7da1f2cd8552f866f643501de633d74cdecab168/aw_analysis/flood.py
    """
    events = deepcopy(events)
    events = sorted(events, key=lambda e: e.timestamp)

    warned_about_negative_gap_safe = False
    warned_about_negative_gap_unsafe = False

    for e1, e2 in zip(events[:-1], events[1:]):
        gap = e2.timestamp - (e1.timestamp + e1.duration)

        # Sanity check in case events overlap
        if gap < timedelta(0):
            if e1.data == e2.data:
                # Events with negative gap but same data safely be merged
                start = min(e1.timestamp, e2.timestamp)
                end = max(e1.timestamp + e1.duration, e2.timestamp + e2.duration)
                e1.timestamp = start
                e1.duration = (end - start)
                e2.timestamp = end
                e2.duration = timedelta(0)
                if not warned_about_negative_gap_safe:
                    logger.warning("Gap was of negative duration ({}s), but could be safely merged. This error will only show once per batch.".format(gap.total_seconds()))
                    warned_about_negative_gap_safe = True
                gap = e2.timestamp - (e1.timestamp + e1.duration)
            elif not warned_about_negative_gap_unsafe:
                # Events with negative gap but differing data cannot be merged
                logger.warning("Gap was of negative duration ({}s), and could NOT be safely merged. This error will only show once per batch.".format(gap.total_seconds()))
                warned_about_negative_gap_unsafe = True
                # logger.warning("Event 1 (id {}): {} {}".format(e1.id, e1.timestamp, e1.duration))
                # logger.warning("Event 2 (id {}): {} {}".format(e2.id, e2.timestamp, e2.duration))

        # if timedelta(0) <= gap <= timedelta(seconds=pulsetime):
        if timedelta(0) < gap <= timedelta(seconds=pulsetime):
            e2_end = e2.timestamp + e2.duration

            # Prioritize flooding from the longer event
            if e1.duration >= e2.duration:
                if e1.data == e2.data:
                    # Extend e1 to the end of e2
                    # Set duration of e2 to zero (mark to delete)
                    e1.duration = e2_end - e1.timestamp
                    e2.timestamp = e2_end
                    e2.duration = timedelta(0)
                else:
                    # Extend e1 to the start of e2
                    e1.duration = e2.timestamp - e1.timestamp
            else:
                if e1.data == e2.data:
                    # Extend e2 to the start of e1, discard e1
                    e2.timestamp = e1.timestamp
                    e2.duration = e2_end - e2.timestamp
                    e1.duration = timedelta(0)
                else:
                    # Extend e2 backwards to end of e1
                    e2.timestamp = e1.timestamp + e1.duration
                    e2.duration = e2_end - e2.timestamp

    # Filter out remaining zero-duration events
    events = [e for e in events if e.duration > timedelta(0)]

    return events
