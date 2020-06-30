import logging
from datetime import timedelta
from copy import deepcopy
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def flood(events: List[Event], pulsetime: float = 5) -> List[Event]:
    """
    Takes a list of events and "floods" any empty space between events by extending one of the surrounding events to cover the empty space.

    For more details on flooding, see this issue:
     - https://github.com/ActivityWatch/activitywatch/issues/124
    """
    # Originally written in aw-research: https://github.com/ActivityWatch/aw-analysis/blob/7da1f2cd8552f866f643501de633d74cdecab168/aw_analysis/flood.py
    # NOTE: This algorithm has a lot of smaller details that need to be
    #       carefully considered by anyone wishing to edit it, see:
    #        - https://github.com/ActivityWatch/aw-core/pull/73

    events = deepcopy(events)
    events = sorted(events, key=lambda e: e.timestamp)

    # If negative gaps are smaller than this, prune them to become zero
    negative_gap_trim_thres = timedelta(seconds=0.1)

    warned_about_negative_gap_safe = False
    warned_about_negative_gap_unsafe = False

    for e1, e2 in zip(events[:-1], events[1:]):
        gap = e2.timestamp - (e1.timestamp + e1.duration)

        if not gap:
            continue

        # Sanity check in case events overlap
        if gap < timedelta(0) and e1.data == e2.data:
            # Events with negative gap but same data can safely be merged
            start = min(e1.timestamp, e2.timestamp)
            end = max(e1.timestamp + e1.duration, e2.timestamp + e2.duration)
            e1.timestamp, e1.duration = start, (end - start)
            e2.timestamp, e2.duration = end, timedelta(0)
            if not warned_about_negative_gap_safe:
                logger.warning(
                    "Gap was of negative duration but could be safely merged ({}s). This message will only show once per batch.".format(
                        gap.total_seconds()
                    )
                )
                warned_about_negative_gap_safe = True
        elif gap < -negative_gap_trim_thres and not warned_about_negative_gap_unsafe:
            # Events with negative gap but differing data cannot be merged safely
            logger.warning(
                "Gap was of negative duration and could NOT be safely merged ({}s). This warning will only show once per batch.".format(
                    gap.total_seconds()
                )
            )
            warned_about_negative_gap_unsafe = True
            # logger.warning("Event 1 (id {}): {} {}".format(e1.id, e1.timestamp, e1.duration))
            # logger.warning("Event 2 (id {}): {} {}".format(e2.id, e2.timestamp, e2.duration))
        elif -negative_gap_trim_thres < gap <= timedelta(seconds=pulsetime):
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
