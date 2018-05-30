import logging
from datetime import timedelta
from copy import deepcopy
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def flood(events: List[Event], pulsetime: float=5):
    """
    See details on flooding here:
     - https://github.com/ActivityWatch/activitywatch/issues/124

    Copied here from: https://github.com/ActivityWatch/aw-analysis/blob/7da1f2cd8552f866f643501de633d74cdecab168/aw_analysis/flood.py
    """
    events = deepcopy(events)
    events = sorted(events, key=lambda e: e.timestamp)

    for e1, e2 in zip(events[:-1], events[1:]):
        gap = e2.timestamp - (e1.timestamp + e1.duration)

        # Sanity check
        if gap < timedelta(0):
            logger.warning("Gap was of negative duration ({}s), this might cause issues".format(gap.total_seconds()))
            logger.warning(str(e1))

        if gap <= timedelta(seconds=pulsetime):
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
