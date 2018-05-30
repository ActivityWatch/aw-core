import logging
from datetime import timedelta
from copy import deepcopy

logger = logging.getLogger(__name__)


def flood(events, pulsetime=5, trixy=False):
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
            logger.warning(e1)

        if gap <= timedelta(seconds=pulsetime):
            # Prioritize flooding from the longer event
            if e1.duration >= e2.duration:
                # Extend e1 forwards to start of e2
                e1.duration = e2.timestamp - e1.timestamp
            else:
                # Extend e2 backwards to end of e1
                e2_end = e2.timestamp + e2.duration
                e2.timestamp = e1.timestamp + e1.duration
                e2.duration = e2_end - e2.timestamp

    return events
