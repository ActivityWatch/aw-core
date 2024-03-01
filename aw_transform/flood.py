import logging
from datetime import timedelta
from copy import deepcopy
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def _flood(e1: Event, e2: Event):
    """Floods the larger event over the smaller event, in-place"""
    # Prioritize flooding from the longer event
    # NOTE: Perhaps better to flood from the former event?
    e2_end = e2.timestamp + e2.duration
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


def _flood_first(e1: Event, e2: Event):
    """Floods the larger event over the smaller event, in-place"""
    # Prioritize flooding from the longer event
    # NOTE: Perhaps better to flood from the former event?
    e2_end = e2.timestamp + e2.duration
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


def _trim(e1: Event, e2: Event):
    """Trims the part of a smaller event covered by a larger event, in-place"""
    e1_end = e1.timestamp + e1.duration
    e2_end = e2.timestamp + e2.duration

    if e1.duration > e2.duration:
        # Trim e2 to remove overlap
        e2.timestamp = e1_end
        e2.duration = e2_end - e1_end
    else:
        # Trim e1 to remove overlap
        e1.duration = e2.timestamp - e1.timestamp


def flood(events: List[Event], pulsetime: float = 5) -> List[Event]:
    """
    Floods event to the nearest neighbouring event if within the specified ``pulsetime``.

    Takes a list of ``events`` and "floods" empty space between events smaller than ``pulsetime``, by extending one of the surrounding events to cover the empty space.

    Also merges events if they have the same data and are within the pulsetime

    Originally written in aw-research: https://github.com/ActivityWatch/aw-analysis/blob/7da1f2cd8552f866f643501de633d74cdecab168/aw_analysis/flood.py

    Also implemented in aw-server-rust: https://github.com/ActivityWatch/aw-server-rust/blob/master/aw-transform/src/flood.rs

    # Example

    ```ignore
    pulsetime: 1 second (one space)
    input:     [a] [a]  [b][b]    [b][c]
    output:    [a     ][b    ]    [b][c]
    ```

    For more details on flooding, see this issue:
     - https://github.com/ActivityWatch/activitywatch/issues/124

    NOTE: This algorithm has a lot of smaller details that need to be
          carefully considered by anyone wishing to edit it, see:
           - https://github.com/ActivityWatch/aw-core/pull/73
    """

    events = deepcopy(events)
    events = sorted(events, key=lambda e: (e.timestamp, e.duration))

    # If negative gaps are smaller than this, prune them to become zero
    gap_trim_thres = timedelta(seconds=0.1)

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
                    f"Gap was negative but could be safely merged ({gap.total_seconds()}s). Will only warn once per batch."  # {e1.data}
                )
                logger.debug(f"{e1.data}")
                warned_about_negative_gap_safe = True
        elif gap < -gap_trim_thres:
            # Events with negative gap but differing data cannot be merged safely
            # We still need to get rid of the gap however, so we will trim the smaller event.
            # TODO: This might be a bad idea, could lead to a huge chunk of non-AFK time getting whacked, or vice versa.
            _trim(e1, e2)

            if not warned_about_negative_gap_unsafe:
                logger.warning(
                    f"Gap was negative and could NOT be safely merged ({gap.total_seconds()}s). Will only warn once per batch."
                )
                logger.debug(f"{e1.data} != {e2.data}")
                warned_about_negative_gap_unsafe = True
                # logger.warning("Event 1 (id {}): {} {}".format(e1.id, e1.timestamp, e1.duration))
                # logger.warning("Event 2 (id {}): {} {}".format(e2.id, e2.timestamp, e2.duration))
        elif -gap_trim_thres < gap <= timedelta(seconds=pulsetime):
            _flood(e1, e2)

    # Filter out remaining zero-duration events
    events = [e for e in events if e.duration > timedelta(0)]

    return events
