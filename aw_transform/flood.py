import logging
from datetime import timedelta
from copy import deepcopy
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def flood(events: List[Event], pulsetime: float = 5) -> List[Event]:
    """Fill short gaps between events and merge nearby equal-data events.

    Events are ordered by timestamp and duration. Gaps no larger than
    ``pulsetime`` are filled. Equal-data neighbours merge across the gap; when
    their data differs, both events extend to the midpoint. Splitting an
    uncertain interval evenly is independent of the surrounding event lengths
    and matches the Rust implementation.

    Overlapping equal-data events merge. For overlapping differing-data events,
    final normalization gives the later event precedence so the result never
    double-counts time.

    See https://github.com/ActivityWatch/activitywatch/issues/124 for the data
    collection uncertainty that flooding is intended to handle.
    """
    # Originally written in aw-research: https://github.com/ActivityWatch/aw-analysis/blob/7da1f2cd8552f866f643501de633d74cdecab168/aw_analysis/flood.py
    # NOTE: This algorithm has a lot of smaller details that need to be
    #       carefully considered by anyone wishing to edit it, see:
    #        - https://github.com/ActivityWatch/aw-core/pull/73

    events = deepcopy(events)
    # Sort by (timestamp, duration) so shorter same-timestamp events come first.
    # This matches aw-server-rust's sort_by_timestamp behavior and ensures
    # a well-defined processing order when events share the same timestamp.
    events = sorted(events, key=lambda e: (e.timestamp, e.duration))

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
                    f"Gap was of negative duration but could be safely merged ({gap.total_seconds()}s). This message will only show once per batch."
                )
                warned_about_negative_gap_safe = True
        elif gap < -negative_gap_trim_thres and not warned_about_negative_gap_unsafe:
            # Events with negative gap but differing data cannot be merged safely
            logger.warning(
                f"Gap was of negative duration and could NOT be safely merged ({gap.total_seconds()}s). This warning will only show once per batch."
            )
            warned_about_negative_gap_unsafe = True
            # logger.warning("Event 1 (id {}): {} {}".format(e1.id, e1.timestamp, e1.duration))
            # logger.warning("Event 2 (id {}): {} {}".format(e2.id, e2.timestamp, e2.duration))
        elif -negative_gap_trim_thres < gap <= timedelta(seconds=pulsetime):
            e2_end = e2.timestamp + e2.duration

            if e1.data == e2.data:
                # Preserve the longer neighbour's extent while merging across
                # the gap, matching the existing semantics for equal data.
                if e1.duration >= e2.duration:
                    e1.duration = e2_end - e1.timestamp
                    e2.timestamp = e2_end
                    e2.duration = timedelta(0)
                else:
                    e2.timestamp = e1.timestamp
                    e2.duration = e2_end - e2.timestamp
                    e1.duration = timedelta(0)
            else:
                # The gap is an interval of uncertainty: without evidence that
                # either neighbour owns more of it, split it at the midpoint.
                midpoint = e1.timestamp + e1.duration + gap / 2
                e1.duration = midpoint - e1.timestamp
                e2.timestamp = midpoint
                e2.duration = e2_end - midpoint

    # Pairwise flooding can mutate an event after its previous pair has already
    # been processed. Normalize the final stream so downstream consumers never
    # double-count overlapping time. For differing data, the later event wins.
    normalized: List[Event] = []
    for event in (e for e in events if e.duration > timedelta(0)):
        while (
            normalized
            and normalized[-1].timestamp + normalized[-1].duration > event.timestamp
        ):
            previous = normalized[-1]

            if previous.data == event.data:
                end = max(
                    previous.timestamp + previous.duration,
                    event.timestamp + event.duration,
                )
                previous.duration = end - previous.timestamp
                break

            previous.duration = event.timestamp - previous.timestamp
            if previous.duration <= timedelta(0):
                normalized.pop()
                continue

            normalized.append(event)
            break
        else:
            normalized.append(event)

    return normalized
