import logging
from copy import deepcopy
from datetime import timedelta
from typing import List, Optional, Tuple

from aw_core.models import Event
from timeslot import Timeslot

from .filter_period_intersect import _get_event_period

logger = logging.getLogger(__name__)


def merge_subwatcher_fields(
    base_events: List[Event],
    subwatcher_events: List[Event],
    keys: List[str],
    conflict: str = "base_wins",
) -> List[Event]:
    """
    Split each event in *base_events* on overlapping subwatcher boundaries and
    copy the named *keys* from the matching subwatcher event into each segment's
    ``data`` dict.

    Unlike the ``concat`` workaround, this does not fabricate extra duration.
    App/title/duration aggregations stay correct because the output still covers
    exactly the same total time as *base_events*; only the segmentation changes
    where subwatcher fields actually change.

    This is the backend primitive that lets every client (webui, native UIs,
    exporters) categorize by subwatcher fields (browser ``url``/``$domain``;
    editor ``project``/``file``/``language``) without bespoke per-watcher
    client-side code.

    Args:
        base_events: The canonical window/afk-filtered stream to enrich.
        subwatcher_events: Events from a subwatcher bucket (e.g. aw-watcher-vim,
            aw-watcher-web).  Should already be clipped via
            ``filter_period_intersect`` before passing here.
        keys: Which keys to copy from the subwatcher event into the base event.
            Keys already present in the base event are left untouched when
            ``conflict="base_wins"`` (default).
        conflict: ``"base_wins"`` (default) — base event's existing keys are
            never overwritten; subwatcher fields are purely additive.
            ``"sub_wins"`` — subwatcher fields overwrite base fields.

    Returns:
        A new list of base event segments with subwatcher fields injected.
        Events in *base_events* that have no overlapping subwatcher event are
        returned with their original data unchanged.

    Example::

        window_events = query_bucket(bid_window)
        editor_events = flood(query_bucket(bid_editor))
        editor_events = filter_period_intersect(editor_events, window_events)
        window_events = merge_subwatcher_fields(
            window_events, editor_events, ["project", "file", "language"]
        )
        # Now categorize(window_events, ...) can match on "project"/"file"

    Note on overlap:
        Base events are split at the clipped subwatcher boundaries so each
        output segment only carries the subwatcher fields that were actually
        present during that slice of time. If multiple subwatcher events cover
        the same slice, the most recent one wins so a later transition does not
        get smeared backward by an older, longer pulse.
    """
    if conflict not in ("base_wins", "sub_wins"):
        raise ValueError(
            f"conflict must be 'base_wins' or 'sub_wins', got {conflict!r}"
        )
    if not subwatcher_events or not keys:
        return [deepcopy(e) for e in base_events]

    # Build a sorted copy so we can do a linear scan
    sub_sorted = sorted(subwatcher_events, key=lambda e: e.timestamp)

    result: List[Event] = []
    for base in base_events:
        base_period = _get_event_period(base)
        base_is_instant = base_period.duration == timedelta(0)
        overlapping: List[Tuple[Event, Timeslot]] = []
        boundaries = {base_period.start, base_period.end}

        for sub in sub_sorted:
            sub_period = _get_event_period(sub)
            # Once sub starts after base ends we can stop
            if sub_period.start >= base_period.end and not base_is_instant:
                break
            # Skip sub events that end before base starts
            if sub_period.end <= base_period.start:
                continue
            ip = base_period.intersection(sub_period)
            if not ip:
                continue
            if base_is_instant:
                # An instantaneous base needs whichever sub covers that
                # instant, even though the intersection itself is zero-length.
                overlapping.append((sub, sub_period))
            elif ip.duration > timedelta(0):
                # Zero-duration intersections on a non-instant base mean a
                # sub only touched a single boundary point (e.g. instantaneous
                # sub event). That doesn't represent a slice where the sub was
                # actually active, so it must not introduce a split or color
                # any segment.
                overlapping.append((sub, sub_period))
                boundaries.add(ip.start)
                boundaries.add(ip.end)

        if not overlapping:
            result.append(deepcopy(base))
            continue

        # A zero-duration base event has no slice for boundaries to split,
        # but it would otherwise be silently dropped by the segment loop
        # below (boundary_points has a single element so the zip is empty).
        # Preserve it as a single enriched event from whichever overlapping
        # sub covers the instant, using the same "latest sub wins" rule.
        if base_is_instant:
            instant_best_sub, instant_best_period = overlapping[0]
            for sub, sub_period in overlapping[1:]:
                if sub.timestamp > instant_best_sub.timestamp or (
                    sub.timestamp == instant_best_sub.timestamp
                    and sub_period.end > instant_best_period.end
                ):
                    instant_best_sub = sub
                    instant_best_period = sub_period
            enriched = deepcopy(base)
            for key in keys:
                if key in instant_best_sub.data:
                    if conflict == "base_wins" and key in enriched.data:
                        continue
                    enriched.data[key] = deepcopy(instant_best_sub.data[key])
            result.append(enriched)
            continue

        boundary_points = sorted(boundaries)
        base_segments: List[Event] = []
        for start, end in zip(boundary_points, boundary_points[1:]):
            segment_period = Timeslot(start, end)
            best_sub: Optional[Event] = None
            best_sub_period: Optional[Timeslot] = None

            for sub, sub_period in overlapping:
                seg_ip = segment_period.intersection(sub_period)
                # Skip subs that only touch this segment at a single point;
                # they don't actually cover any of its duration.
                if not seg_ip or seg_ip.duration == timedelta(0):
                    continue

                # Later subwatcher events should supersede older overlapping
                # pulses on the shared slice instead of letting stale data linger.
                if (
                    best_sub is None
                    or sub.timestamp > best_sub.timestamp
                    or (
                        sub.timestamp == best_sub.timestamp
                        and best_sub_period is not None
                        and sub_period.end > best_sub_period.end
                    )
                ):
                    best_sub = sub
                    best_sub_period = sub_period

            enriched = deepcopy(base)
            enriched.timestamp = start
            enriched.duration = end - start

            if best_sub is not None:
                for key in keys:
                    if key in best_sub.data:
                        if conflict == "base_wins" and key in enriched.data:
                            continue
                        enriched.data[key] = deepcopy(best_sub.data[key])

            if (
                base_segments
                and base_segments[-1].timestamp + base_segments[-1].duration
                == enriched.timestamp
                and base_segments[-1].data == enriched.data
            ):
                base_segments[-1].duration += enriched.duration
            else:
                base_segments.append(enriched)

        result.extend(base_segments)

    return result
