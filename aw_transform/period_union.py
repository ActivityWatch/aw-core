import logging
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)


def period_union(events1: List[Event], events2: List[Event]) -> List[Event]:
    """
    Creates union of 2 event lists removing duplicates
    Example:
      windowevents_and_notafk = period_union(windowevents, notafkevents)
    """

    events1 = sorted(events1, key=lambda e: (e.timestamp, e.duration))
    events2 = sorted(events2, key=lambda e: (e.timestamp, e.duration))
    events_union = []

    e1_i = 0
    e2_i = 0
    while e1_i < len(events1) and e2_i < len(events2):
        e1 = events1[e1_i]
        e2 = events2[e2_i]

        if e1 == e2:
            events_union.append(e1)
            e1_i += 1
            e2_i += 1
        else:
            if e1.timestamp < e2.timestamp:
                events_union.append(e1)
                e1_i += 1
            elif e1.timestamp > e2.timestamp:
                events_union.append(e2)
                e2_i += 1
            elif e1.duration < e2.duration:
                events_union.append(e1)
                e1_i += 1
            else:
                events_union.append(e2)
                e2_i += 1

    if e1_i < len(events1):
        events_union.extend(events1[e1_i:])

    if e2_i < len(events2):
        events_union.extend(events2[e2_i:])

    return events_union
