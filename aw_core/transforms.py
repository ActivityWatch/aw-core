import logging
from datetime import datetime, timedelta
from typing import List, Any

from aw_core.models import Event
from aw_core import TimePeriod

logger = logging.getLogger("aw.core.transform")


def get_event_interval(event: Event) -> TimePeriod:
    start = event["timestamp"][0]
    end = start + event["duration"][0]
    return TimePeriod(start, end)


def filter_afk_events(events, afkevents):
    # TODO: Generalize for filtering with other events than afkevents
    # https://stackoverflow.com/questions/325933/determine-whether-two-date-ranges-overlap
    afkevents = sorted(afkevents, key=lambda e: e["timestamp"][0])
    filtered_events = []

    e_i = 0
    a_i = 0
    while e_i < len(events) and a_i < len(afkevents):
        event = events[e_i]
        afkevent = afkevents[a_i]
        e_interval = get_event_interval(event)
        a_interval = get_event_interval(afkevent)

        if e_interval.is_within(a_interval):
            filtered_events.append(event)

        raise NotImplementedError


"""
def test_filter_afk_events():
    # Entire event overlaps
    events = [Event(timestamp=datetime.now(), duration=timedelta(hours=1))]
    afkevents = [Event(timestamp=datetime.now() + timedelta(minutes=10), duration=timedelta(minutes=30))]
    print(filter_afk_events(events, afkevents))

    # Event doesn't overlap
    events = [Event(timestamp=datetime.now(), duration=timedelta(hours=1))]
    afkevents = [Event(timestamp=datetime.now() + timedelta(hours=2), duration=timedelta(hours=1))]
    print(filter_afk_events(events, afkevents))
"""


def chunk(events: List[Event]) -> dict:
    eventcount = 0
    chunk = dict()  # type: Dict[str, Any]
    for event in events:
        if "label" in event:
            eventcount += 1
            for label in event["label"]:
                if label not in chunk:
                    chunk[label] = {"other_labels": []}
                for co_label in event["label"]:
                    if co_label != label and co_label not in chunk[label]["other_labels"]:
                        chunk[label]["other_labels"].append(co_label)
                if "duration" in event:
                    if "duration" not in chunk[label]:
                        chunk[label]["duration"] = event["duration"][0].copy()
                    else:
                        chunk[label]["duration"]["value"] += event["duration"][0]["value"]
    payload = {
        "eventcount": eventcount,
        "chunks": chunk,
    }
    return payload
