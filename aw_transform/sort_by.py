import logging
from datetime import datetime, timedelta
from typing import List
from aw_core.models import Event

logger = logging.getLogger(__name__)


def sort_by_timestamp(events) -> List[Event]:
    return sorted(events, key=lambda e: e.timestamp)

def sort_by_duration(events) -> List[Event]:
    return sorted(events, key=lambda e: e.duration, reverse=True)

def limit_events(events, count) -> List[Event]:
    return events[:count]

def sum_durations(events) -> timedelta:
    return timedelta(seconds=(sum(event.duration.total_seconds() for event in events)))

def sum_event_lists(events1, events2) -> List[Event]:
    events = events1 + events2
    events = sorted(events, key=lambda e: e.timestamp)
    return events