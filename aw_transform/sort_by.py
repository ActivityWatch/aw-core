import logging
from datetime import timedelta
from typing import List
from aw_core.models import Event

logger = logging.getLogger(__name__)


def sort_by_timestamp(events) -> List[Event]:
    """Sorts a list of events by timestamp"""
    return sorted(events, key=lambda e: e.timestamp)


def sort_by_duration(events) -> List[Event]:
    """Sorts a list of events by duration"""
    return sorted(events, key=lambda e: e.duration, reverse=True)


def limit_events(events, count) -> List[Event]:
    """Returns the ``count`` first events in the list of events"""
    return events[:count]


def sum_durations(events) -> timedelta:
    """Sums the durations for the given events"""
    return timedelta(seconds=(sum(event.duration.total_seconds() for event in events)))


def concat(events1, events2) -> List[Event]:
    """Concatenates two lists of events"""
    events = events1 + events2
    return events
