from datetime import datetime, timedelta
from typing import Optional, Union


class TimePeriod:
    # Inspired by: http://www.codeproject.com/Articles/168662/Time-Period-Library-for-NET
    # TODO: Move to its own package
    def __init__(self, start: datetime, end: datetime) -> None:
        # assert start <= end
        self.start = start
        self.end = end

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def overlaps(self, other: "TimePeriod") -> bool:
        """Checks if this event is overlapping partially or entirely with another event"""
        return self.start < other.start < self.end \
            or self.start < other.end < self.end \
            or other.start < self.start and self.end < other.end \
            or self == other

    def contains(self, other: Union[datetime, "TimePeriod"]) -> bool:
        """Checks if this event contains the entirety of another event"""
        if isinstance(other, TimePeriod):
            return self.start <= other.start and other.end <= self.end
        elif isinstance(other, datetime):
            return self.start <= other <= self.end
        else:
            raise ValueError("argument of invalid type '{}'".format(type(other)))

    def __contains__(self, other: Union[datetime, "TimePeriod"]) -> bool:
        return self.contains(other)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TimePeriod):
            return self.start == other.start and self.end == other.end
        else:
            return False

    def intersection(self, other: "TimePeriod") -> Optional["TimePeriod"]:
        """Returns the timeperiod contained in both periods"""
        # https://stackoverflow.com/posts/3721426/revisions
        if self.contains(other):
            # Entirety of other is within self
            return other
        elif self.start <= other.start < self.end:
            # End part of self intersects
            return TimePeriod(other.start, self.end)
        elif self.start < other.end <= self.end:
            # Start part of self intersects
            return TimePeriod(self.start, other.end)
        elif other.contains(self):
            # Entirety of self is within other
            return self
        return None

    def adjacent(self, other: "TimePeriod") -> bool:
        """Iff timeperiods are exactly next to each other, return True."""
        return self.start == other.end or self.end == other.start

    def gap(self, other: "TimePeriod") -> Optional["TimePeriod"]:
        """If periods are separated by a non-zero gap, return the gap as a new timeperiod, else None"""
        if not (self.overlaps(other) or self.adjacent(other)):
            gap_start = min(self.end, other.end)
            gap_end = max(self.start, other.start)
            return TimePeriod(gap_start, gap_end)
        return None

    def union(self, other: "TimePeriod") -> "TimePeriod":
        if not self.gap(other):
            return TimePeriod(min(self.start, other.start), max(self.end, other.end))
        else:
            raise Exception("TimePeriods must be overlapping or adjacent to be unioned")
