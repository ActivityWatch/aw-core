from datetime import datetime, timedelta
from typing import Optional, Union


class TimePeriod:
    # Inspired by: http://www.codeproject.com/Articles/168662/Time-Period-Library-for-NET
    def __init__(self, start: datetime, end: datetime) -> None:
        self.start = start
        self.end = end

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def overlaps(self, other: "TimePeriod") -> bool:
        # Checks if this event is overlapping partially with another event
        return self.start < other.start < self.end \
            or self.start < other.end < self.end \
            or other.start < self.start and self.end < other.end

    def contains(self, other: Union[datetime, "TimePeriod"]) -> bool:
        # Checks if this event contains the entirety of another event
        if isinstance(other, TimePeriod):
            return self.start <= other.start and other.end <= self.end
        elif isinstance(other, datetime):
            return self.start <= other <= self.end
        else:
            raise ValueError("argument of invalid type '{}'".format(type(other)))

    def __contains__(self, other: Union[datetime, "TimePeriod"]) -> bool:
        return self.contains(other)

    def intersection(self, other: "TimePeriod") -> Optional["TimePeriod"]:
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
