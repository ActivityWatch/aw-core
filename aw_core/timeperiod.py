from datetime import datetime, timedelta
from typing import Optional, Union


class TimePeriod:
    # Inspired by: http://www.codeproject.com/Articles/168662/Time-Period-Library-for-NET
    # TODO: Move to its own package
    def __init__(self, start: datetime, end: datetime) -> None:
        # TODO: Introduce once tested in production (where negative duration events might occur)
        # if start > end:
        #     raise ValueError("TimePeriod cannot have negative duration, start '{}' came after end '{}'".format(start, end))
        self.start = start
        self.end = end

    @property
    def duration(self) -> timedelta:
        return self.end - self.start

    def overlaps(self, other: "TimePeriod") -> bool:
        """Checks if this timeperiod is overlapping partially or entirely with another timeperiod"""
        return (
            self.start <= other.start < self.end
            or self.start < other.end <= self.end
            or self in other
        )

    def intersects(self, other: "TimePeriod") -> bool:
        """Alias for overlaps"""
        return self.overlaps(other)

    def contains(self, other: Union[datetime, "TimePeriod"]) -> bool:
        """Checks if this timeperiod contains the entirety of another timeperiod or a datetime"""
        if isinstance(other, TimePeriod):
            return self.start <= other.start and other.end <= self.end
        elif isinstance(other, datetime):
            return self.start <= other <= self.end
        else:
            raise TypeError("argument of invalid type '{}'".format(type(other)))

    def __contains__(self, other: Union[datetime, "TimePeriod"]) -> bool:
        return self.contains(other)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TimePeriod):
            return self.start == other.start and self.end == other.end
        else:
            return False

    def __lt__(self, other: object) -> bool:
        # implemented to easily allow sorting of a list of timeperiods
        if isinstance(other, TimePeriod):
            return self.start < other.start
        else:
            raise TypeError(
                "operator not supported between instaces of '{}' and '{}'".format(
                    type(self), type(other)
                )
            )

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
        if self.end < other.start:
            return TimePeriod(self.end, other.start)
        elif other.end < self.start:
            return TimePeriod(other.end, self.start)
        else:
            return None

    def union(self, other: "TimePeriod") -> "TimePeriod":
        if not self.gap(other):
            return TimePeriod(min(self.start, other.start), max(self.end, other.end))
        else:
            raise Exception("TimePeriods must not have a gap if they are to be unioned")
