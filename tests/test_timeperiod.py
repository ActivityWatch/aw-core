import logging
import unittest
from datetime import datetime, timedelta

from aw_core import TimePeriod

logger = logging.getLogger("aw.core.transform")


class TimePeriodTest(unittest.TestCase):
    def test_intersection_start(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(minutes=10), now + timedelta(minutes=50))
        assert tp1.intersection(tp2).duration == timedelta(minutes=50)
        assert tp2.intersection(tp1).duration == timedelta(minutes=50)

    def test_intersection_end(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now + timedelta(minutes=10), now + timedelta(hours=1))
        assert tp1.intersection(tp2).duration == timedelta(minutes=50)
        assert tp2.intersection(tp1).duration == timedelta(minutes=50)

    def test_intersection_entire(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(minutes=10), now + timedelta(minutes=70))
        assert tp1.intersection(tp2).duration == timedelta(minutes=60)
        assert tp2.intersection(tp1).duration == timedelta(minutes=60)

    def test_intersection_none(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(hours=1), now)
        assert tp1.intersection(tp2) is None
        assert tp2.intersection(tp1) is None

    def test_contains(self):
        now = datetime.now()
        tp1 = TimePeriod(now - timedelta(hours=1), now + timedelta(hours=1))
        tp2 = TimePeriod(now, now + timedelta(hours=1))
        assert tp1.contains(tp2)
        assert not tp2.contains(tp1)

    def test_overlaps(self):
        now = datetime.now()
        # If periods are just "touching", they should not count as overlap
        tp1 = TimePeriod(now - timedelta(hours=1), now)
        tp2 = TimePeriod(now, now + timedelta(hours=1))
        assert not tp1.overlaps(tp2)
        assert not tp2.overlaps(tp1)

        # If outer contains inner, or vice versa, they overlap
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(hours=1), now + timedelta(hours=2))
        assert tp1.overlaps(tp2)
        assert tp2.overlaps(tp1)

        # If start/end is contained in the other event, they overlap
        tp1 = TimePeriod(now, now + timedelta(hours=2))
        tp2 = TimePeriod(now - timedelta(hours=1), now + timedelta(hours=1))
        assert tp1.overlaps(tp2)
        assert tp2.overlaps(tp1)
