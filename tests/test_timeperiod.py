import logging
import unittest
from datetime import datetime, timedelta

from nose.tools import assert_equal

from aw_core import TimePeriod

logger = logging.getLogger("aw.core.transform")


class TimePeriodTest(unittest.TestCase):
    def test_intersection_start(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(minutes=10), now + timedelta(minutes=50))
        assert_equal(tp1.intersection(tp2).duration, timedelta(minutes=50))
        assert_equal(tp2.intersection(tp1).duration, timedelta(minutes=50))

    def test_intersection_end(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now + timedelta(minutes=10), now + timedelta(hours=1))
        assert_equal(tp1.intersection(tp2).duration, timedelta(minutes=50))
        assert_equal(tp2.intersection(tp1).duration, timedelta(minutes=50))

    def test_intersection_entire(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(minutes=10), now + timedelta(minutes=70))
        assert_equal(tp1.intersection(tp2).duration, timedelta(minutes=60))
        assert_equal(tp2.intersection(tp1).duration, timedelta(minutes=60))

    def test_intersection_none(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(hours=1), now)
        assert_equal(tp1.intersection(tp2), None)
        assert_equal(tp2.intersection(tp1), None)

    def test_contains(self):
        now = datetime.now()
        tp1 = TimePeriod(now - timedelta(hours=1), now + timedelta(hours=1))
        tp2 = TimePeriod(now, now + timedelta(hours=1))
        assert tp1.contains(tp2)
        assert not tp2.contains(tp1)

    def test_overlaps(self):
        now = datetime.now()
        tp1 = TimePeriod(now, now + timedelta(hours=1))
        tp2 = TimePeriod(now - timedelta(hours=1), now + timedelta(hours=1))
        assert tp1.overlaps(tp2)
        assert tp2.overlaps(tp1)
