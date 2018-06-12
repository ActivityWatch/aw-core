from datetime import datetime, timedelta

import pytest

from aw_core import TimePeriod


def test_intersection_start():
    now = datetime.now()
    tp1 = TimePeriod(now, now + timedelta(hours=1))
    tp2 = TimePeriod(now - timedelta(minutes=10), now + timedelta(minutes=50))
    assert tp1.intersection(tp2).duration == timedelta(minutes=50)
    assert tp2.intersection(tp1).duration == timedelta(minutes=50)


def test_intersection_end():
    now = datetime.now()
    tp1 = TimePeriod(now, now + timedelta(hours=1))
    tp2 = TimePeriod(now + timedelta(minutes=10), now + timedelta(hours=1))
    assert tp1.intersection(tp2).duration == timedelta(minutes=50)
    assert tp2.intersection(tp1).duration == timedelta(minutes=50)


def test_intersection_entire():
    now = datetime.now()
    tp1 = TimePeriod(now, now + timedelta(hours=1))
    tp2 = TimePeriod(now - timedelta(minutes=10), now + timedelta(minutes=70))
    assert tp1.intersection(tp2).duration == timedelta(minutes=60)
    assert tp2.intersection(tp1).duration == timedelta(minutes=60)


def test_intersection_none():
    now = datetime.now()
    tp1 = TimePeriod(now, now + timedelta(hours=1))
    tp2 = TimePeriod(now - timedelta(hours=1), now)
    assert tp1.intersection(tp2) is None
    assert tp2.intersection(tp1) is None


def test_contains():
    now = datetime.now()

    tp1 = TimePeriod(now - timedelta(hours=1), now + timedelta(hours=1))
    tp2 = TimePeriod(now, now + timedelta(hours=1))
    assert tp1.contains(tp2)
    assert not tp2.contains(tp1)

    # if datetime is contained in period
    assert now in tp1
    assert now in tp2

    # __contains__  operator overloading
    assert tp2 in tp1
    assert tp1 not in tp2

    with pytest.raises(ValueError):
        assert 0 in tp1


def test_overlaps():
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
