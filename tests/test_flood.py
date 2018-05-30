from datetime import datetime, timedelta

from aw_core.models import Event

from aw_transform import flood


now = datetime.now()
td1s = timedelta(seconds=1)


def test_flood_forwards():
    events = [
        Event(timestamp=now, duration=10),
        Event(timestamp=now + 15 * td1s, duration=5),
    ]
    flooded = flood(events)
    assert (flooded[0].timestamp + flooded[0].duration) - flooded[1].timestamp == timedelta(0)


def test_flood_backwards():
    events = [
        Event(timestamp=now, duration=5),
        Event(timestamp=now + 10 * td1s, duration=10),
    ]
    flooded = flood(events)
    assert (flooded[0].timestamp + flooded[0].duration) - flooded[1].timestamp == timedelta(0)
