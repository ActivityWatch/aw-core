"""
Tests of aw-core transforms using hypothesis
"""

from datetime import datetime, timedelta

from aw_core.models import Event
from hypothesis import assume, given
from hypothesis import strategies as st


@st.composite
def events(draw, min_start=datetime.min):
    start = draw(st.datetimes(min_value=min_start, timezones=st.timezones()))
    duration = draw(st.timedeltas(min_value=timedelta(seconds=0)))
    title = draw(st.text())

    return Event(timestamp=start, duration=duration, data={"title": title})


@given(eventlist=st.lists(events(), min_size=1))
def test_sum_durations(eventlist):
    from aw_transform import sum_durations

    assert sum_durations(eventlist) == sum(
        [e.duration for e in eventlist], timedelta(seconds=0)
    )
