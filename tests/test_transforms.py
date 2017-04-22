import logging
import unittest
from datetime import datetime, timedelta, timezone

from aw_core.models import Event
from aw_core.transforms import full_chunk, label_chunk, filter_period_intersect, include_labels, exclude_labels, merge_queries


class ChunkTest(unittest.TestCase):
    # Tests the chunk transform

    def test_chunk_full(self):
        eventcount = 8
        events = []
        for i in range(eventcount):
            events.append(Event(label="test",
                                data={"key"+str(i%2): "val"+str(i%4)},
                                timestamp=datetime.now(timezone.utc) + timedelta(seconds=i),
                                duration=timedelta(seconds=1)))
        res = full_chunk(events)
        logging.debug(res)
        assert res['eventcount'] == eventcount
        assert res['duration'] == timedelta(seconds=eventcount)
        print(res)
        assert res['chunks']['test']['duration'] == timedelta(seconds=eventcount)
        assert res['chunks']['test']['data']['key0']['duration'] == timedelta(seconds=eventcount/2)
        assert res['chunks']['test']['data']['key0']['values']['val0']['duration'] == timedelta(seconds=eventcount/4)
        assert res['chunks']['test']['data']['key0']['values']['val2']['duration'] == timedelta(seconds=eventcount/4)
        assert res['chunks']['test']['duration'] == timedelta(seconds=eventcount)
        assert res['chunks']['test']['data']['key1']['duration'] == timedelta(seconds=eventcount/2)
        assert res['chunks']['test']['data']['key1']['values']['val1']['duration'] == timedelta(seconds=eventcount/4)
        assert res['chunks']['test']['data']['key1']['values']['val3']['duration'] == timedelta(seconds=eventcount/4)


    def test_chunk_label(self):
        eventcount = 8
        events = []
        for i in range(eventcount):
            events.append(Event(label="test",
                                timestamp=datetime.now(timezone.utc) + timedelta(seconds=i),
                                duration=timedelta(seconds=1)))
        res = label_chunk(events)
        logging.debug(res)
        assert res['eventcount'] == eventcount
        assert res['duration'] == timedelta(seconds=eventcount)
        print(res)
        assert res['chunks']['test']['duration'] == timedelta(seconds=eventcount)


class IncludeLabelsTest(unittest.TestCase):
    def test_include_labels(self):
        labels = ["a","c"]
        events = [
            Event(label="a"),
            Event(label="b"),
            Event(label="c"),
        ]
        included_labels = include_labels(events, labels)
        excluded_labels = exclude_labels(events, labels)
        assert len(included_labels) == 2
        assert len(excluded_labels) == 1


class FilterPeriodIntersectTest(unittest.TestCase):
    def test_filter_period_intersect(self):
        td1h = timedelta(hours=1)
        td30min = timedelta(minutes=30)
        now = datetime.now()

        # Filter 1h event with another 1h event at a 30min offset
        to_filter = [Event(label="lala", timestamp=now, duration=td1h)]
        filter_with = [Event(timestamp=now + timedelta(minutes=30), duration=td1h)]
        filtered_events = filter_period_intersect(to_filter, filter_with)
        assert filtered_events[0].duration == timedelta(minutes=30)

        # Filter 2x 30min events with a 15min gap with another 45min event in between intersecting both
        to_filter = [
            Event(label="lala", timestamp=now, duration=td30min),
            Event(label="lala", timestamp=now + timedelta(minutes=45), duration=td30min)
        ]
        filter_with = [Event(timestamp=now + timedelta(minutes=15), duration=timedelta(minutes=45))]
        filtered_events = filter_period_intersect(to_filter, filter_with)
        assert filtered_events[0].duration == timedelta(minutes=15)
        assert filtered_events[1].duration == timedelta(minutes=15)


class MergeQueriesTest(unittest.TestCase):
    def test_merge_queries_chunk_full(self):
        # This also excensively tests the merge_chunks transform
        now = datetime.now(timezone.utc)
        eventcount = 8
        events = []
        for i in range(eventcount):
            events.append(Event(label="test",
                                data={"key"+str(i%2): "val"+str(i%4)},
                                timestamp=now + timedelta(seconds=i),
                                duration=timedelta(seconds=1)))
        res1 = full_chunk(events)
        events.append(Event(label="test",
                            data={"key2": "val4"},
                            timestamp=now,
                            duration=timedelta(seconds=1)))
        events.append(Event(label="test2",
                            timestamp=now,
                            duration=timedelta(seconds=1)))
        res2 = full_chunk(events)
        print(res1)
        res_merged = merge_queries(res1, res2)
        print(res_merged)
        assert 18 == res_merged["eventcount"]
        assert timedelta(seconds=18) == res_merged["duration"]
        assert timedelta(seconds=4) == res_merged["chunks"]["test"]["data"]["key0"]["values"]["val0"]["duration"]
        assert timedelta(seconds=4) == res_merged["chunks"]["test"]["data"]["key1"]["values"]["val1"]["duration"]
        assert timedelta(seconds=4) == res_merged["chunks"]["test"]["data"]["key0"]["values"]["val2"]["duration"]
        assert timedelta(seconds=4) == res_merged["chunks"]["test"]["data"]["key1"]["values"]["val3"]["duration"]
        assert timedelta(seconds=1) == res_merged["chunks"]["test"]["data"]["key2"]["values"]["val4"]["duration"]
        assert timedelta(seconds=1) == res_merged["chunks"]["test2"]["duration"]

    def test_merge_queries_list(self):
        eventcount = 8
        events = []
        for i in range(eventcount):
            events.append(Event(label="test",
                                data={"key": "val"},
                                timestamp=datetime.now(timezone.utc) + timedelta(seconds=i),
                                duration=timedelta(seconds=1)))
        res1 = {
            "duration": {"unit":"s", "value": eventcount},
            "eventlist": events,
            "eventcount": eventcount
        }
        res2 = res1
        res_merged = merge_queries(res1, res2)
        assert 16 == res_merged["eventcount"]
        assert timedelta(seconds=16) == res_merged["duration"]
        assert 16 == len(res_merged["eventlist"])
        assert timedelta(seconds=1) == res_merged["eventlist"][0]["duration"]
