from pprint import pprint
from datetime import datetime, timedelta, timezone

import pytest
from aw_core.models import Event
from aw_transform import (
    filter_period_intersect,
    filter_keyvals_regex,
    filter_keyvals,
    merge_subwatcher_fields,
    period_union,
    sort_by_timestamp,
    sort_by_duration,
    sum_durations,
    merge_events_by_keys,
    chunk_events_by_key,
    split_url_events,
    simplify_string,
    union,
    union_no_overlap,
    categorize,
    tag,
    Rule,
)
from aw_transform.filter_period_intersect import _intersecting_eventpairs


def test_simplify_string():
    events = [
        Event(data={"label": "(99) Facebook"}),
        Event(data={"label": "(14) YouTube"}),
    ]
    assert simplify_string(events, "label")[0].data["label"] == "Facebook"
    assert simplify_string(events, "label")[1].data["label"] == "YouTube"

    events = [Event(data={"app": "Cemu.exe", "title": "Cemu - FPS: 133.7 - BotW"})]
    assert simplify_string(events, "title")[0].data["title"] == "Cemu - FPS: ... - BotW"

    events = [
        Event(data={"app": "VSCode.exe", "title": "● report.md - Visual Studio Code"})
    ]
    assert (
        simplify_string(events, "title")[0].data["title"]
        == "report.md - Visual Studio Code"
    )

    events = [Event(data={"app": "Gedit", "title": "*test.md - gedit"})]
    assert simplify_string(events, "title")[0].data["title"] == "test.md - gedit"


def test_filter_keyval():
    labels = ["aa", "cc"]
    events = [
        Event(data={"label": "aa"}),
        Event(data={"label": "bb"}),
        Event(data={"label": "cc"}),
    ]
    included_events = filter_keyvals(events, "label", labels)
    excluded_events = filter_keyvals(events, "label", labels, exclude=True)
    assert len(included_events) == 2
    assert len(excluded_events) == 1


def test_filter_keyval_regex():
    events = [
        Event(data={"label": "aa"}),
        Event(data={"label": "bb"}),
        Event(data={"label": "cc"}),
    ]
    events_re = filter_keyvals_regex(events, "label", "aa|cc")
    assert len(events_re) == 2


def test_filter_keyval_regex_keyerror():
    events = [
        Event(data={"label": "aa"}),
        Event(),
        Event(data={"label": "cc"}),
    ]
    events_re = filter_keyvals_regex(events, "label", "aa|cc")
    assert len(events_re) == 2


def test_intersecting_eventpairs():
    td1h = timedelta(hours=1)
    now = datetime.now()

    # Test with two identical lists
    e1 = [
        Event(timestamp=now, duration=td1h),
        Event(timestamp=now + td1h, duration=td1h),
    ]
    e2 = [
        Event(timestamp=now, duration=td1h),
        Event(timestamp=now + td1h, duration=td1h),
    ]
    intersecting = list(_intersecting_eventpairs(e1, e2))
    assert len(intersecting) == 2

    # Test with events in first list being in between events of second list
    e1 = [
        Event(timestamp=now + td1h, duration=td1h),
    ]
    e2 = [
        Event(timestamp=now, duration=td1h),
        Event(timestamp=now + 2 * td1h, duration=td1h),
    ]
    intersecting = list(_intersecting_eventpairs(e1, e2))
    assert not intersecting

    # Test with event in first list being identical to middle event in second list
    e1 = [
        Event(timestamp=now + td1h, duration=td1h),
    ]
    e2 = [
        Event(timestamp=now, duration=td1h),
        Event(timestamp=now + 1 * td1h, duration=td1h),
        Event(timestamp=now + 2 * td1h, duration=td1h),
    ]
    intersecting = list(_intersecting_eventpairs(e1, e2))
    assert len(intersecting) == 1

    # Test same as before, but reversed
    e1 = list(reversed(e1))
    e2 = list(reversed(e2))
    intersecting = list(_intersecting_eventpairs(e1, e2))
    assert len(intersecting) == 1


def test_filter_period_intersect():
    td1h = timedelta(hours=1)
    td30min = timedelta(minutes=30)
    now = datetime.now()

    # Filter 1h event with another 1h event at a 30min offset
    to_filter = [Event(timestamp=now, duration=td1h)]
    filter_with = [Event(timestamp=now + td30min, duration=td1h)]
    filtered_events = filter_period_intersect(to_filter, filter_with)
    assert filtered_events[0].duration == td30min

    # Filter 2x 30min events with a 15min gap with another 45min event in between intersecting both
    to_filter = [
        Event(timestamp=now, duration=td30min),
        Event(timestamp=now + timedelta(minutes=45), duration=td30min),
    ]
    filter_with = [
        Event(timestamp=now + timedelta(minutes=15), duration=timedelta(minutes=45))
    ]
    filtered_events = filter_period_intersect(to_filter, filter_with)
    assert len(filtered_events) == 2
    assert filtered_events[0].duration == timedelta(minutes=15)
    assert filtered_events[1].duration == timedelta(minutes=15)

    # Same as previous intersection, but reversing filter and to_filter events
    to_filter = [
        Event(timestamp=now + timedelta(minutes=15), duration=timedelta(minutes=45))
    ]
    filter_with = [
        Event(timestamp=now, duration=td30min),
        Event(timestamp=now + timedelta(minutes=45), duration=td30min),
    ]
    filtered_events = filter_period_intersect(to_filter, filter_with)
    assert len(filtered_events) == 2
    assert filtered_events[0].duration == timedelta(minutes=15)
    assert filtered_events[1].duration == timedelta(minutes=15)


def test_period_union():
    now = datetime.now(timezone.utc)

    # Events overlapping
    events1 = [Event(timestamp=now, duration=timedelta(seconds=10))]
    events2 = [
        Event(timestamp=now + timedelta(seconds=9), duration=timedelta(seconds=10))
    ]
    unioned_events = period_union(events1, events2)
    assert len(unioned_events) == 1

    # Events adjacent but not overlapping
    events1 = [Event(timestamp=now, duration=timedelta(seconds=10))]
    events2 = [
        Event(timestamp=now + timedelta(seconds=10), duration=timedelta(seconds=10))
    ]
    unioned_events = period_union(events1, events2)
    assert len(unioned_events) == 1

    # Events not overlapping or adjacent
    events1 = [Event(timestamp=now, duration=timedelta(seconds=10))]
    events2 = [
        Event(timestamp=now + timedelta(seconds=11), duration=timedelta(seconds=10))
    ]
    unioned_events = period_union(events1, events2)
    assert len(unioned_events) == 2


def test_sort_by_timestamp():
    now = datetime.now(timezone.utc)
    events = []
    events.append(
        Event(timestamp=now + timedelta(seconds=2), duration=timedelta(seconds=1))
    )
    events.append(
        Event(timestamp=now + timedelta(seconds=1), duration=timedelta(seconds=2))
    )
    events_sorted = sort_by_timestamp(events)
    assert events_sorted == events[::-1]


def test_sort_by_duration():
    now = datetime.now(timezone.utc)
    events = []
    events.append(
        Event(timestamp=now + timedelta(seconds=2), duration=timedelta(seconds=1))
    )
    events.append(
        Event(timestamp=now + timedelta(seconds=1), duration=timedelta(seconds=2))
    )
    events_sorted = sort_by_duration(events)
    assert events_sorted == events[::-1]


def test_sum_durations():
    now = datetime.now(timezone.utc)
    events = []
    for i in range(10):
        events.append(
            Event(timestamp=now + timedelta(seconds=i), duration=timedelta(seconds=1))
        )
    result = sum_durations(events)
    assert result == timedelta(seconds=10)


def test_merge_events_by_keys_1():
    now = datetime.now(timezone.utc)
    events = []
    e1_data = {"label": "a"}
    e2_data = {"label": "b"}
    e1 = Event(data=e1_data, timestamp=now, duration=timedelta(seconds=1))
    e2 = Event(data=e2_data, timestamp=now, duration=timedelta(seconds=1))
    events = events + [e1] * 10
    events = events + [e2] * 5

    # Check that an empty key list has no effect
    assert merge_events_by_keys(events, []) == events

    # Check that trying to merge on unavailable key has no effect
    assert len(merge_events_by_keys(events, ["unknown"])) == 1

    result = merge_events_by_keys(events, ["label"])
    result = sort_by_duration(result)
    print(result)
    print(len(result))
    assert len(result) == 2
    assert result[0].duration == timedelta(seconds=10)
    assert result[1].duration == timedelta(seconds=5)


def test_merge_events_by_keys_2():
    now = datetime.now(timezone.utc)
    events = []
    e1_data = {"k1": "a", "k2": "a"}
    e2_data = {"k1": "a", "k2": "c"}
    e3_data = {"k1": "b", "k2": "a"}
    e1 = Event(data=e1_data, timestamp=now, duration=timedelta(seconds=1))
    e2 = Event(data=e2_data, timestamp=now, duration=timedelta(seconds=1))
    e3 = Event(data=e3_data, timestamp=now, duration=timedelta(seconds=1))
    events = events + [e1] * 10
    events = events + [e2] * 9
    events = events + [e3] * 8
    result = merge_events_by_keys(events, ["k1", "k2"])
    result = sort_by_duration(result)
    print(result)
    print(len(result))
    assert len(result) == 3
    assert result[0].data == e1_data
    assert result[0].duration == timedelta(seconds=10)
    assert result[1].data == e2_data
    assert result[1].duration == timedelta(seconds=9)
    assert result[2].data == e3_data
    assert result[2].duration == timedelta(seconds=8)


def test_chunk_events_by_key():
    now = datetime.now(timezone.utc)
    events = []
    e1_data = {"label1": "1a", "label2": "2a"}
    e2_data = {"label1": "1a", "label2": "2b"}
    e3_data = {"label1": "1b", "label2": "2b"}
    e1 = Event(data=e1_data, timestamp=now, duration=timedelta(seconds=1))
    e2 = Event(data=e2_data, timestamp=now, duration=timedelta(seconds=1))
    e3 = Event(data=e3_data, timestamp=now, duration=timedelta(seconds=1))
    events = [e1, e2, e3]
    result = chunk_events_by_key(events, "label1")
    print(len(result))
    pprint(result)
    assert len(result) == 2
    # Check root label
    assert result[0].data["label1"] == "1a"
    assert result[1].data["label1"] == "1b"
    # Check timestamp
    assert result[0].timestamp == e1.timestamp
    assert result[1].timestamp == e3.timestamp
    # Check duration
    assert result[0].duration == e1.duration + e2.duration
    assert result[1].duration == e3.duration
    # Check subevents
    assert result[0].data["subevents"][0] == e1
    assert result[0].data["subevents"][1] == e2
    assert result[1].data["subevents"][0] == e3


def test_url_parse_event():
    now = datetime.now(timezone.utc)
    e = Event(
        data={"url": "http://asd.com/test/?a=1"},
        timestamp=now,
        duration=timedelta(seconds=1),
    )
    result = split_url_events([e])
    print(result)
    assert result[0].data["$protocol"] == "http"
    assert result[0].data["$domain"] == "asd.com"
    assert result[0].data["$path"] == "/test/"
    assert result[0].data["$params"] == ""
    assert result[0].data["$options"] == "a=1"
    assert result[0].data["$identifier"] == ""

    e2 = Event(
        data={"url": "https://www.asd.asd.com/test/test2/meh;meh2?asd=2&asdf=3#id"},
        timestamp=now,
        duration=timedelta(seconds=1),
    )
    result = split_url_events([e2])
    print(result)
    assert result[0].data["$protocol"] == "https"
    assert result[0].data["$domain"] == "asd.asd.com"
    assert result[0].data["$path"] == "/test/test2/meh"
    assert result[0].data["$params"] == "meh2"
    assert result[0].data["$options"] == "asd=2&asdf=3"
    assert result[0].data["$identifier"] == "id"

    e3 = Event(
        data={"url": "file:///home/johan/myfile.txt"},
        timestamp=now,
        duration=timedelta(seconds=1),
    )
    result = split_url_events([e3])
    print(result)
    assert result[0].data["$protocol"] == "file"
    assert result[0].data["$domain"] == "file"
    assert result[0].data["$path"] == "/home/johan/myfile.txt"
    assert result[0].data["$params"] == ""
    assert result[0].data["$options"] == ""
    assert result[0].data["$identifier"] == ""

    # Test about: URLs
    e4 = Event(
        data={"url": "about:blank"},
        timestamp=now,
        duration=timedelta(seconds=1),
    )
    result = split_url_events([e4])
    assert result[0].data["$protocol"] == "about"
    assert result[0].data["$domain"] == "about"


def test_union():
    now = datetime.now(timezone.utc)

    e1 = Event(timestamp=now - timedelta(seconds=20), duration=timedelta(seconds=5))
    e2 = Event(timestamp=now - timedelta(seconds=10), duration=timedelta(seconds=5))
    e3 = Event(timestamp=now, duration=timedelta(seconds=1))
    e4 = Event(timestamp=now + timedelta(seconds=20), duration=timedelta(seconds=1))

    # union separate event lists with duplicates
    events_union = union([e1, e2, e4], [e2, e3])
    assert events_union == [e1, e2, e3, e4]

    e1 = Event(timestamp=now - timedelta(seconds=20), duration=timedelta(seconds=5))
    e2 = Event(timestamp=now - timedelta(seconds=10), duration=timedelta(seconds=5))
    e3 = Event(timestamp=now - timedelta(seconds=10), duration=timedelta(seconds=10))
    e4 = Event(timestamp=now - timedelta(seconds=5), duration=timedelta(seconds=5))
    e5 = Event(timestamp=now, duration=timedelta(seconds=10))

    # union event lists with intersecting duplicates
    events_union = union([e3, e2, e5], [e1, e3, e4, e5])
    assert events_union == [e1, e2, e3, e4, e5]

    e1 = Event(timestamp=now - timedelta(seconds=30), duration=timedelta(seconds=15))
    e2 = Event(timestamp=now, duration=timedelta(seconds=3))
    e3 = Event(timestamp=now, duration=timedelta(seconds=5))
    e4 = Event(timestamp=now, duration=timedelta(seconds=10))

    # union event lists with same timestamp but different duration duplicates
    events_union = union([e1, e2, e4], [e3, e2, e1])
    assert events_union == [e1, e2, e3, e4]


def test_categorize():
    now = datetime.now(timezone.utc)

    classes = [
        (["Test"], Rule({"regex": "^just"})),
        (["Test", "Subtest"], Rule({"regex": "subtest$"})),
        (["Test", "Ignorecase"], Rule({"regex": "ignorecase", "ignore_case": True})),
    ]
    events = [
        Event(timestamp=now, duration=0, data={"key": "just a test"}),
        Event(timestamp=now, duration=0, data={"key": "just a subtest"}),
        Event(timestamp=now, duration=0, data={"key": "just a IGNORECASE test"}),
        Event(timestamp=now, duration=0, data={}),
    ]
    events = categorize(events, classes)

    assert events[0].data["$category"] == ["Test"]
    assert events[1].data["$category"] == ["Test", "Subtest"]
    assert events[2].data["$category"] == ["Test", "Ignorecase"]
    assert events[3].data["$category"] == ["Uncategorized"]


def _event(value: str = "just a test") -> Event:
    return Event(timestamp=datetime.now(timezone.utc), duration=0, data={"key": value})


def test_categorize_depth_wins_without_priority():
    events = categorize(
        [_event()],
        [
            (["A"], Rule({"regex": "test"})),
            (["B", "B1"], Rule({"regex": "test"})),
        ],
    )
    assert events[0].data["$category"] == ["B", "B1"]


def test_categorize_explicit_priority_overrides_depth():
    # Default for B1 is depth 2 → 20; 25 beats it.
    events = categorize(
        [_event()],
        [
            (["A"], Rule({"regex": "test", "priority": 25})),
            (["B", "B1"], Rule({"regex": "test"})),
        ],
    )
    assert events[0].data["$category"] == ["A"]


def test_categorize_weight_alias():
    events = categorize(
        [_event()],
        [
            (["A"], Rule({"regex": "test", "weight": 25})),
            (["B", "B1"], Rule({"regex": "test"})),
        ],
    )
    assert events[0].data["$category"] == ["A"]


def test_categorize_inter_level_priority():
    between = categorize(
        [_event()],
        [
            (["A"], Rule({"regex": "test"})),
            (["A2"], Rule({"regex": "test", "priority": 15})),
        ],
    )
    assert between[0].data["$category"] == ["A2"]

    still_loses_to_deeper = categorize(
        [_event()],
        [
            (["A2"], Rule({"regex": "test", "priority": 15})),
            (["B", "B1"], Rule({"regex": "test"})),
        ],
    )
    assert still_loses_to_deeper[0].data["$category"] == ["B", "B1"]


def test_categorize_lower_priority_loses_to_default_depth():
    events = categorize(
        [_event()],
        [
            (["A"], Rule({"regex": "test"})),
            (["B", "B1"], Rule({"regex": "test", "priority": 0})),
        ],
    )
    assert events[0].data["$category"] == ["A"]


def test_categorize_equal_priority_keeps_later_match():
    events = categorize(
        [_event()],
        [
            (["First"], Rule({"regex": "test", "priority": 5})),
            (["Second"], Rule({"regex": "test", "priority": 5})),
        ],
    )
    assert events[0].data["$category"] == ["Second"]


def test_categorize_negative_priority_still_beats_uncategorized():
    events = categorize(
        [_event()],
        [(["Low"], Rule({"regex": "test", "priority": -100}))],
    )
    assert events[0].data["$category"] == ["Low"]


def test_categorize_priority_below_i64_min_still_beats_uncategorized():
    # Python ints are unbounded; a signed-64-bit fallback sentinel would
    # incorrectly keep Uncategorized for values below -(2**63).
    events = categorize(
        [_event()],
        [(["Low"], Rule({"regex": "test", "priority": -(2**63) - 1}))],
    )
    assert events[0].data["$category"] == ["Low"]


def test_categorize_empty_category_keeps_uncategorized():
    events = categorize(
        [_event()],
        [([], Rule({"regex": "test"}))],
    )
    assert events[0].data["$category"] == ["Uncategorized"]


def test_rule_invalid_priority():
    with pytest.raises(ValueError, match="integer"):
        Rule({"regex": "test", "priority": 1.5})
    with pytest.raises(ValueError, match="integer"):
        Rule({"regex": "test", "priority": "high"})
    with pytest.raises(ValueError, match="integer"):
        Rule({"regex": "test", "priority": True})


def test_categorize_cache_correctness():
    """Cache reuses category for identical data; distinct data gets its own category."""
    now = datetime.now(timezone.utc)

    classes = [
        (["Browser"], Rule({"regex": "Firefox"})),
        (["Editor"], Rule({"regex": "vim"})),
    ]
    firefox_data = {"app": "Firefox", "title": "Home"}
    vim_data = {"app": "vim", "title": "classify.py"}

    # 50 Firefox events, 1 vim event, 50 more Firefox events
    events = (
        [Event(timestamp=now, duration=0, data=dict(firefox_data)) for _ in range(50)]
        + [Event(timestamp=now, duration=0, data=dict(vim_data))]
        + [Event(timestamp=now, duration=0, data=dict(firefox_data)) for _ in range(50)]
    )
    result = categorize(events, classes)

    for e in result[:50]:
        assert e.data["$category"] == ["Browser"]
    assert result[50].data["$category"] == ["Editor"]
    for e in result[51:]:
        assert e.data["$category"] == ["Browser"]

    # Mutating one event's category must not affect others sharing the same data fingerprint
    result[0].data["$category"].append("MUTATED")
    assert result[1].data["$category"] == ["Browser"]


def test_tags():
    now = datetime.now(timezone.utc)

    classes = [
        ("Test", Rule({"regex": "value$"})),
        ("Test", Rule({"regex": "^just"})),
    ]
    events = [
        Event(timestamp=now, duration=0, data={"key": "just a test value"}),
        Event(timestamp=now, duration=0, data={}),
    ]
    events = tag(events, classes)

    assert len(events[0].data["$tags"]) == 2
    assert len(events[1].data["$tags"]) == 0


def test_union_no_overlap():
    from pprint import pprint

    now = datetime(2018, 1, 1, 0, 0)
    td1h = timedelta(hours=1)
    events1 = [
        Event(timestamp=now + 2 * i * td1h, duration=td1h, data={"test": 1})
        for i in range(3)
    ]
    events2 = [
        Event(timestamp=now + (2 * i + 0.5) * td1h, duration=td1h, data={"test": 2})
        for i in range(3)
    ]

    events_union = union_no_overlap(events1, events2)
    # pprint(events_union)
    dur = sum((e.duration for e in events_union), timedelta(0))
    assert dur == timedelta(hours=4, minutes=30)
    assert sorted(events_union, key=lambda e: e.timestamp)

    events_union = union_no_overlap(events2, events1)
    # pprint(events_union)
    dur = sum((e.duration for e in events_union), timedelta(0))
    assert dur == timedelta(hours=4, minutes=30)
    assert sorted(events_union, key=lambda e: e.timestamp)

    events1 = [
        Event(timestamp=now + (2 * i) * td1h, duration=td1h, data={"test": 1})
        for i in range(3)
    ]
    events2 = [Event(timestamp=now, duration=5 * td1h, data={"test": 2})]
    events_union = union_no_overlap(events1, events2)
    pprint(events_union)
    dur = sum((e.duration for e in events_union), timedelta(0))
    assert dur == timedelta(hours=5, minutes=0)
    assert sorted(events_union, key=lambda e: e.timestamp)


def test_merge_subwatcher_fields_basic():
    """Fully overlapping subwatcher fields are injected without changing duration."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td1h = timedelta(hours=1)

    base = [
        Event(
            timestamp=now,
            duration=td1h,
            data={"app": "vim", "title": "file.py"},
        )
    ]
    sub = [
        Event(
            timestamp=now,
            duration=td1h,
            data={"project": "myproject", "file": "file.py", "language": "python"},
        )
    ]
    result = merge_subwatcher_fields(base, sub, ["project", "file", "language"])

    assert len(result) == 1
    # Original base fields preserved
    assert result[0].data["app"] == "vim"
    assert result[0].data["title"] == "file.py"
    # Subwatcher fields injected
    assert result[0].data["project"] == "myproject"
    assert result[0].data["language"] == "python"
    # Exact overlap means no extra segmentation
    assert result[0].timestamp == now
    assert result[0].duration == td1h


def test_merge_subwatcher_fields_partial_overlap_splits_base():
    """Partial overlap only enriches the covered slice of the base event."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td15m = timedelta(minutes=15)
    td30m = timedelta(minutes=30)
    td1h = timedelta(hours=1)

    base = [Event(timestamp=now, duration=td1h, data={"app": "vim"})]
    sub = [
        Event(
            timestamp=now + td15m,
            duration=td30m,
            data={"project": "myproject"},
        )
    ]

    result = merge_subwatcher_fields(base, sub, ["project"])

    assert len(result) == 3
    assert [event.duration for event in result] == [td15m, td30m, td15m]
    assert [event.timestamp for event in result] == [
        now,
        now + td15m,
        now + td15m + td30m,
    ]
    assert "project" not in result[0].data
    assert result[1].data["project"] == "myproject"
    assert "project" not in result[2].data
    assert sum_durations(result) == td1h


def test_merge_subwatcher_fields_overlapping_subwatchers_prefer_latest_start():
    """Newer overlapping subwatcher events take over from their own start time."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td20m = timedelta(minutes=20)
    td40m = timedelta(minutes=40)
    td1h = timedelta(hours=1)

    base = [Event(timestamp=now, duration=td1h, data={"app": "vim"})]
    sub = [
        Event(timestamp=now, duration=td40m, data={"project": "alpha"}),
        Event(timestamp=now + td20m, duration=td40m, data={"project": "beta"}),
    ]

    result = merge_subwatcher_fields(base, sub, ["project"])

    assert len(result) == 2
    assert [event.timestamp for event in result] == [now, now + td20m]
    assert [event.duration for event in result] == [td20m, td40m]
    assert [event.data.get("project") for event in result] == ["alpha", "beta"]

    by_project = {
        event.data.get("project"): event.duration
        for event in merge_events_by_keys(result, ["project"])
    }
    assert by_project["alpha"] == td20m
    assert by_project["beta"] == td40m
    assert sum_durations(result) == td1h


def test_merge_subwatcher_fields_no_overlap():
    """Base events with no overlapping subwatcher event are returned unchanged."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td1h = timedelta(hours=1)

    base = [Event(timestamp=now, duration=td1h, data={"app": "vim"})]
    # Subwatcher event is entirely after the base event
    sub = [
        Event(
            timestamp=now + 2 * td1h,
            duration=td1h,
            data={"project": "other"},
        )
    ]
    result = merge_subwatcher_fields(base, sub, ["project"])

    assert len(result) == 1
    assert "project" not in result[0].data
    assert result[0].data["app"] == "vim"


def test_merge_subwatcher_fields_base_wins_conflict():
    """With conflict='base_wins' (default), existing base keys are not overwritten."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td1h = timedelta(hours=1)

    base = [Event(timestamp=now, duration=td1h, data={"app": "vim", "file": "base.py"})]
    sub = [Event(timestamp=now, duration=td1h, data={"file": "sub.py", "project": "p"})]

    result = merge_subwatcher_fields(
        base, sub, ["file", "project"], conflict="base_wins"
    )
    # base's "file" must not be overwritten
    assert result[0].data["file"] == "base.py"
    # "project" not in base → injected from sub
    assert result[0].data["project"] == "p"


def test_merge_subwatcher_fields_sub_wins_conflict():
    """With conflict='sub_wins', subwatcher fields overwrite base fields."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td1h = timedelta(hours=1)

    base = [Event(timestamp=now, duration=td1h, data={"app": "vim", "file": "base.py"})]
    sub = [Event(timestamp=now, duration=td1h, data={"file": "sub.py"})]

    result = merge_subwatcher_fields(base, sub, ["file"], conflict="sub_wins")
    assert result[0].data["file"] == "sub.py"


def test_merge_subwatcher_fields_multiple_subsegments_preserve_duration():
    """Repeated subwatcher values aggregate to their true covered duration."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td15m = timedelta(minutes=15)
    td1h = timedelta(hours=1)

    base = [Event(timestamp=now, duration=td1h, data={"app": "vim"})]
    sub = [
        Event(timestamp=now, duration=td15m, data={"project": "alpha"}),
        Event(timestamp=now + td15m, duration=td15m, data={"project": "beta"}),
        Event(
            timestamp=now + 2 * td15m,
            duration=td15m,
            data={"project": "alpha"},
        ),
    ]

    result = merge_subwatcher_fields(base, sub, ["project"])

    by_project = {
        event.data.get("project"): event.duration
        for event in merge_events_by_keys(result, ["project"])
    }
    by_app = {
        event.data.get("app"): event.duration
        for event in merge_events_by_keys(result, ["app"])
    }

    assert by_project["alpha"] == 2 * td15m
    assert by_project["beta"] == td15m
    assert by_project[None] == td15m
    assert by_app["vim"] == td1h
    assert sum_durations(result) == td1h


def test_merge_subwatcher_fields_empty_inputs():
    """Empty sub or keys returns a defensive copy of base (not the same list)."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td1h = timedelta(hours=1)
    base = [Event(timestamp=now, duration=td1h, data={"app": "vim"})]

    # Empty subwatcher list — returns a new list, data unchanged
    result = merge_subwatcher_fields(base, [], ["project"])
    assert result[0].data == {"app": "vim"}
    assert result is not base

    # Empty keys list
    sub = [Event(timestamp=now, duration=td1h, data={"project": "p"})]
    result = merge_subwatcher_fields(base, sub, [])
    assert "project" not in result[0].data
    assert result is not base

    # Both empty
    result = merge_subwatcher_fields(base, [], [])
    assert result[0].data == {"app": "vim"}
    assert result is not base


def test_merge_subwatcher_fields_invalid_conflict():
    """Invalid conflict value raises ValueError immediately."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    td1h = timedelta(hours=1)
    base = [Event(timestamp=now, duration=td1h, data={"app": "vim"})]
    sub = [Event(timestamp=now, duration=td1h, data={"project": "p"})]

    with pytest.raises(ValueError, match="conflict must be"):
        merge_subwatcher_fields(base, sub, ["project"], conflict="invalid")
