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
from aw_transform.split_url_events import _serialize_ipv6


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


def test_filter_period_intersect_overlapping_filters():
    """Time covered by several overlapping filter events is only kept once."""
    now = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    td1s = timedelta(seconds=1)
    to_filter = [Event(timestamp=now, duration=10 * td1s)]

    filter_with = [
        Event(timestamp=now, duration=6 * td1s),
        Event(timestamp=now + 4 * td1s, duration=4 * td1s),
    ]
    filtered_events = filter_period_intersect(to_filter, filter_with)
    assert [(e.timestamp - now, e.duration) for e in filtered_events] == [
        (0 * td1s, 6 * td1s),
        (6 * td1s, 2 * td1s),
    ]

    # A filter event contained in an earlier one adds nothing, also when they
    # end at the same time
    for start, duration in [(2, 2), (2, 4)]:
        filter_with = [
            Event(timestamp=now, duration=6 * td1s),
            Event(timestamp=now + start * td1s, duration=duration * td1s),
        ]
        filtered_events = filter_period_intersect(to_filter, filter_with)
        assert [(e.timestamp - now, e.duration) for e in filtered_events] == [
            (0 * td1s, 6 * td1s),
        ]


def test_filter_period_intersect_zero_duration():
    """Zero-duration events within a filter event are kept, boundaries included."""
    now = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
    td1s = timedelta(seconds=1)
    filter_with = [Event(timestamp=now, duration=10 * td1s)]
    for offset, kept in [(-1, False), (0, True), (5, True), (10, True), (11, False)]:
        to_filter = [Event(timestamp=now + offset * td1s, duration=0)]
        assert filter_period_intersect(to_filter, filter_with) == (
            to_filter if kept else []
        )


def test_period_union_does_not_modify_inputs():
    """The input events keep their data (they're often shared with other variables)"""
    now = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)
    window = [
        Event(timestamp=now, duration=30, data={"app": "Slack"}),
        Event(timestamp=now + timedelta(minutes=5), duration=10, data={"app": "Code"}),
    ]
    not_afk = [Event(timestamp=now + timedelta(minutes=5), duration=5, data={})]
    slack = [e for e in window if e.data["app"] == "Slack"]  # same objects
    result = period_union(not_afk, slack)
    assert all(e.data == {} for e in result)
    assert [e.data for e in window] == [{"app": "Slack"}, {"app": "Code"}]
    assert filter_period_intersect(window, result)[0].data == {"app": "Slack"}


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

    # An empty key list merges nothing into nothing, like aw-server-rust
    assert merge_events_by_keys(events, []) == []

    # Events missing a key are dropped
    assert merge_events_by_keys(events, ["unknown"]) == []

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


def test_merge_events_by_keys_keeps_first_payload():
    """Like aw-server-rust: the first event's data is kept, missing keys are dropped"""
    now = datetime.now(timezone.utc)
    events = [
        Event(
            timestamp=now,
            duration=10,
            data={"app": "x", "title": "t1", "$category": ["Work"]},
        ),
        Event(
            timestamp=now + timedelta(seconds=10),
            duration=5,
            data={"app": "x", "title": "t2", "$category": ["Work"]},
        ),
        Event(
            timestamp=now + timedelta(seconds=20), duration=7, data={"title": "no app"}
        ),
    ]
    result = merge_events_by_keys(events, ["app"])
    assert len(result) == 1
    assert result[0].data == {"app": "x", "title": "t1", "$category": ["Work"]}
    assert result[0].timestamp == events[0].timestamp
    assert result[0].duration == timedelta(seconds=15)
    assert result[0].id is None

    # The merged event doesn't share mutable data with the input
    result[0].data["$category"].append("MUTATED")
    assert events[0].data["$category"] == ["Work"]

    # List values (like categories) can be merge keys, and 1 and 1.0 differ
    by_cat = merge_events_by_keys(events, ["$category"])
    assert [e.duration for e in by_cat] == [timedelta(seconds=15)]
    nums = [
        Event(timestamp=now, duration=1, data={"n": 1}),
        Event(timestamp=now, duration=1, data={"n": 1.0}),
    ]
    assert len(merge_events_by_keys(nums, ["n"])) == 2


def test_merge_events_by_keys_non_json_values_dont_collide():
    """A non-JSON value doesn't merge with a string that looks the same"""
    now = datetime.now(timezone.utc)
    events = [
        Event(timestamp=now, duration=1, data={"k": datetime(2020, 1, 1)}),
        Event(timestamp=now, duration=1, data={"k": "2020-01-01 00:00:00"}),
    ]
    assert len(merge_events_by_keys(events, ["k"])) == 2


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
    with pytest.warns(DeprecationWarning):
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


def _split(url):
    e = Event(data={"url": url}, timestamp=datetime.now(timezone.utc), duration=1)
    return split_url_events([e])[0].data


def test_url_parse_event():
    """Same fields and values as aw-server-rust (ActivityWatch/activitywatch#1466)"""
    assert _split("http://asd.com/test/?a=1") == {
        "url": "http://asd.com/test/?a=1",
        "$protocol": "http",
        "$domain": "asd.com",
        "$path": "/test/",
        "$params": "a=1",
    }

    data = _split("https://www.asd.asd.com/test/test2/meh;meh2?asd=2&asdf=3#id")
    assert data["$domain"] == "asd.asd.com"
    assert data["$path"] == "/test/test2/meh;meh2"
    assert data["$params"] == "asd=2&asdf=3"
    assert "$options" not in data and "$identifier" not in data

    # The port and userinfo aren't part of the domain
    data = _split("https://user:pw@x.org:8080/a;p?b=c#d")
    assert (data["$domain"], data["$path"], data["$params"]) == ("x.org", "/a;p", "b=c")
    assert _split("http://[::1]:5600/api")["$domain"] == "[::1]"
    assert _split("HTTPS://WWW.Example.COM")["$domain"] == "example.com"

    # Special schemes always have a path
    assert _split("https://x.org")["$path"] == "/"

    # No host: the scheme is the domain
    data = _split("file:///home/johan/myfile.txt")
    assert (data["$protocol"], data["$domain"]) == ("file", "file")
    assert data["$path"] == "/home/johan/myfile.txt"
    data = _split("about:blank")
    assert (data["$protocol"], data["$domain"], data["$path"]) == (
        "about",
        "about",
        "blank",
    )

    # Not an absolute URL, or not a string: left unchanged
    for url in ["not a url", "/relative/path", "http://"]:
        assert _split(url) == {"url": url}
    e = Event(data={"url": 5}, timestamp=datetime.now(timezone.utc), duration=1)
    assert split_url_events([e])[0].data == {"url": 5}


@pytest.mark.parametrize(
    "addr,expected",
    [
        ("::", "::"),
        ("::1", "::1"),
        ("1::", "1::"),
        # Hex pieces, not a dotted IPv4 tail as newer Pythons' ipaddress writes
        ("::ffff:1.2.3.4", "::ffff:102:304"),
        ("::1.2.3.4", "::102:304"),
        # First of the longest zero runs; a single zero piece isn't compressed
        ("1:0:0:2:0:0:0:3", "1:0:0:2::3"),
        ("1:0:0:0:2:0:0:3", "1::2:0:0:3"),
        # Equal-length runs: the first one is compressed
        ("1:0:0:2:0:0:3:4", "1::2:0:0:3:4"),
        ("0:0:1:0:0:2:0:0", "::1:0:0:2:0:0"),
        ("1:0:2:3:4:5:6:7", "1:0:2:3:4:5:6:7"),
        ("2001:DB8::8:800:200C:417A", "2001:db8::8:800:200c:417a"),
    ],
)
def test_serialize_ipv6_like_url_standard(addr, expected):
    assert _serialize_ipv6(addr) == expected


def test_url_parse_event_like_url_standard():
    """Special-scheme URLs are parsed like aw-server-rust's WHATWG parser"""

    def fields(url):
        d = _split(url)
        return (d["$domain"], d["$path"], d["$params"]) if "$domain" in d else None

    # Any number of slashes (or backslashes) before the authority
    assert fields("http:example.com") == ("example.com", "/", "")
    assert fields("http:/example.com/a") == ("example.com", "/a", "")
    assert fields("http:///x") == ("x", "/", "")
    assert fields("http:\\\\x.org\\a") == ("x.org", "/a", "")
    # Invalid ports and hosts leave the event unchanged
    for url in [
        "https://example.com:not-a-port/a",
        "https://example.com:65536/a",
        "http://a b.com/",
        "https://x.org%2Fevil/",
        "http://1.2.3.4.5/",
        "http://1..2/",
    ]:
        assert fields(url) is None, url
    assert fields("https://example.com:65535/a") == ("example.com", "/a", "")
    # Host normalization: punycode, IPv4 forms, compressed IPv6
    assert fields("https://bücher.de/") == ("xn--bcher-kva.de", "/", "")
    assert fields("http://0x7f.1/") == ("127.0.0.1", "/", "")
    assert fields("https://[::ffff:1.2.3.4]/") == ("[::ffff:102:304]", "/", "")
    # Path and query: dot segments resolved, percent-encoded
    assert fields("https://x.org/a/./b/../c") == ("x.org", "/a/c", "")
    assert fields("https://x.org/a/%2e%2E/b") == ("x.org", "/b", "")
    assert fields("https://x.org/a b?q=a b&r=ä#f g") == (
        "x.org",
        "/a%20b",
        "q=a%20b&r=%C3%A4",
    )
    # Non-ASCII digits aren't a port (and don't crash the transform)
    assert fields("https://example.com:\u00b2/a") is None
    assert fields("https://example.org:\uff11\uff12/a") is None
    # UTS 46 non-transitional: ß and ς are kept, compatibility forms mapped
    assert fields("https://fa\u00df.de/") == ("xn--fa-hia.de", "/", "")
    assert (
        fields("https://\uff25\uff38\uff21\uff2d\uff30\uff2c\uff25.com/")[0]
        == "example.com"
    )
    # UTS 46 validity: soft hyphens are removed, invalid labels are rejected
    assert fields("https://foo\u00adbar.com/")[0] == "foobar.com"
    assert fields("https://\u0301.com/") is None  # leading combining mark
    assert fields("https://xn--zz.com/") is None  # invalid A-label
    assert fields("https://xn--fa-hia.de/")[0] == "xn--fa-hia.de"
    # CheckBidi and CheckJoiners
    assert fields("https://\u05d0\u05d1x.com/") is None  # mixed direction label
    assert fields("https://\u05d0\u05d1.com/")[0] == "xn--4dbc.com"
    assert fields("https://\u200d\u094d.com/") is None  # leading joiner
    assert fields("https://xn--x-zhcd.com/") is None  # encoded mixed-direction label
    # ZWNJ in a joining context is valid (Persian)
    persian = "https://\u0646\u0627\u0645\u0647\u200c\u0627\u06cc.com/"
    assert fields(persian)[0] == "xn--mgba3gch31f060k.com"
    # "^" is kept literally in paths, like aw-server-rust
    assert fields("https://x.org/a^b") == ("x.org", "/a^b", "")
    # Other schemes: credentials or a port need a host
    assert fields("foo://:80/a") is None
    assert fields("foo://@/a") is None
    # Other schemes: ports validated, IPv6 without port, encoded like the URL Standard
    assert fields("foo://host:not-a-port/a") is None
    assert fields("foo://host:99999/a") is None
    assert fields("foo://[::1]:80/a") == ("[::1]", "/a", "")
    assert fields("foo://host/a b") == ("host", "/a%20b", "")
    assert fields("mailto:user@example.com?subject=hello world") == (
        "mailto",
        "user@example.com",
        "subject=hello%20world",
    )
    # file: URLs: localhost is no host, Windows drive letters stay in the path
    assert fields("file://localhost/etc") == ("file", "/etc", "")
    assert fields("file:c:/foo") == ("file", "/c:/foo", "")
    assert fields("file:C|/x") == ("file", "/C:/x", "")
    assert fields("file:///C:/../a") == ("file", "/C:/a", "")
    assert fields("file://host.example/share/x") == ("host.example", "/share/x", "")
    # "?" and "#" end the authority, as in the URL Standard
    assert fields("http://example.com?x/y") == ("example.com", "/", "x/y")
    # Other fields of the event are kept, like in aw-server-rust
    e = Event(
        data={"url": "https://x.org/", "$options": "old"},
        timestamp=datetime.now(timezone.utc),
        duration=1,
    )
    assert split_url_events([e])[0].data["$options"] == "old"


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

    # Two rules with the same tag give it once
    assert events[0].data["$tags"] == ["Test"]
    assert events[1].data["$tags"] == []

    # Sorted, not in rule order (like aw-server-rust)
    classes = [
        ("Work", Rule({"regex": "Terminal"})),
        ("Comms", Rule({"regex": "Inbox"})),
    ]
    e = Event(timestamp=now, duration=0, data={"app": "Terminal", "title": "Inbox"})
    assert tag([e], classes)[0].data["$tags"] == ["Comms", "Work"]


def test_rule_invalid_regex_does_not_raise():
    # An invalid user-supplied pattern (e.g. "Notepad++" on Python versions
    # where "++" is not a valid quantifier) must not raise from Rule.__init__.
    # Instead the rule should silently disable itself. See:
    # https://github.com/ActivityWatch/activitywatch/issues/1340
    rule = Rule({"regex": "*invalid("})
    assert rule.regex is None

    now = datetime.now(timezone.utc)
    e = Event(timestamp=now, duration=0, data={"key": "anything"})
    assert rule.match(e) is False


def test_categorize_survives_invalid_regex():
    # A single bad rule should not break categorization for the rest.
    now = datetime.now(timezone.utc)
    classes = [
        (["Bad"], Rule({"regex": "*invalid("})),
        (["Test"], Rule({"regex": "^just"})),
    ]
    events = [
        Event(timestamp=now, duration=0, data={"key": "just a test"}),
        Event(timestamp=now, duration=0, data={"key": "unrelated"}),
    ]
    events = categorize(events, classes)
    assert events[0].data["$category"] == ["Test"]
    assert events[1].data["$category"] == ["Uncategorized"]


def test_rule_non_string_regex_does_not_raise():
    # A truthy non-string value (e.g. {"regex": 123}) makes re.compile raise
    # TypeError. Rule.__init__ must disable the rule instead of propagating it,
    # so a single bad rule can't fail an entire query.
    rule = Rule({"regex": 123})
    assert rule.regex is None

    now = datetime.now(timezone.utc)
    e = Event(timestamp=now, duration=0, data={"key": "123"})
    assert rule.match(e) is False


def test_categorize_survives_non_string_regex():
    # Non-string and malformed string rules coexist with a valid rule.
    now = datetime.now(timezone.utc)
    classes = [
        (["Bad"], Rule({"regex": 123})),
        (["Bad2"], Rule({"regex": "*invalid("})),
        (["Test"], Rule({"regex": "^just"})),
    ]
    events = [
        Event(timestamp=now, duration=0, data={"key": "just a test"}),
        Event(timestamp=now, duration=0, data={"key": "unrelated"}),
    ]
    events = categorize(events, classes)
    assert events[0].data["$category"] == ["Test"]
    assert events[1].data["$category"] == ["Uncategorized"]


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
    # merge_events_by_keys drops events without the key, so check those directly
    assert sum_durations([e for e in result if "project" not in e.data]) == td15m
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


def test_merge_subwatcher_fields_zero_duration_base_event_preserved():
    """A zero-duration base event with overlapping sub must not be silently
    dropped. It should be kept as a single zero-duration event enriched with
    the active sub's fields."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    base = [Event(timestamp=now, duration=timedelta(0), data={"app": "vim"})]
    sub = [
        Event(
            timestamp=now - timedelta(seconds=5),
            duration=timedelta(seconds=10),
            data={"project": "P"},
        )
    ]

    result = merge_subwatcher_fields(base, sub, ["project"])

    assert len(result) == 1
    assert result[0].timestamp == now
    assert result[0].duration == timedelta(0)
    assert result[0].data == {"app": "vim", "project": "P"}


def test_merge_subwatcher_fields_zero_duration_base_event_no_overlap_preserved():
    """A zero-duration base event without any overlapping sub must still be
    returned untouched (already worked via fast path, locked in by this test)."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    base = [Event(timestamp=now, duration=timedelta(0), data={"app": "vim"})]
    sub = [
        Event(
            timestamp=now + timedelta(minutes=5),
            duration=timedelta(seconds=10),
            data={"project": "P"},
        )
    ]

    result = merge_subwatcher_fields(base, sub, ["project"])

    assert len(result) == 1
    assert result[0].timestamp == now
    assert result[0].duration == timedelta(0)
    assert result[0].data == {"app": "vim"}


def test_merge_subwatcher_fields_zero_duration_sub_does_not_color_base():
    """An instantaneous (zero-duration) sub event whose timestamp falls inside
    a base event must not split or color the base. The sub was active for
    zero time, so there is no slice of the base to enrich."""
    now = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    base = [Event(timestamp=now, duration=timedelta(seconds=10), data={"app": "vim"})]
    sub = [
        Event(
            timestamp=now + timedelta(seconds=3),
            duration=timedelta(0),
            data={"project": "P"},
        )
    ]

    result = merge_subwatcher_fields(base, sub, ["project"])

    # Base should pass through unchanged: no split, no color.
    assert len(result) == 1
    assert result[0].timestamp == now
    assert result[0].duration == timedelta(seconds=10)
    assert result[0].data == {"app": "vim"}
