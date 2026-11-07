"""Android toolbar URLs may have had their protocol stripped before storage."""

from datetime import datetime, timedelta, timezone

import pytest

from aw_core.models import Event
from aw_query import query
from aw_transform import split_url_events

from .utils import TempTestBucket, param_datastore_objects


def _split(url):
    event = Event(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        duration=1,
        data={"url": url, "title": "kept", "$options": "kept"},
    )
    result = split_url_events([event])
    assert result[0] is event
    assert event.data["url"] == url
    assert event.data["title"] == "kept"
    assert event.data["$options"] == "kept"
    return event.data


@pytest.mark.parametrize(
    "url,domain,path,params",
    [
        ("www.example.com/path?x=1#frag", "example.com", "/path", "x=1"),
        ("example.com", "example.com", "/", ""),
        ("//www.example.com/path?x=1#frag", "example.com", "/path", "x=1"),
        ("example.com:8080/path", "example.com", "/path", ""),
        ("example.com:0/path", "example.com", "/path", ""),
        ("example.com:65535/path", "example.com", "/path", ""),
        ("example.com:80?x=1#frag", "example.com", "/", "x=1"),
        ("example.com./path", "example.com.", "/path", ""),
        ("//example.com:8080/path", "example.com", "/path", ""),
        ("example.com?x=1#frag", "example.com", "/", "x=1"),
        ("WWW.Example.COM/a/../b;p?x=1#frag", "example.com", "/b;p", "x=1"),
        ("bücher.example/über?q=é", "xn--bcher-kva.example", "/%C3%BCber", "q=%C3%A9"),
        ("127.0.0.1:8080/path", "127.0.0.1", "/path", ""),
        ("[2001:DB8::1]:8080/path", "[2001:db8::1]", "/path", ""),
        ("//[::1]/path", "[::1]", "/path", ""),
    ],
)
def test_schemeless_url_fields(url, domain, path, params):
    data = _split(url)
    assert {key: data[key] for key in ("$protocol", "$domain", "$path", "$params")} == {
        "$protocol": "",
        "$domain": domain,
        "$path": path,
        "$params": params,
    }
    assert "$identifier" not in data


@pytest.mark.parametrize("separator", ["\u3002", "\uff0e", "\uff61"])
@pytest.mark.parametrize("prefix", ["", "//"])
@pytest.mark.parametrize("port", ["", ":8080"])
def test_schemeless_unicode_domain_separators(separator, prefix, port):
    authority = f"例え{separator}テスト{port}"
    data = _split(f"{prefix}{authority}/path?x=1#frag")
    explicit = _split(f"https://{authority}/path?x=1#frag")
    assert data["$protocol"] == ""
    assert data["$domain"] == "xn--r8jz45g.xn--zckzah"
    for key in ("$domain", "$path", "$params"):
        assert data[key] == explicit[key]


@pytest.mark.parametrize(
    "url",
    [
        "",
        "random text",
        "random",
        "/relative/path",
        "./example.com/path",
        "../example.com/path",
        "localhost/path",
        "//localhost/path",
        "2130706433/path",
        "0x7f000001/path",
        "///example.com/path",
        "not a.example/path",
        "example..com/path",
        "-example.com/path",
        "//example.com:bad/path",
        "[invalid]/path",
        "[::1]extra/path",
        "//example.com:65536/path",
        "[::1]:99999/path",
        "user@example.com/path",
    ],
)
def test_not_a_schemeless_hostname(url):
    assert set(_split(url)) == {"url", "title", "$options"}


@pytest.mark.parametrize(
    "url,protocol,domain,path",
    [
        ("https://www.example.com/path", "https", "example.com", "/path"),
        ("http://example.com", "http", "example.com", "/"),
        ("file:///tmp/example.com", "file", "file", "/tmp/example.com"),
        ("about:blank", "about", "about", "blank"),
        ("chrome://settings/privacy", "chrome", "settings", "/privacy"),
        ("custom:example.com/path", "custom", "custom", "example.com/path"),
        ("com.example:opaque/path", "com.example", "com.example", "opaque/path"),
        ("com.example://host/path", "com.example", "host", "/path"),
        ("com.example:123opaque", "com.example", "com.example", "123opaque"),
        ("example.com:99999/path", "example.com", "example.com", "99999/path"),
        ("custom://example.com/path", "custom", "example.com", "/path"),
        ("localhost:8080/path", "localhost", "localhost", "8080/path"),
    ],
)
def test_explicit_schemes_still_win(url, protocol, domain, path):
    data = _split(url)
    assert (data["$protocol"], data["$domain"], data["$path"], data["$params"]) == (
        protocol,
        domain,
        path,
        "",
    )


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_query2_filters_historical_android_domains(datastore):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    urls = [
        "www.example.com/path?x=1#frag",
        "example.com:8080/path",
        "//example.com/path",
        "https://www.example.com/path",
        "other.example/path",
    ]
    with TempTestBucket(datastore) as bucket:
        for i, url in enumerate(urls):
            bucket.insert(
                Event(
                    timestamp=start + timedelta(seconds=i),
                    duration=1,
                    data={"url": url},
                )
            )
        result = query(
            "schemeless-domains",
            f'events = query_bucket("{bucket.bucket_id}"); '
            "events = split_url_events(events); "
            'RETURN = filter_keyvals(events, "$domain", ["example.com"]);',
            start,
            start + timedelta(hours=1),
            datastore,
        )
        assert {event.data["url"] for event in result} == set(urls[:4])
        assert {event.data["$protocol"] for event in result} == {"", "https"}

        # The frontend groups these historical toolbar URLs with explicit
        # web URLs; domain inference must not silently lose their duration.
        grouped = query(
            "schemeless-domain-duration",
            f'events = split_url_events(query_bucket("{bucket.bucket_id}")); '
            'RETURN = merge_events_by_keys(events, ["$domain"]);',
            start,
            start + timedelta(hours=1),
            datastore,
        )
        assert {event.data["$domain"]: event.duration for event in grouped} == {
            "example.com": timedelta(seconds=4),
            "other.example": timedelta(seconds=1),
        }
