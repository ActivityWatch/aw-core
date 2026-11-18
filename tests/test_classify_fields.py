"""Per-field patterns on the existing ``regex`` rule type (aw-webui#939).

mstsc and winbox can show the same host in their window titles; only app AND
title together tell them apart.
"""

import logging
from datetime import datetime, timezone

from aw_core import Event
from aw_transform.classify import Rule, categorize, compile_rules, tag

TS = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _event(app: str, title: str) -> Event:
    return Event(timestamp=TS, duration=1, data={"app": app, "title": title})


RDP = ["Remote", "RDP"]
WINBOX = ["Remote", "Winbox"]
CLASSES = [
    (
        RDP,
        {
            "type": "regex",
            "fields": {"app": r"mstsc\.exe", "title": r"office\.example"},
        },
    ),
    (
        WINBOX,
        {"type": "regex", "fields": {"app": r"winbox", "title": r"office\.example"}},
    ),
]


def test_fields_split_same_title_by_app():
    events = [
        _event("mstsc.exe", "office.example.com - Remote Desktop"),
        _event("winbox64.exe", "admin@office.example.com"),
        _event("firefox", "office.example.com"),
    ]
    rules_key, compiled = compile_rules(CLASSES)
    out = categorize(events, compiled, rules_key=rules_key)
    assert [e.data["$category"] for e in out] == [RDP, WINBOX, ["Uncategorized"]]
    # Rule.match agrees with the planned fast path
    rule = Rule(CLASSES[0][1])
    assert [rule.match(e) for e in events] == [True, False, False]


def test_fields_are_anded_and_require_present_string_values():
    rule = Rule({"type": "regex", "fields": {"app": "mstsc", "title": "office"}})
    assert not rule.match(_event("mstsc.exe", "home"))
    assert not rule.match(Event(timestamp=TS, duration=1, data={"app": "mstsc.exe"}))
    assert not rule.match(
        Event(timestamp=TS, duration=1, data={"app": "mstsc.exe", "title": 5})
    )


def test_fields_use_search_semantics_and_ignore_case():
    # Unanchored like ``regex``; explicit ^...$ is how exact match is asked for.
    assert Rule({"fields": {"app": "^mstsc"}}).match(_event("mstsc.exe", "x"))
    assert not Rule({"fields": {"app": "^mstsc$"}}).match(_event("mstsc.exe", "x"))
    assert Rule({"fields": {"app": "MSTSC"}, "ignore_case": True}).match(
        _event("mstsc.exe", "x")
    )


def test_ambiguous_or_invalid_fields_match_nothing(caplog):
    bad = [
        {"fields": {"app": "mstsc"}, "regex": "office"},
        {"fields": {"app": "mstsc"}, "select_keys": ["title"]},
        {"fields": {}},
        {"fields": ["app"]},
        {"fields": {"app": ""}},
        {"fields": {"app": "("}},
    ]
    with caplog.at_level(logging.WARNING):
        for spec in bad:
            rule = Rule(spec)
            assert rule.fields is None and rule.regex is None
            assert not rule.match(_event("mstsc.exe", "office"))
    assert len(caplog.records) == len(bad)


def test_fields_rules_work_for_tags():
    rules_key, compiled = compile_rules([("rdp", CLASSES[0][1])])
    out = tag([_event("mstsc.exe", "office.example")], compiled, rules_key=rules_key)
    assert out[0].data["$tags"] == ["rdp"]


def test_legacy_regex_rules_unchanged():
    rule = Rule({"type": "regex", "regex": "office", "select_keys": ["title"]})
    assert rule.fields is None
    assert rule.match(_event("mstsc.exe", "office"))
    assert not rule.match(_event("office", "home"))


def test_empty_regex_or_select_keys_count_as_unset():
    # Same rule as plain regex rules: an empty value means "not set". Editors may
    # serialize regex="" next to fields; older servers also treat "" as no regex,
    # so such a rule still matches nothing there.
    for spec in (
        {"fields": {"app": "mstsc"}, "regex": ""},
        {"fields": {"app": "mstsc"}, "select_keys": []},
    ):
        assert Rule(spec).match(_event("mstsc.exe", "x"))
