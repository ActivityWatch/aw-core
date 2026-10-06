"""Process-level caches in aw_query/aw_transform must not change results."""

import json
import random
from datetime import timedelta

import iso8601
import pytest
from aw_core.cache import LRUCache
from aw_core.models import Event
from aw_query import query
from aw_query.exceptions import QueryInterpretException
from aw_transform import Rule, categorize, compile_rules, tag
from aw_transform import classify

from .utils import TempTestBucket, param_datastore_objects

CLASSES = [
    [["Work"], {"type": "regex", "regex": "code|terminal", "ignore_case": True}],
    [["Work", "Programming"], {"type": "regex", "regex": "vim|Code"}],
    [["Media"], {"type": "regex", "regex": "youtube", "priority": 100}],
    [
        ["Comms"],
        {
            "type": "regex",
            "regex": "slack",
            "select_keys": ["app"],
            "ignore_case": True,
        },
    ],
    [["Empty"], {"type": "regex", "regex": ""}],
]

DATAS = [
    {"app": "Code", "title": "main.py"},
    {"app": "Terminal", "title": "vim notes"},
    {"app": "Firefox", "title": "youtube - Code review"},
    {"app": "Slack", "title": "general"},
    {"app": "Firefox", "title": "slack in the title only"},
    {"app": "Finder", "title": ""},
]


def _events():
    t = iso8601.parse_date("2026-01-01")
    return [Event(timestamp=t, duration=1, data=dict(d)) for d in DATAS]


def _uncached(events, classes):
    return categorize(events, [(c, Rule(r)) for c, r in classes])


@pytest.fixture(autouse=True)
def clear_caches():
    for c in (
        classify._compiled_rules_cache,
        classify._category_memo,
        classify._tag_memo,
    ):
        c.clear()
    yield


def test_lru_bound_and_order():
    c = LRUCache(maxsize=2)
    c.put("a", 1)
    c.put("b", 2)
    assert c.get("a") == 1  # a is now most recent
    c.put("c", 3)
    assert "b" not in c and "a" in c and "c" in c
    assert len(c) == 2
    with pytest.raises(ValueError):
        LRUCache(maxsize=0)


def test_categorize_cached_equals_uncached():
    expected = [e.data["$category"] for e in _uncached(_events(), CLASSES)]
    rules_key, compiled = compile_rules(CLASSES)
    for _ in range(2):  # second round is served from the cross-call memo
        got = [
            e.data["$category"]
            for e in categorize(_events(), compiled, rules_key=rules_key)
        ]
        assert got == expected
    assert expected[2] == ["Media"]  # explicit priority beats deeper category


def test_tag_cached_equals_uncached():
    expected = [
        e.data["$tags"] for e in tag(_events(), [(c, Rule(r)) for c, r in CLASSES])
    ]
    rules_key, compiled = compile_rules(CLASSES)
    for _ in range(2):
        got = [e.data["$tags"] for e in tag(_events(), compiled, rules_key=rules_key)]
        assert got == expected


def test_memo_hit_skips_matching(monkeypatch):
    rules_key, compiled = compile_rules(CLASSES)
    categorize(_events(), compiled, rules_key=rules_key)
    calls = []
    real = classify._matching
    monkeypatch.setattr(
        classify, "_matching", lambda *a, **kw: calls.append(1) or real(*a, **kw)
    )
    out = categorize(_events(), compiled, rules_key=rules_key)
    assert calls == []
    assert out[0].data["$category"] == ["Work", "Programming"]


def _reference_category(e, classes):
    return classify._pick_category([(c, r) for c, r in classes if r.match(e)])


def test_categorize_matches_reference_randomized():
    """Early-exit matching must pick exactly what _pick_category picks from all matches."""
    rng = random.Random(1465)
    words = ["code", "vim", "slack", "mail", "tube", "doc", "x", "Y"]
    for _ in range(200):
        classes = []
        for _ in range(rng.randint(1, 12)):
            cat = [rng.choice(["A", "B", "C"]) for _ in range(rng.randint(0, 3))]
            rule = {"type": "regex", "regex": rng.choice(words + [""])}
            if rng.random() < 0.3:
                rule["priority"] = rng.randint(-5, 40)
            if rng.random() < 0.3:
                rule["select_keys"] = rng.sample(
                    ["app", "title", "url"], rng.randint(0, 2)
                )
            if rng.random() < 0.3:
                rule["ignore_case"] = True
            classes.append([cat, rule])
        compiled = [(c, Rule(r)) for c, r in classes]
        t = iso8601.parse_date("2026-01-01")
        events = [
            Event(
                timestamp=t,
                duration=1,
                data={
                    "app": rng.choice(words),
                    "title": " ".join(rng.sample(words, 2)),
                    "url": rng.choice(words),
                    "n": 1,
                },
            )
            for _ in range(20)
        ]
        expected = [_reference_category(e, compiled) for e in events]
        got = [
            e.data["$category"]
            for e in categorize([Event(**e) for e in events], compiled)
        ]
        assert got == expected
        expected_tags = [
            classify._sorted_tags([c for c, r in compiled if r.match(e)])
            for e in events
        ]
        got_tags = [
            e.data["$tags"] for e in tag([Event(**e) for e in events], compiled)
        ]
        assert got_tags == expected_tags


def test_compiled_rules_reused_and_keyed_by_content():
    k1, c1 = compile_rules(CLASSES)
    k2, c2 = compile_rules(json.loads(json.dumps(CLASSES)))  # equal, not identical
    assert k1 == k2 and c1 is c2
    changed = [[["Other"], {"type": "regex", "regex": "Code"}]]
    k3, _ = compile_rules(changed)
    assert k3 != k1
    out = categorize(_events(), compile_rules(changed)[1], rules_key=k3)
    assert out[0].data["$category"] == ["Other"]


def test_invalid_rules_not_cached():
    bad = [[["x"], {"type": "regex", "regex": "x", "priority": "high"}]]
    for _ in range(2):
        with pytest.raises(ValueError):
            compile_rules(bad)
    assert len(classify._compiled_rules_cache) == 0


def test_returned_categories_are_fresh_lists():
    rules_key, compiled = compile_rules(CLASSES)
    a = categorize(_events(), compiled, rules_key=rules_key)[0].data["$category"]
    a.append("mutated")
    b = categorize(_events(), compiled, rules_key=rules_key)[0].data["$category"]
    assert b == ["Work", "Programming"]


def test_unstable_keys_not_memoized_globally():
    rules_key, compiled = compile_rules(CLASSES)
    t = iso8601.parse_date("2026-01-01")
    e = Event(timestamp=t, duration=1, data={"app": "Code", "obj": object()})
    categorize([e], compiled, rules_key=rules_key)
    assert e.data["$category"] == ["Work", "Programming"]
    assert len(classify._category_memo) == 0


@pytest.mark.parametrize("datastore", param_datastore_objects())
def test_cached_statements_resolve_variables_per_query(datastore):
    """Identical statements reused across queries must see each query's own data."""
    day1 = iso8601.parse_date("2026-01-01")
    day2 = day1 + timedelta(days=1)
    with TempTestBucket(datastore) as bucket:
        bucket.insert(Event(timestamp=day1, duration=10, data={"app": "Code"}))
        bucket.insert(
            Event(
                timestamp=day2 + timedelta(hours=12), duration=10, data={"app": "Slack"}
            )
        )
        q = f"""
            events = query_bucket("{bucket.bucket_id}");
            events = categorize(events, {json.dumps(CLASSES)});
            RETURN = events;
        """
        r1 = query("q", q, day1, day2, datastore)
        r2 = query("q", q, day2, day2 + timedelta(days=1), datastore)
        assert [e["data"]["app"] for e in r1] == ["Code"]
        assert [e["data"]["app"] for e in r2] == ["Slack"]
        assert r2[0]["data"]["$category"] == ["Comms"]


def test_undefined_variable_still_raises_with_cache():
    t = iso8601.parse_date("1970-01-01")
    for _ in range(2):
        with pytest.raises(QueryInterpretException):
            query("q", "RETURN = not_defined;", t, t + timedelta(days=1), None)


def test_lru_weight_budget():
    c = LRUCache(maxsize=10, max_weight=10)
    c.put("a", 1, weight=4)
    c.put("b", 2, weight=4)
    c.put("c", 3, weight=4)  # total 12 > 10, evicts oldest
    assert "a" not in c and "b" in c and "c" in c and c.weight == 8
    c.put("huge", 4, weight=11)  # heavier than the whole budget: not cached
    assert "huge" not in c and c.weight == 8
    c.put("b", 5, weight=1)  # replacing an entry updates the weight
    assert c.get("b") == 5 and c.weight == 5


def test_non_json_classes_fall_back_uncached():
    """Non-serializable class lists still reach Rule() so its error is reported."""
    with pytest.raises(ValueError):
        compile_rules([[["x"], {"type": "regex", "regex": "x", "priority": object()}]])
    key, compiled = compile_rules(
        [[("x",), {"type": "regex", "regex": "x", "o": object()}]]
    )
    assert key is None and len(compiled) == 1
    assert len(classify._compiled_rules_cache) == 0
