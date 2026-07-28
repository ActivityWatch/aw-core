import hashlib
import json
import logging
from typing import Callable, Pattern, List, Iterable, Tuple, Dict, Optional, Any
import re

from aw_core import Event
from aw_core.cache import LRUCache


logger = logging.getLogger(__name__)

Tag = str
Category = List[str]

# Process-level caches shared across queries. Long-range views issue one query
# per day with the same (often large) rule set, so both the compiled rules and
# the per-event results are reused across requests instead of rebuilt each time.
# Results are keyed by a hash of the rule set, so editing categories never
# returns stale results; old rule sets just age out of the LRU.
_compiled_rules_cache = LRUCache(maxsize=16)
_plan_cache = LRUCache(maxsize=16)
_category_memo = LRUCache(maxsize=50_000)
_tag_memo = LRUCache(maxsize=50_000)


def _parse_optional_priority(rules: Dict[str, Any]) -> Optional[int]:
    if "priority" in rules:
        val = rules["priority"]
    elif "weight" in rules:
        val = rules["weight"]
    else:
        return None
    # bool is a subclass of int
    if isinstance(val, bool) or not isinstance(val, int):
        raise ValueError("priority/weight must be an integer")
    return val


class Rule:
    regex: Optional[Pattern]
    select_keys: Optional[List[str]]
    ignore_case: bool
    priority: Optional[int]

    def __init__(self, rules: Dict[str, Any]) -> None:
        self.select_keys = rules.get("select_keys", None)
        self.ignore_case = rules.get("ignore_case", False)
        self.priority = _parse_optional_priority(rules)

        # NOTE: Also checks that the regex isn't an empty string (which would erroneously match everything)
        regex_str = rules.get("regex", None)
        if regex_str:
            try:
                self.regex = re.compile(
                    regex_str,
                    (re.IGNORECASE if self.ignore_case else 0) | re.UNICODE,
                )
            except re.error as e:
                # An invalid user-supplied pattern should not crash categorization
                # for the entire query. Log it and disable this rule (match nothing).
                logger.warning(
                    "Invalid regex pattern %r in category/tag rule (%s); "
                    "this rule will match nothing.",
                    regex_str,
                    e,
                )
                self.regex = None
        else:
            self.regex = None

    def match(self, e: Event) -> bool:
        if self.select_keys:
            values = [e.data.get(key, None) for key in self.select_keys]
        else:
            values = list(e.data.values())
        if self.regex:
            for val in values:
                if isinstance(val, str) and self.regex.search(val):
                    return True
        return False


def compile_rules(
    classes: List[Tuple[Any, Dict[str, Any]]],
) -> Tuple[Optional[str], List[Tuple[Any, Rule]]]:
    """Build Rule objects for a class list, reusing them across queries.

    Returns ``(rules_key, compiled)``; ``rules_key`` is None if the class list
    isn't JSON-serializable (then nothing is cached). ``rules_key`` identifies the rule set and
    scopes the cross-query result memos in :func:`categorize` and :func:`tag`.
    The compiled list is shared between callers and must not be mutated.
    Invalid rules raise ValueError and are never cached.
    """
    try:
        rules_key = hashlib.sha1(
            json.dumps(classes, sort_keys=True).encode("utf-8")
        ).hexdigest()
    except TypeError:
        # Not a plain JSON rule list: compile uncached so Rule() reports the
        # real problem, and skip the cross-query memos.
        return None, [(_cls, Rule(rule_dict)) for _cls, rule_dict in classes]
    compiled = _compiled_rules_cache.get(rules_key)
    if compiled is None:
        compiled = [(_cls, Rule(rule_dict)) for _cls, rule_dict in classes]
        _compiled_rules_cache.put(rules_key, compiled)
    return rules_key, compiled


def _data_key(e: Event) -> Tuple[str, bool]:
    """Key identifying an event's data; the bool says if it is stable across queries."""
    try:
        return json.dumps(e.data, sort_keys=True), True
    except TypeError:
        # id() is only unique while the object lives, so never memoize it globally
        return str(id(e.data)), False


_Search = Callable[[str], Any]
_Step = Tuple[Any, _Search, Optional[Tuple[str, ...]]]


def _plan(classes: List[Tuple[Any, Rule]], for_category: bool) -> List[_Step]:
    """Precompute (class, regex.search, select_keys) steps for matching.

    For categories the steps are ordered by (rank, position) descending, so the
    first match is exactly what :func:`_pick_category` would pick from all
    matches (highest rank, later rule on ties) and matching can stop early.
    Rules without a regex never match and are dropped, as are empty categories.
    """
    steps = [
        (i, cls, rule)
        for i, (cls, rule) in enumerate(classes)
        if rule.regex is not None and (cls or not for_category)
    ]
    if for_category:
        steps.sort(key=lambda s: (_effective_rank(s[1], s[2]), s[0]), reverse=True)
    return [
        (
            cls,
            rule.regex.search,  # type: ignore[union-attr]
            tuple(rule.select_keys) if rule.select_keys else None,
        )
        for _, cls, rule in steps
    ]


def _cached_plan(
    classes: List[Tuple[Any, Rule]], rules_key: Optional[str], for_category: bool
) -> List[_Step]:
    if rules_key is None:
        return _plan(classes, for_category)
    plan_key = (rules_key, for_category)
    plan = _plan_cache.get(plan_key)
    if plan is None:
        plan = _plan(classes, for_category)
        _plan_cache.put(plan_key, plan)
    return plan


def _matching(data: Dict[str, Any], plan: List[_Step], first_only: bool) -> list:
    """Classes whose rule matches any (selected) string value, like Rule.match.

    String values are collected once per event instead of once per rule.
    """
    all_values = [v for v in data.values() if isinstance(v, str)]
    selected: Dict[Tuple[str, ...], List[str]] = {}
    found = []
    for cls, search, keys in plan:
        if keys is None:
            values = all_values
        else:
            values = selected.get(keys)  # type: ignore[assignment]
            if values is None:
                values = selected[keys] = [
                    v for v in (data.get(k) for k in keys) if isinstance(v, str)
                ]
        for v in values:
            if search(v):
                found.append(cls)
                if first_only:
                    return found
                break
    return found


def categorize(
    events: List[Event],
    classes: List[Tuple[Category, Rule]],
    rules_key: Optional[str] = None,
) -> List[Event]:
    """Set ``$category`` on each event.

    Results are memoized per distinct event data within the call, and also
    across calls when ``rules_key`` (from :func:`compile_rules`) is given.
    """
    plan = _cached_plan(classes, rules_key, for_category=True)
    cache: Dict[str, Tuple[str, ...]] = {}
    for e in events:
        key, stable = _data_key(e)
        category = cache.get(key)
        if category is None:
            memo_key = (rules_key, key) if rules_key is not None and stable else None
            if memo_key is not None:
                category = _category_memo.get(memo_key)
            if category is None:
                match = _matching(e.data, plan, first_only=True)
                category = tuple(match[0]) if match else ("Uncategorized",)
                if memo_key is not None:
                    _category_memo.put(memo_key, category)
            cache[key] = category
        e.data["$category"] = list(category)
    return events


def _categorize_one(e: Event, classes: List[Tuple[Category, Rule]]) -> Event:
    e.data["$category"] = _pick_category(
        [(_cls, rule) for _cls, rule in classes if rule.match(e)]
    )
    return e


def tag(
    events: List[Event],
    classes: List[Tuple[Tag, Rule]],
    rules_key: Optional[str] = None,
) -> List[Event]:
    """Set ``$tags`` on each event, memoized like :func:`categorize`."""
    plan = _cached_plan(classes, rules_key, for_category=False)
    cache: Dict[str, Tuple[Tag, ...]] = {}
    for e in events:
        key, stable = _data_key(e)
        tags = cache.get(key)
        if tags is None:
            memo_key = (rules_key, key) if rules_key is not None and stable else None
            if memo_key is not None:
                tags = _tag_memo.get(memo_key)
            if tags is None:
                tags = tuple(_matching(e.data, plan, first_only=False))
                if memo_key is not None:
                    _tag_memo.put(memo_key, tags)
            cache[key] = tags
        e.data["$tags"] = list(tags)
    return events


def _tag_one(e: Event, classes: List[Tuple[Tag, Rule]]) -> Event:
    e.data["$tags"] = [_cls for _cls, rule in classes if rule.match(e)]
    return e


def _effective_rank(category: Category, rule: Rule) -> int:
    # Integer-only. Default is depth * 10 so explicit priorities can slot
    # between nesting levels (depth 1 → 10, depth 2 → 20). Relative order of
    # unprioritized rules is unchanged.
    if rule.priority is not None:
        return rule.priority
    return len(category) * 10


def _pick_category(matches: Iterable[Tuple[Category, Rule]]) -> Category:
    category: Category = ["Uncategorized"]
    rank: Optional[int] = None
    for cat, rule in matches:
        if not cat:
            continue
        item_rank = _effective_rank(cat, rule)
        # None means no match yet, so any non-empty category wins — including
        # an explicit priority below a signed 64-bit floor. Equal ranks keep
        # the later match (same contract as the old depth-only `>=`).
        if rank is None or item_rank >= rank:
            category = cat
            rank = item_rank
    return category
