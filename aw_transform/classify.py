import json
from typing import Pattern, List, Iterable, Tuple, Dict, Optional, Any
import re

from aw_core import Event


Tag = str
Category = List[str]

# Sentinel so any real match — including a large negative explicit priority —
# still beats the Uncategorized fallback.
_UNCATEGORIZED_RANK = -(2**63)


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
        self.regex = (
            re.compile(
                regex_str, (re.IGNORECASE if self.ignore_case else 0) | re.UNICODE
            )
            if regex_str
            else None
        )

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


def categorize(
    events: List[Event], classes: List[Tuple[Category, Rule]]
) -> List[Event]:
    cache: Dict[str, Category] = {}
    for e in events:
        try:
            key = json.dumps(e.data, sort_keys=True)
        except TypeError:
            key = str(id(e.data))
        if key not in cache:
            cache[key] = _pick_category(
                [(_cls, rule) for _cls, rule in classes if rule.match(e)]
            )
        e.data["$category"] = list(cache[key])
    return events


def _categorize_one(e: Event, classes: List[Tuple[Category, Rule]]) -> Event:
    e.data["$category"] = _pick_category(
        [(_cls, rule) for _cls, rule in classes if rule.match(e)]
    )
    return e


def tag(events: List[Event], classes: List[Tuple[Tag, Rule]]) -> List[Event]:
    cache: Dict[str, List[Tag]] = {}
    for e in events:
        try:
            key = json.dumps(e.data, sort_keys=True)
        except TypeError:
            key = str(id(e.data))
        if key not in cache:
            cache[key] = [_cls for _cls, rule in classes if rule.match(e)]
        e.data["$tags"] = list(cache[key])
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
    rank = _UNCATEGORIZED_RANK
    for cat, rule in matches:
        if not cat:
            continue
        item_rank = _effective_rank(cat, rule)
        if item_rank >= rank:
            category = cat
            rank = item_rank
    return category
