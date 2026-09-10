import json
from typing import Pattern, List, Iterable, Tuple, Dict, Optional, Any
import re

from aw_core import Event


Tag = str
Category = List[str]


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
    """Category/tag rule.

    Supports two variants, selected by the optional ``type`` field:

    * ``"regex"`` (default when ``type`` is absent): the existing per-field
      ``search`` rule.  A single ``regex`` pattern is tested against every event
      value (or the ``select_keys`` subset).  Backward-compatible.

    * ``"regex_fields"``: a new whole-field AND rule.  Every key in ``fields``
      must exist in the event data, be a string, and its value must satisfy the
      corresponding pattern in its entirety (``re.fullmatch``).  ``regex`` and
      ``select_keys`` are forbidden on this variant.
    """

    regex: Optional[Pattern]
    select_keys: Optional[List[str]]
    ignore_case: bool
    priority: Optional[int]
    _rule_type: str
    # Only populated for regex_fields rules.
    _field_patterns: Optional[Dict[str, Pattern]]

    def __init__(self, rules: Dict[str, Any]) -> None:
        self._rule_type = rules.get("type", "regex")
        if self._rule_type not in ("regex", "regex_fields"):
            raise ValueError(f"unsupported rule type: {self._rule_type!r}")
        self.ignore_case = rules.get("ignore_case", False)
        self.priority = _parse_optional_priority(rules)
        flags = (re.IGNORECASE if self.ignore_case else 0) | re.UNICODE

        if self._rule_type == "regex_fields":
            # Guard against the identified rollout hazard: old Python silently
            # reads a stale `regex` member and produces false matches.  Reject
            # both forbidden members explicitly.
            if "regex" in rules:
                raise ValueError(
                    "regex_fields rule must not contain a 'regex' member "
                    "(use 'fields' instead)"
                )
            if "select_keys" in rules:
                raise ValueError(
                    "regex_fields rule must not contain 'select_keys' "
                    "(use 'fields' instead)"
                )
            raw_fields: Any = rules.get("fields")
            if not isinstance(raw_fields, dict) or not raw_fields:
                raise ValueError("regex_fields rule requires a non-empty 'fields' dict")
            self._field_patterns = {}
            for field, pattern in raw_fields.items():
                if not isinstance(field, str) or not field:
                    raise ValueError(
                        "regex_fields: field names must be non-empty strings"
                    )
                if not isinstance(pattern, str) or not pattern:
                    raise ValueError(
                        f"regex_fields: pattern for field '{field}' must be a non-empty string"
                    )
                try:
                    self._field_patterns[field] = re.compile(pattern, flags)
                except re.error as exc:
                    raise ValueError(
                        f"regex_fields: invalid pattern for field '{field}': {exc}"
                    ) from exc
            # Legacy attributes unused for this variant.
            self.regex = None
            self.select_keys = None
        else:
            # Legacy "regex" variant (also the default when type is absent).
            self._field_patterns = None
            self.select_keys = rules.get("select_keys", None)

            # NOTE: Also checks that the regex isn't an empty string (which would erroneously match everything)
            regex_str = rules.get("regex", None)
            self.regex = re.compile(regex_str, flags) if regex_str else None

    def match(self, e: Event) -> bool:
        if self._rule_type == "regex_fields":
            # ALL named fields must exist, be strings, and fully satisfy their pattern.
            assert self._field_patterns is not None
            for field, pattern in self._field_patterns.items():
                value = e.data.get(field)
                if not isinstance(value, str):
                    return False
                if not pattern.fullmatch(value):
                    return False
            return bool(
                self._field_patterns
            )  # empty map never matches (guarded at init)
        else:
            # Legacy regex variant.
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
