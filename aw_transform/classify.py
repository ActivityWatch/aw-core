from typing import Pattern, List, Iterable, Tuple, Dict, Optional
from functools import reduce
import re

from aw_core import Event


Tag = str
Category = List[str]


class Rule:
    regex: Optional[Pattern]

    def __init__(self, rules: Dict[str, str]):
        if "regex" in rules:
            self.regex = re.compile(rules["regex"]) if rules["regex"] else None

    def match(self, e: Event):
        for val in e.data.values():
            if isinstance(val, str):
                if self.regex and self.regex.search(val):
                    return True
        return False


def categorize(events: List[Event], classes: List[Tuple[Category, Rule]]):
    return [_categorize_one(e, classes) for e in events]


def _categorize_one(e: Event, classes: List[Tuple[Category, Rule]]) -> Event:
    e.data["$category"] = _pick_category([_cls for _cls, rule in classes if rule.match(e)])
    return e


def tag(events: List[Event], classes: List[Tuple[Tag, Rule]]):
    return [_tag_one(e, classes) for e in events]


def _tag_one(e: Event, classes: List[Tuple[Tag, Rule]]) -> Event:
    e.data["$tags"] = [_cls for _cls, rule in classes if rule.match(e)]
    return e


def _pick_category(tags: Iterable[Category]) -> Category:
    return reduce(_pick_deepest_cat, tags, ["Uncategorized"])


def _pick_deepest_cat(t1: Category, t2: Category) -> Category:
    # t1 will be the accumulator when used in reduce
    # Always bias against t1, since it could be "Uncategorized"
    return t2 if len(t2) >= len(t1) else t1
