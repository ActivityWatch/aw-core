from typing import Pattern, List, Set, Iterable, Tuple
from functools import reduce

from aw_core import Event


def classify(events: List[Event], classes: List[Tuple[str, Pattern]]):
    return [_classify_one(e, classes) for e in events]


def _classify_one(e: Event, classes: List[Tuple[str, Pattern]]) -> Event:
    tags = set()  # type: Set[str]
    for _cls, re in classes:
        for val in e.data.values():
            if isinstance(val, str) and re.search(val):
                tags.add(_cls)
                break
    e.data["$tags"] = list(tags)
    e.data["$category"] = _pick_category(tags)
    return e


def _pick_category(tags: Iterable[str]) -> str:
    return reduce(_pick_deepest_cat, tags, "Uncategorized")


def _pick_deepest_cat(t1: str, t2: str) -> str:
    # t1 will be the accumulator when used in reduce
    # Always bias against t1, since it could be "Uncategorized"
    if t2.startswith("#"):
        return t1
    else:
        return t2 if t2.count("->") >= t1.count("->") else t1
