import threading
from collections import OrderedDict
from typing import Any, Hashable, Optional, Tuple


class LRUCache:
    """A small thread-safe LRU mapping bounded by entry count.

    Used for process-level caches that outlive a single query (parsed query
    statements, compiled category rules, per-event categorization results).

    With ``max_weight``, entries also carry a caller-supplied weight (e.g. the
    length of the cached text) and the total is kept under that budget. An
    entry heavier than the whole budget is not cached at all.
    """

    _MISSING = object()

    def __init__(self, maxsize: int, max_weight: Optional[int] = None) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self.maxsize = maxsize
        self.max_weight = max_weight
        self._weight = 0
        self._data: OrderedDict[Hashable, Tuple[Any, int]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Hashable, default: Any = None) -> Any:
        with self._lock:
            item = self._data.get(key, self._MISSING)
            if item is self._MISSING:
                return default
            self._data.move_to_end(key)
            return item[0]  # type: ignore[index]

    def put(self, key: Hashable, value: Any, weight: int = 0) -> None:
        if self.max_weight is not None and weight > self.max_weight:
            return
        with self._lock:
            old = self._data.pop(key, None)
            if old is not None:
                self._weight -= old[1]
            self._data[key] = (value, weight)
            self._weight += weight
            while len(self._data) > self.maxsize or (
                self.max_weight is not None and self._weight > self.max_weight
            ):
                _, (_, w) = self._data.popitem(last=False)
                self._weight -= w

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._weight = 0

    @property
    def weight(self) -> int:
        with self._lock:
            return self._weight

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __contains__(self, key: Hashable) -> bool:
        with self._lock:
            return key in self._data
