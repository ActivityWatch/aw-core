import threading
from collections import OrderedDict
from typing import Any, Hashable


class LRUCache:
    """A small thread-safe LRU mapping bounded by entry count.

    Used for process-level caches that outlive a single query (parsed query
    statements, compiled category rules, per-event categorization results).
    """

    _MISSING = object()

    def __init__(self, maxsize: int) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self.maxsize = maxsize
        self._data: OrderedDict[Hashable, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Hashable, default: Any = None) -> Any:
        with self._lock:
            value = self._data.get(key, self._MISSING)
            if value is self._MISSING:
                return default
            self._data.move_to_end(key)
            return value

    def put(self, key: Hashable, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __contains__(self, key: Hashable) -> bool:
        with self._lock:
            return key in self._data
