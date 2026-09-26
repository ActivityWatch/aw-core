import logging
from typing import List, Optional
from urllib.parse import SplitResult, urlsplit

from aw_core.models import Event

logger = logging.getLogger(__name__)

# WHATWG "special" schemes: they always have a host (except file) and a path
# that is at least "/", and their host is case-insensitive.
_SPECIAL_SCHEMES = {"http", "https", "ws", "wss", "ftp", "file"}


def _host(parts: SplitResult) -> str:
    """The host of a URL without userinfo and port (IPv6 keeps its brackets)."""
    host = parts.netloc.rpartition("@")[2]
    if host.startswith("["):
        return host[: host.find("]") + 1] if "]" in host else host
    host = host.partition(":")[0]
    return host.lower() if parts.scheme in _SPECIAL_SCHEMES else host


def _split_url(url: str) -> Optional[dict]:
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if not parts.scheme:
        return None  # a relative URL or plain text, not something we can split
    host = _host(parts)
    special = parts.scheme in _SPECIAL_SCHEMES
    if special and parts.scheme != "file" and not host:
        return None  # e.g. "http://", which aw-server-rust rejects as well
    domain = host
    while domain.startswith("www."):
        domain = domain[4:]
    path = parts.path
    if special and not path:
        path = "/"
    return {
        "$protocol": parts.scheme,
        # For URLs without a host (e.g. file://, about:), fall back to the
        # scheme so they don't all cluster as an empty string.
        "$domain": domain or parts.scheme,
        "$path": path,
        "$params": parts.query,
    }


def split_url_events(events: List[Event]) -> List[Event]:
    """
    Adds ``$protocol``, ``$domain``, ``$path`` and ``$params`` to events with a
    ``url``, the same way as aw-server-rust (ActivityWatch/activitywatch#1466):

    - ``$domain`` is the host without port, userinfo or leading ``www.`` (the
      scheme when there's no host, like ``about:blank`` or ``file:///x``)
    - ``$path`` is the path, including any ``;params`` segment
    - ``$params`` is the query string (``b=c`` in ``/a?b=c``)

    Events whose ``url`` isn't a string or an absolute URL are left unchanged.
    """
    for event in events:
        url = event.data.get("url")
        if not isinstance(url, str):
            continue
        fields = _split_url(url)
        if fields is not None:
            event.data.update(fields)
    return events
