import ipaddress
import logging
import re
from typing import List, Optional
from urllib.parse import unquote_to_bytes, urlsplit

from aw_core.models import Event

logger = logging.getLogger(__name__)

# The fields match aw-server-rust, which parses URLs with the WHATWG URL
# Standard (the `url` crate). For "special" schemes the parsing below follows
# the standard's steps that matter for these fields; other schemes have opaque
# hosts and paths and are split as-is.
_SPECIAL_SCHEMES = {"http", "https", "ws", "wss", "ftp", "file"}
_SCHEME_RE = re.compile(r"([A-Za-z][A-Za-z0-9+.\-]*):(.*)", re.DOTALL)
# Characters a domain may not contain (after percent-decoding)
_FORBIDDEN_HOST = set(" #%/:<>?@[\\]^|") | {chr(c) for c in range(0x20)} | {"\x7f"}
# Percent-encode sets from the URL Standard
_PATH_ENCODE = set(' "#<>?`{}')
_QUERY_ENCODE = set(" \"#<>'")  # "'" only for special schemes, which is all we encode


def _percent_encode(text: str, encode_set: set) -> str:
    out = []
    for char in text:
        if ord(char) < 0x21 or ord(char) > 0x7E or char in encode_set:
            out.extend(f"%{byte:02X}" for byte in char.encode("utf-8"))
        else:
            out.append(char)
    return "".join(out)


def _is_dot(segment: str, dots: str) -> bool:
    return segment.replace("%2e", ".").replace("%2E", ".") == dots


def _normalize_path(path: str) -> str:
    """Special-scheme path: "/" separators, dot segments resolved, encoded."""
    segments = path.replace("\\", "/").split("/")[1:]
    out: List[str] = []
    for i, segment in enumerate(segments):
        last = i == len(segments) - 1
        if _is_dot(segment, ".."):
            if out:
                out.pop()
            if last:
                out.append("")
        elif _is_dot(segment, "."):
            if last:
                out.append("")
        else:
            out.append(segment)
    return "/" + "/".join(_percent_encode(s, _PATH_ENCODE) for s in out)


def _ipv4_number(part: str) -> Optional[int]:
    if part[:2].lower() == "0x":
        digits, base = part[2:], 16
    elif len(part) > 1 and part.startswith("0"):
        digits, base = part[1:], 8
    else:
        digits, base = part, 10
    if digits == "":
        return 0
    try:
        return int(digits, base)
    except ValueError:
        return None


def _parse_ipv4(host: str) -> Optional[str]:
    """IPv4 in any form the URL Standard accepts (127.1, 0x7f.1, 2130706433),
    serialized dotted-decimal; "" if host isn't IPv4, None if it's invalid."""
    parts = host.split(".")
    if parts[-1] == "" and len(parts) > 1:
        parts.pop()
    last = parts[-1]
    ends_in_number = last.isdigit() or (
        last[:2].lower() == "0x" and all(c in "0123456789abcdefABCDEF" for c in last[2:])
    )
    if not ends_in_number:
        return ""
    if len(parts) > 4 or "" in parts:
        return None
    numbers = [_ipv4_number(p) for p in parts]
    if any(n is None for n in numbers) or any(n > 255 for n in numbers[:-1]):
        return None
    if numbers[-1] >= 256 ** (5 - len(numbers)):
        return None
    value = numbers[-1]
    for i, n in enumerate(numbers[:-1]):
        value += n * 256 ** (3 - i)
    return ".".join(str((value >> shift) & 0xFF) for shift in (24, 16, 8, 0))


def _parse_host(host: str) -> Optional[str]:
    """A special-scheme host as serialized by the URL Standard, or None if invalid."""
    if host.startswith("["):
        if not host.endswith("]"):
            return None
        try:
            return f"[{ipaddress.IPv6Address(host[1:-1]).compressed}]"
        except ValueError:
            return None
    try:
        decoded = unquote_to_bytes(host).decode("utf-8")
    except UnicodeDecodeError:
        return None
    if not decoded or any(c in _FORBIDDEN_HOST for c in decoded):
        return None
    if decoded.isascii():
        ascii_host = decoded.lower()
    else:
        try:
            ascii_host = decoded.encode("idna").decode("ascii").lower()
        except UnicodeError:
            return None
    ipv4 = _parse_ipv4(ascii_host)
    if ipv4 is None:
        return None
    return ipv4 or ascii_host


def _split_special(scheme: str, rest: str) -> Optional[dict]:
    # Any number of slashes or backslashes may precede the authority.
    rest = rest.lstrip("/\\")
    end = len(rest)
    for sep in "/\\?#":
        idx = rest.find(sep)
        if idx != -1:
            end = min(end, idx)
    authority, remainder = rest[:end], rest[end:]
    hostport = authority.rpartition("@")[2]
    if hostport.startswith("["):
        close = hostport.find("]")
        host, port = hostport[: close + 1], hostport[close + 1 :]
        if port and not port.startswith(":"):
            return None
        port = port[1:]
    else:
        host, _, port = hostport.partition(":")
    if port and (not port.isdigit() or int(port) > 65535):
        return None
    parsed_host = _parse_host(host)
    if parsed_host is None:
        return None
    path, _, query = remainder.partition("#")[0].partition("?")
    return {
        "$protocol": scheme,
        "$domain": _strip_www(parsed_host),
        "$path": _normalize_path(path or "/"),
        "$params": _percent_encode(query, _QUERY_ENCODE),
    }


def _strip_www(host: str) -> str:
    while host.startswith("www."):
        host = host[4:]
    return host


def _split_other(scheme: str, url: str) -> dict:
    parts = urlsplit(url)
    host = parts.netloc.rpartition("@")[2]
    if not host.startswith("["):
        host = host.partition(":")[0]
    path = parts.path
    if scheme == "file":
        path = _normalize_path(path.replace("\\", "/") or "/")
        host = host.lower()
    return {
        "$protocol": scheme,
        # No host (e.g. about:blank, file:///x): the scheme is the domain, so
        # these don't all cluster as an empty string.
        "$domain": _strip_www(host) or scheme,
        "$path": path,
        "$params": parts.query,
    }


def _split_url(url: str) -> Optional[dict]:
    # Like the URL Standard: strip leading/trailing C0 controls and spaces,
    # and remove tabs and newlines anywhere.
    url = url.strip("".join(chr(c) for c in range(0x21)))
    url = re.sub(r"[\t\n\r]", "", url)
    match = _SCHEME_RE.fullmatch(url)
    if not match:
        return None  # a relative URL or plain text, not something we can split
    scheme, rest = match.group(1).lower(), match.group(2)
    if scheme in _SPECIAL_SCHEMES and scheme != "file":
        return _split_special(scheme, rest)
    try:
        return _split_other(scheme, f"{scheme}:{rest}")
    except ValueError:
        return None


def split_url_events(events: List[Event]) -> List[Event]:
    """
    Adds ``$protocol``, ``$domain``, ``$path`` and ``$params`` to events with a
    ``url``, the same way as aw-server-rust (ActivityWatch/activitywatch#1466):

    - ``$domain`` is the host without port, userinfo or leading ``www.`` (the
      scheme when there's no host, like ``about:blank`` or ``file:///x``)
    - ``$path`` is the path, including any ``;params`` segment
    - ``$params`` is the query string (``b=c`` in ``/a?b=c``)

    For http(s), ws(s) and ftp URLs, the host is normalized (lowercase,
    punycode, compressed IPv6) and the path and query are percent-encoded with
    dot segments resolved, as the URL Standard does. Events whose ``url`` isn't
    a string or a valid absolute URL (no scheme, an invalid host or port) are
    left unchanged. Other fields of the event are never removed.
    """
    for event in events:
        url = event.data.get("url")
        if not isinstance(url, str):
            continue
        fields = _split_url(url)
        if fields is not None:
            event.data.update(fields)
    return events
