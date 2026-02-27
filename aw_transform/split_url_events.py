import logging
from typing import List

from urllib.parse import urlparse

from aw_core.models import Event

logger = logging.getLogger(__name__)


def split_url_events(events: List[Event]) -> List[Event]:
    for event in events:
        if "url" in event.data:
            url = event.data["url"]
            parsed_url = urlparse(url)
            event.data["$protocol"] = parsed_url.scheme
            netloc = parsed_url.netloc
            if netloc:
                domain = netloc[4:] if netloc[:4] == "www." else netloc
            elif parsed_url.scheme:
                # For URLs without a domain (e.g. file://, about:),
                # use the scheme as domain so they don't all cluster as empty.
                domain = parsed_url.scheme
            else:
                domain = ""
            event.data["$domain"] = domain
            event.data["$path"] = parsed_url.path
            event.data["$params"] = parsed_url.params
            event.data["$options"] = parsed_url.query
            event.data["$identifier"] = parsed_url.fragment
            # TODO: Parse user, port etc aswell
    return events
