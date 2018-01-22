import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from copy import copy, deepcopy
import operator
from functools import reduce
from collections import defaultdict

from aw_core.models import Event
from aw_core import TimePeriod

logger = logging.getLogger(__name__)


from urllib.parse import urlparse

def split_url_events(events):
    for event in events:
        if "url" in event.data:
            url = event.data["url"]
            parsed_url = urlparse(url)
            event.data["protocol"] = parsed_url.scheme
            event.data["domain"] = parsed_url.netloc
            if event.data["domain"][:4] == "www.":
                event.data["domain"] = event.data["domain"][4:]
            event.data["path"] = parsed_url.path
            event.data["params"] = parsed_url.params
            event.data["options"] = parsed_url.query
            event.data["identifier"] = parsed_url.fragment
            # TODO: Parse user, port etc aswell
    return events
