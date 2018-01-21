import logging
from typing import List

from aw_core.models import Event

logger = logging.getLogger(__name__)

def limit_events(events, count) -> List[Event]:
    return events[:count]
