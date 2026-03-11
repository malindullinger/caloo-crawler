from __future__ import annotations

from typing import Dict, Type

from .base import BaseAdapter
from .adapters.maennedorf_portal import MaennedorfPortalAdapter
from .adapters.eventbrite import EventbriteAdapter


ADAPTERS: Dict[str, Type[BaseAdapter]] = {
    "maennedorf_portal": MaennedorfPortalAdapter,
    "eventbrite": EventbriteAdapter,
}


def get_adapter(name: str) -> BaseAdapter:
    cls = ADAPTERS[name]
    return cls()
