from __future__ import annotations

from typing import Dict, Type

from .base import BaseAdapter
from .adapters.maennedorf_portal import MaennedorfPortalAdapter
from .adapters.church_hub import ChurchHubAdapter
from .adapters.vereins_directory import VereinsDirectoryAdapter
from .adapters.kino_wildenmann import KinoWildenmannAdapter
from .adapters.eventbrite import EventbriteAdapter


ADAPTERS: Dict[str, Type[BaseAdapter]] = {
    "maennedorf_portal": MaennedorfPortalAdapter,
    "church_hub": ChurchHubAdapter,
    "vereins_directory": VereinsDirectoryAdapter,
    "kino_wildenmann": KinoWildenmannAdapter,
    "eventbrite": EventbriteAdapter,
}


def get_adapter(name: str) -> BaseAdapter:
    cls = ADAPTERS[name]
    return cls()
