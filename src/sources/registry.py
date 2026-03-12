from __future__ import annotations

from typing import Dict, Type

from .base import BaseAdapter
from .adapters.maennedorf_portal import MaennedorfPortalAdapter
from .adapters.eventbrite import EventbriteAdapter
from .adapters.familienclub_herrliberg import FamilienclubHerrlibergAdapter
from .adapters.gemeinde_herrliberg import GemeindeHerrlibergAdapter
from .adapters.kirchenweb import KirchenwebAdapter


ADAPTERS: Dict[str, Type[BaseAdapter]] = {
    "maennedorf_portal": MaennedorfPortalAdapter,
    "eventbrite": EventbriteAdapter,
    "familienclub_herrliberg": FamilienclubHerrlibergAdapter,
    "gemeinde_herrliberg": GemeindeHerrlibergAdapter,
    "kirchenweb": KirchenwebAdapter,
}


def get_adapter(name: str) -> BaseAdapter:
    cls = ADAPTERS[name]
    return cls()
