from __future__ import annotations

from typing import Dict, Type

from .base import BaseAdapter
from .adapters.maennedorf_portal import MaennedorfPortalAdapter
from .adapters.eventbrite import EventbriteAdapter
from .adapters.familienclub_herrliberg import FamilienclubHerrlibergAdapter
from .adapters.gemeinde_herrliberg import GemeindeHerrlibergAdapter
from .adapters.kirchenweb import KirchenwebAdapter
from .adapters.frauenverein_maennedorf import FrauenvereinMaennedorfAdapter
from .adapters.ref_kirche_maennedorf import RefKircheMaennedorfAdapter
from .adapters.fluugepilz import FluugepilzAdapter
from .adapters.clubdesk import ClubDeskAdapter
from .adapters.govis import GovisAdapter
from .adapters.lanterne_magique import LanterneMagiqueAdapter


ADAPTERS: Dict[str, Type[BaseAdapter]] = {
    "lanterne_magique": LanterneMagiqueAdapter,
    "maennedorf_portal": MaennedorfPortalAdapter,
    "clubdesk": ClubDeskAdapter,
    "govis": GovisAdapter,
    "eventbrite": EventbriteAdapter,
    "familienclub_herrliberg": FamilienclubHerrlibergAdapter,
    "gemeinde_herrliberg": GemeindeHerrlibergAdapter,
    "kirchenweb": KirchenwebAdapter,
    "frauenverein_maennedorf": FrauenvereinMaennedorfAdapter,
    "ref_kirche_maennedorf": RefKircheMaennedorfAdapter,
    "fluugepilz": FluugepilzAdapter,
}


def get_adapter(name: str) -> BaseAdapter:
    cls = ADAPTERS[name]
    return cls()
