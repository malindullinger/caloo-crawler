# src/extraction/organizer/extractors/html_labeled_field.py
"""
HtmlLabeledFieldExtractor — generalized from maennedorf_portal._extract_organizer_name.

Searches HTML for explicit organizer labels using three strategies:
  1. Text line scan for "Veranstalter: X" / "Organisator: X" / "Organisation: X"
  2. <dt>Veranstalter</dt><dd>Name</dd> pattern (immediate next sibling only)
  3. <th>Veranstalter</th><td>Name</td> pattern (immediate next sibling only)

Source-agnostic: works on any BeautifulSoup tree.
"""
from __future__ import annotations

import re
from typing import List

from bs4 import BeautifulSoup

from ..normalize import is_junk_organizer_name, normalize_organizer_name
from ..types import EvidenceType, OrganizerCandidate

_ORGANIZER_LABEL_RE = re.compile(
    r"^(Veranstalter|Organisator|Organisation)\s*:\s*",
    re.IGNORECASE,
)

_ORGANIZER_LABELS: frozenset[str] = frozenset({
    "veranstalter", "organisator", "organisation",
})


def _get_container(soup: BeautifulSoup) -> BeautifulSoup:
    """Scope extraction to main content area, matching existing adapter convention."""
    return soup.select_one("main") or soup.select_one("article") or soup


class HtmlLabeledFieldExtractor:
    """
    Extract organizer candidates from explicit HTML labels.

    Confidence model:
      - dt/dd structured pair:        90
      - th/td table pair:             90
      - inline text "Label: Value":   85
    """

    def extract(self, soup: BeautifulSoup) -> List[OrganizerCandidate]:
        """
        Return all organizer candidates found in the HTML.

        May return 0, 1, or multiple candidates.
        All candidates are normalized and junk-filtered before returning.
        """
        candidates: List[OrganizerCandidate] = []
        candidates.extend(self._extract_from_dt_dd(soup))
        candidates.extend(self._extract_from_th_td(soup))
        candidates.extend(self._extract_from_text_lines(soup))
        return candidates

    def _extract_from_text_lines(
        self, soup: BeautifulSoup,
    ) -> List[OrganizerCandidate]:
        """Strategy 1: scan text lines for 'Label: Value' patterns."""
        container = _get_container(soup)
        text = container.get_text("\n", strip=True)
        candidates: List[OrganizerCandidate] = []

        for line in text.split("\n"):
            line = line.strip()
            m = _ORGANIZER_LABEL_RE.match(line)
            if not m:
                continue
            raw_value = line[m.end():].strip()
            if not raw_value:
                continue
            name = normalize_organizer_name(raw_value)
            if not name or is_junk_organizer_name(name):
                continue
            label = m.group(1)
            candidates.append(OrganizerCandidate(
                name=name,
                confidence=85,
                evidence_type=EvidenceType.HTML_LABELED_FIELD,
                evidence_ref=f"text_line:{label}",
                evidence_excerpt=line[:200],
            ))

        return candidates

    def _extract_from_dt_dd(
        self, soup: BeautifulSoup,
    ) -> List[OrganizerCandidate]:
        """Strategy 2: <dt>Label</dt><dd>Value</dd> — immediate next sibling only."""
        container = _get_container(soup)
        candidates: List[OrganizerCandidate] = []

        for dt in container.select("dt"):
            label = (dt.get_text(strip=True) or "").strip().rstrip(":")
            if label.lower() not in _ORGANIZER_LABELS:
                continue
            # Require immediate next sibling element to be <dd>
            sib = dt.find_next_sibling()
            if not sib or sib.name != "dd":
                continue
            raw_value = sib.get_text(strip=True).strip()
            if not raw_value:
                continue
            name = normalize_organizer_name(raw_value)
            if not name or is_junk_organizer_name(name):
                continue
            candidates.append(OrganizerCandidate(
                name=name,
                confidence=90,
                evidence_type=EvidenceType.HTML_LABELED_FIELD,
                evidence_ref=f"dt/dd:{label}",
                evidence_excerpt=f"<dt>{label}</dt><dd>{raw_value}</dd>"[:200],
            ))

        return candidates

    def _extract_from_th_td(
        self, soup: BeautifulSoup,
    ) -> List[OrganizerCandidate]:
        """Strategy 3: <th>Label</th><td>Value</td> — immediate next sibling only."""
        container = _get_container(soup)
        candidates: List[OrganizerCandidate] = []

        for th in container.select("th"):
            label = (th.get_text(strip=True) or "").strip().rstrip(":")
            if label.lower() not in _ORGANIZER_LABELS:
                continue
            # Require immediate next sibling element to be <td>
            sib = th.find_next_sibling()
            if not sib or sib.name != "td":
                continue
            raw_value = sib.get_text(strip=True).strip()
            if not raw_value:
                continue
            name = normalize_organizer_name(raw_value)
            if not name or is_junk_organizer_name(name):
                continue
            candidates.append(OrganizerCandidate(
                name=name,
                confidence=90,
                evidence_type=EvidenceType.HTML_LABELED_FIELD,
                evidence_ref=f"th/td:{label}",
                evidence_excerpt=f"<th>{label}</th><td>{raw_value}</td>"[:200],
            ))

        return candidates
