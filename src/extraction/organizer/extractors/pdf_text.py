# src/extraction/organizer/extractors/pdf_text.py
"""
PdfTextExtractor — extract organizer from pre-fetched PDF text.

Phase 1 contract:
  - Accepts pre-extracted plain text only (no fetching, no file I/O)
  - Returns empty list if pdf_text is None or empty
  - Uses same label patterns as HtmlLabeledFieldExtractor
  - Confidence: 75 (text extraction from PDFs is lower fidelity than HTML)
"""
from __future__ import annotations

import re
from typing import List, Optional

from ..normalize import is_junk_organizer_name, normalize_organizer_name
from ..types import EvidenceType, OrganizerCandidate

_ORGANIZER_LABEL_RE = re.compile(
    r"^(Veranstalter|Organisator|Organisation)\s*:\s*",
    re.IGNORECASE,
)


class PdfTextExtractor:
    """
    Extract organizer candidates from pre-fetched PDF text content.

    Does NOT fetch PDFs or perform file I/O.
    Does NOT use OCR or AI inference.
    """

    def extract(self, pdf_text: Optional[str]) -> List[OrganizerCandidate]:
        """
        Scan pre-extracted PDF text for organizer label patterns.

        Parameters:
            pdf_text: Plain text extracted from PDF. None if not available.

        Returns:
            List of OrganizerCandidate. Empty if pdf_text is None/empty.
        """
        if not pdf_text or not pdf_text.strip():
            return []

        candidates: List[OrganizerCandidate] = []

        for line in pdf_text.split("\n"):
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
                confidence=75,
                evidence_type=EvidenceType.PDF_TEXT,
                evidence_ref=f"pdf_line:{label}",
                evidence_excerpt=line[:200],
            ))

        return candidates
