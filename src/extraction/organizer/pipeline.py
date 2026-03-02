# src/extraction/organizer/pipeline.py
"""
OrganizerExtractionPipeline — orchestrator.

Runs extractors in order, collects candidates, selects winner.
Pure, deterministic, no I/O, no side effects.
"""
from __future__ import annotations

from typing import List, Optional

from bs4 import BeautifulSoup

from .extractors.external_link import ExternalLinkExtractor
from .extractors.html_labeled_field import HtmlLabeledFieldExtractor
from .extractors.pdf_text import PdfTextExtractor
from .selection import select_winner, sort_candidates
from .types import OrganizerCandidate, OrganizerResult


class OrganizerExtractionPipeline:
    """
    Source-agnostic organizer extraction pipeline.

    Usage::

        pipeline = OrganizerExtractionPipeline()
        result = pipeline.run(soup=soup, base_url=url)
        if result.winner:
            organizer_name = result.winner.name
            extraction_confidence = result.winner.confidence
    """

    def __init__(self) -> None:
        self._html_extractor = HtmlLabeledFieldExtractor()
        self._pdf_extractor = PdfTextExtractor()
        self._link_extractor = ExternalLinkExtractor()

    def run(
        self,
        *,
        soup: Optional[BeautifulSoup] = None,
        base_url: str = "",
        pdf_text: Optional[str] = None,
        outbound_urls: Optional[List[str]] = None,
    ) -> OrganizerResult:
        """
        Run all extractors and return the pipeline result.

        Parameters:
            soup: Parsed HTML (required for HTML and link extractors)
            base_url: Page URL (for resolving external links)
            pdf_text: Pre-extracted PDF text (Phase 1: typically None)
            outbound_urls: Pre-extracted outbound URLs (reserved for future use)

        Returns:
            OrganizerResult with winner and all_candidates.
        """
        all_candidates: List[OrganizerCandidate] = []

        # Stage 1: HTML labeled fields
        if soup is not None:
            all_candidates.extend(self._html_extractor.extract(soup))

        # Stage 2: PDF text
        if pdf_text:
            all_candidates.extend(self._pdf_extractor.extract(pdf_text))

        # Stage 3: External links (requires both soup and base_url)
        if soup is not None and base_url:
            all_candidates.extend(
                self._link_extractor.extract(
                    soup, base_url, outbound_urls=outbound_urls,
                )
            )

        # Deterministic selection
        sorted_candidates = sort_candidates(all_candidates)
        winner = select_winner(all_candidates)

        return OrganizerResult(
            winner=winner,
            all_candidates=sorted_candidates,
        )
