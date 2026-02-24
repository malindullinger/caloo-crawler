# Caloo — Minimum Trust Standard (MTS-v1.2)

Status: Locked
Scope: Feed eligibility, governance, confidence baseline
Phase: Phase 9 — Feed Integrity Layer

---

## Purpose

The Minimum Trust Standard (MTS) defines the minimum information required for Caloo to responsibly recommend an event or activity to families.

MTS determines:

- Feed eligibility
- trust_status state
- Review triggers
- Confidence scoring baseline
- UI assumptions
- Governance enforcement

This is a product-level contract, not a UI detail.

---

# Canonical Layers

Caloo data model layers:

- Happening → Concept / identity
- Offering → Schedule container
- Occurrence → Specific dated instance
- Organizer → Trust entity

Fields must belong to the correct layer to avoid drift.

---

# LEVEL A — Blocking (Feed Eligibility Core)

If any Level A field is missing or invalid:
→ trust_status = suppressed
→ OR trust_status = needs_review (depending on severity)
→ Feed may suppress item

## Identity (Happening)

- title (required, non-placeholder)
- short_summary (min length threshold)
- category (≥ 1 primary category)
- event_space (indoor | outdoor | both)
- age_relevance (min_age or explicit all_ages)

## Organizer

- organizer entity must exist and be linked

## Occurrence

- start_at (valid datetime)
- end_at (valid or deterministically derived)
- location (address + geo required)

## Planning

- registration_status explicit (required | not_required | unknown)
- add_to_calendar capability must exist
- price_type explicit:
    - free
    - fixed
    - range
    - donation
    - member_only
    - unknown

If price_type = unknown:
→ trust_status = needs_review
→ visible in feed
→ confidence penalty

---

# LEVEL B — High Confidence Boosters

Missing Level B fields does NOT suppress feed visibility.
They reduce confidence_score.

- ≥ 1 valid image
- explicit duration clarity
- organizer logo
- prerequisites clarity
- accessibility notes
- structured price clarity beyond minimal type

---

# LEVEL C — Enrichment (Modular)

These fields enhance experience but never affect eligibility:

- cancellation policy
- what to prepare
- how you'll enjoy your time
- similar events
- more from organizer
- video
- seasonal decorations
- extended organizer bio

---

# trust_status ENUM

valid
needs_review
suppressed

---

# trust_status Rules

valid:
- All Level A satisfied
- No integrity violations

needs_review:
- price_type = unknown
- registration_status = unknown
- summary barely meets threshold
- missing image
- conflicting source data
- derived end_at without strong source clarity

suppressed:
- missing title
- missing start_at
- missing location entirely
- no organizer
- invariant violation
- invalid datetime structure

---

# Feed Eligibility Rule (Phase 9)

A display occurrence is eligible if:

- trust_status != 'suppressed'
- end_at > now()
- start_at exists
- location exists

---

# Time State Contract

Display occurrence exposes:

time_state:
- upcoming
- ongoing
- ending_soon (≤ 20 minutes remaining)

An occurrence disappears when:
now >= end_at

---

# Confidence Score (Baseline v1)

Confidence starts at 100.

Penalties:

- -20 missing image
- -15 price_type unknown
- -15 missing prerequisites clarity
- -10 no organizer logo
- -10 summary barely valid
- -5 derived end_at
- -5 registration_status unknown

Confidence affects ranking only.
Not eligibility.
