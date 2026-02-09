# Caloo — Product Masterfile

## 1. Purpose of This Document

This is an internal, opinionated reference for product intent, constraints, and decision-making. It serves as a canonical source of truth for the founding team and any collaborators working on Caloo.

**This document is:**
- A decision-making guide for product, data, and architecture
- Explicit about what is decided vs. deferred
- Optimized for execution, not external storytelling

**This document is not:**
- A pitch deck or marketing copy
- A complete specification
- A visual design guide

Related documents:
- `CLAUDE.md` — architectural constraints and feed contract
- `DECISIONS.md` — source of truth policy
- `docs/` — detailed research, explorations, archived decisions

---

## 2. One-Line Synthesis

Caloo is a Switzerland-based, behavior-driven planning tool for parents in urban and suburban contexts, helping them confidently choose weekday and weekend activities, events, and children's courses — while reducing cognitive load and planning stress.

---

## 3. Product Vision

Caloo exists to help parents move from "I should plan something" to "we're doing this" with less effort and more confidence.

Parents in urban and suburban Switzerland have access to many activities for their children. The problem is not scarcity — it's uncertainty, fragmentation, and the mental effort required to evaluate options when energy is low.

Caloo reduces that effort. It surfaces relevant, suitable options at the right time, helps parents feel confident in "good enough" decisions, and removes the guilt and stress of weekend planning.

**What success looks like:**
- Parents open Caloo when they want ideas, not when they want to research
- Decisions happen faster with less second-guessing
- Parents feel relief, not pressure, when using the product
- The product adapts to how parents actually behave, not how they aspire to behave

---

## 4. Geographic Scope

### Current focus (v1)

Caloo targets **Switzerland**, with an intentional initial focus on **urban and suburban regions** where families face the highest planning complexity despite high activity availability.

**Primary geographic focus:**
- Zurich city (inner-city districts)
- Zurich metropolitan area (suburban and commuter municipalities)
- Comparable Swiss urban/suburban contexts (e.g., Winterthur, Baden, Zug, Basel suburbs)

### Why this scope

Parents in these regions experience:
- Information overload rather than lack of options
- Fragmented and unreliable discovery channels (flyers, WhatsApp, social media, word-of-mouth)
- Increased coordination complexity (transport, siblings, time windows, social context)

The mental and emotional effort of planning — not physical distance or scarcity — is the dominant pain. These environments also show strong variation in planning behavior depending on family context, energy, and social reliance, making them ideal for a behavior-driven product.

### Explicitly included contexts

- Families living close to home who prefer walkable or short-distance activities
- Families willing to travel ~20–30 minutes for weekend activities
- Families new to a neighbourhood who lack local knowledge and social anchors
- Families with multiple children across different developmental stages

### Explicitly excluded (initially)

- Rural-only contexts with very limited activity density
- International markets (behavior may generalize, but research is Swiss-based)
- Vacation or long-term travel planning as a primary use case

### Design implication

Caloo should be built with a **local-first data model and region-aware prioritization logic**, allowing new geographies to be added later without changing the core behavioral framework.

---

## 5. Core Problems

Parents struggle not because of a lack of activities, but because of:

| Problem | Description |
|---------|-------------|
| **Uncertainty about suitability** | Will this actually work for my child's age, my family's energy level, our logistics? |
| **Effort required to evaluate** | Too many tabs, too much reading, too many variables to weigh |
| **Low energy at planning time** | Planning happens at end of day or end of week, when capacity is lowest |
| **Emotional pressure** | Guilt about "wasting" limited family time; pressure to make weekends count |

These problems compound. A parent who is tired, unsure, and feeling pressure will default to familiar options or avoid deciding altogether — then feel regret.

Caloo's job is to interrupt this cycle by making "good enough" decisions easy and guilt-free.

---

## 6. Behavioral Target Groups

These are behavioral profiles, not demographic personas. Parents may move between them depending on week, energy, or family context. The product should infer and adapt, not require explicit selection.

### Proactive Planners

Parents who actively think ahead because planning reduces stress.

**Typical contexts:**
- Families with predictable rhythms
- Often 1–2 children
- May include families newly settled in a neighbourhood who want orientation
- Often interested in ongoing courses as well as events

**Core needs:**
- Early overview of suitable options
- Clear signals of fit and progression
- Reassurance they're choosing well

**Problem-to-be-solved:**
Overthinking, comparison fatigue, decision validation effort

---

### Flexible Flow Planners

Parents who value light preparation but want to stay adaptable.

**Typical contexts:**
- Families with mixed or changing rhythms
- Comfortable combining spontaneous outings with occasional courses
- Often no strict distinction between weekday and weekend activities

**Core needs:**
- Inspiration without commitment
- Easy-to-skim ideas
- Options that can flex week to week

**Problem-to-be-solved:**
Mild decision friction when context or energy shifts

---

### Reactive Responders

Parents who decide late, based on energy, weather, or mood.

**Typical contexts:**
- High weekday load
- Often younger children
- Less likely to commit to courses, more focused on one-off activities

**Core needs:**
- Fast, nearby, low-effort ideas
- Clear "works today" signals

**Problem-to-be-solved:**
Feeling too late, repeating defaults, weekend regret

---

### Socially Reliant Planners (cross-cutting)

Parents whose confidence depends on social validation.

**Typical contexts:**
- Families new to a neighbourhood
- Parents without a strong local network
- Families preferring shared or social experiences
- Often influenced by who else attends courses or events

**Core needs:**
- Social proof
- Signals that other families go there
- Reduced dependency on asking around

**Problem-to-be-solved:**
Delayed decisions, missed opportunities, uncertainty when alone

---

### Struggling Planners

Parents who want to plan but feel overwhelmed or blocked.

**Typical contexts:**
- Families with multiple children
- Large developmental spans
- High logistical or emotional load
- Difficulty committing to courses despite interest

**Core needs:**
- Extremely simple choices
- Clear suitability indicators
- Emotional relief and permission to keep things simple

**Problem-to-be-solved:**
Planning avoidance, stress, guilt

---

### Contextual Amplifiers

Certain life contexts strongly affect behavior across all profiles:

- Being new to a neighbourhood
- Having multiple children across different developmental stages
- Low parental energy at end of week
- Fixed vs. open weekly schedules

These contexts modify behavior but do not replace the core behavioral profiles.

---

## 7. Product Principles

These principles are practical decision guardrails, not aspirational values. Use them to resolve trade-offs and scope decisions.

**For agents and contributors:** These principles (including accessibility) are first-class constraints. When planning or implementing changes, verify alignment with these guardrails before proceeding.

### "Good enough" over perfect

Design for parents to feel done, not to keep comparing. Prefer quick, confident decisions over completeness.

*Trade-off guidance: If a feature encourages more comparison or delays decision-making, reconsider it.*

---

### Reduce cognitive load at the moment of need

Assume parents plan when energy, time, and patience are limited. Minimize required thinking, choices, and interpretation.

*Trade-off guidance: Fewer options, clearer defaults, and less text are usually better.*

---

### Suitability signals over information density

Make it easy to see "this works for us" at a glance. Prioritize relevance signals over more data.

*Trade-off guidance: A clear "fits your kids' ages" indicator beats a detailed age-range description.*

---

### Adapt to behavioral variation, not just user segments

The same parent behaves differently depending on context (weekday vs. weekend, low vs. high energy, browsing vs. planning). Design for this variation.

*Trade-off guidance: Avoid forcing users into fixed modes or profiles. Adapt implicitly.*

---

### Local-first and context-aware by default

Relevance is primarily driven by proximity — geographic, temporal, and situational.

*Trade-off guidance: Default to nearby and soon. Global or abstract recommendations are not helpful.*

---

### Accessibility by default, complexity by exception

The UI must be usable and understandable for people with diverse cognitive, emotional, and physical needs. Accessibility at the interface level is a baseline requirement, not an enhancement.

*Trade-off guidance: If a design choice makes the interface harder to parse, slower to understand, or more visually noisy, it needs strong justification.*

---

### Accessibility: Scope Clarification

**UI-level accessibility (priority now):**
- Visual clarity and legibility
- Predictable, consistent layouts
- Reduced cognitive load in navigation and interaction
- Calm, forgiving interactions (no pressure, no urgency tricks)
- Compliance with core accessibility standards (contrast, text sizing, screen reader basics)

**Functional accessibility (priority later):**
- Advanced filtering for specific accessibility needs
- Accessibility-specific metadata on activities (e.g., wheelchair access, sensory-friendly)
- Tailored functionality for specific user needs

Accessibility in *how the interface works* is non-negotiable from the beginning. Accessibility in *what functionality is offered* will evolve once the core experience is proven.

---

## 8. Taxonomy & Structural Concepts

### Structural layer: Happening

**Happening** is the unified structural term for any item that can appear as a card in the weekday/weekend feed.

A Happening represents a time- or date-bound idea that a parent can consider for short- to medium-term planning. This is the level at which the feed, database, and crawler operate.

The canonical feed contract is `public.feed_cards_view` (see `CLAUDE.md`).

---

### Conceptual sub-types (not finalized)

Three conceptual sub-types exist for internal alignment. These are **not** finalized user-facing taxonomy:

| Type | Description |
|------|-------------|
| **Event** | One-off or infrequent, time-bound occurrence |
| **Activity** | Flexible, repeatable, lower commitment |
| **Course** | Structured, multi-session program with progression and commitment |

These distinctions help alignment but are not enforced in the current data model or UI.

---

### Courses: Behavioral distinction

While Courses may be represented structurally as Happenings (or series of Happenings), they differ fundamentally in user intent and behavior:

- **User intent:** Long-term planning, higher commitment, different evaluation criteria
- **Decision timing:** Not spontaneous; requires research and scheduling
- **Evaluation criteria:** Progression, instructor quality, schedule fit, cost over time

**Design implication:**
Courses are expected to live under a **separate product entry point** (e.g., a dedicated "Courses" tab) rather than in the weekday/weekend Happenings feed. This avoids conflating short-term activity discovery with long-term commitment decisions.

Courses are a **top-level behavioral concept**, even if they are not yet modeled as such in the data or feed architecture.

---

### Structural relationships

These are relationship concepts between items, not separate user-facing types:

| Concept | Description |
|---------|-------------|
| **Series** | A grouping of related Happenings (e.g., weekly sessions of the same course) |
| **Occurrence** | A specific instance within a series |
| **Trial Session** | A one-off Happening that serves as an entry point to a course or series |

These relationships exist to support deduplication, display logic, and future functionality — not as primary user-facing concepts.

---

### Summary: Two layers

| Layer | Focus | Example |
|-------|-------|---------|
| **Structural / Feed** | What can appear as a card today | Happening |
| **Behavioral / Intent** | Why a parent is searching | Weekend ideas vs. Course search |

This separation explains why Courses are behaviorally first-class while remaining structurally unified with other Happenings for now.

---

## 9. Explicitly Deferred

The following are intentionally out of scope for current product, data, and architectural decisions:

| Topic | Status |
|-------|--------|
| **Monetization / business model** | Deferred. Should not influence current decisions. Focus is on correctness, usability, and value creation. |
| **Final taxonomy** | Deferred. Event/Activity/Course distinctions are working definitions, not finalized. |
| **User-facing type labels** | Deferred. No commitment to exposing sub-types in UI yet. |
| **Personalization / auth-based features** | Deferred. Public feed is the current contract. |
| **Advanced accessibility features** | Deferred. UI accessibility is priority; functional accessibility evolves later. |
| **Dedupe migration to SQL** | Deferred. Frontend dedupe is authoritative for now (see `CLAUDE.md`). |
| **Pricing, booking links, images** | Deferred. Not part of current feed contract. |
| **Series semantics (final)** | Deferred. Current approach is working, not locked. |

---

## 10. Relationship to Other Documents

| Document | Purpose |
|----------|---------|
| `CLAUDE.md` | Architectural constraints, feed contract, technical guardrails |
| `DECISIONS.md` | Source of truth policy (GitHub > Supabase > UI) |
| `docs/architecture-notes.md` | Detailed data architecture notes |
| `docs/` (other) | Research summaries, explorations, archived decisions |

`PRODUCT.md` is the canonical entry point for product intent. Other documents provide supporting detail or domain-specific constraints.

When in doubt:
- **What are we building and why?** → `PRODUCT.md`
- **How does the data/feed work?** → `CLAUDE.md`
- **Where does truth live?** → `DECISIONS.md`
