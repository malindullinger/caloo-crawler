# Claude Code rules for this repo (caloo-crawler)

## Scope
- This repo is the crawler + normalization pipeline + Supabase SQL views.
- UI/Lovable code lives elsewhere.

## Must follow
- Do NOT change database semantics without updating docs in DECISIONS.md.
- Prefer minimal diffs: small commits, focused changes.
- Never edit secrets. Never commit `.env` or Supabase keys.
- When adding SQL:
  - Put it under `sql/` and document in `sql/views/README.md`.
  - Use `create or replace view` carefully (prefer new view name when changing columns).

## Workflow
1. Make changes
2. Run formatting/lint/tests (if present)
3. Summarize what changed + why
4. Provide exact commands to run
