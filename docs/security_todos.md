# Phase 11 — Security Hardening (Future)

Tracked security items to resolve before production hardening.
None of these block current functionality, but they should be addressed
before scaling access or onboarding external integrations.

---

## 1. Row-Level Security (RLS)

### Problem

Supabase tables currently have RLS disabled or permissive policies.
Any authenticated (or anon) client with the API key can read/write
all rows.

### Tables needing RLS review

| Table | Current state | Required |
|-------|--------------|----------|
| `happening` | No RLS | Read: public. Write: service_role only. |
| `offering` | No RLS | Read: public. Write: service_role only. |
| `occurrence` | No RLS | Read: public. Write: service_role only. |
| `source_happenings` | No RLS | Read/Write: service_role only. |
| `merge_run_stats` | No RLS | Read: public (dashboard). Write: service_role only. |
| `canonical_field_history` | No RLS | Read: public (audit). Write: service_role only (via RPC). |
| `canonical_review_outcomes` | No RLS | Read/Write: service_role only. |
| `source_configs` | No RLS | Read/Write: service_role only. |

### Action

- Enable RLS on all tables.
- Create policies: `service_role` gets full access; `anon` gets SELECT
  on feed-facing tables only.
- Verify `feed_cards_view` still works for anon users after RLS is enabled
  (views execute with the definer's permissions — see item 2).

---

## 2. SECURITY DEFINER views

### Problem

`feed_cards_view` and other views may need `SECURITY DEFINER` to allow
anon users to read from tables that have RLS restricting direct access.
However, `SECURITY DEFINER` views run with the **owner's** privileges,
which can bypass RLS entirely if the owner is a superuser.

### Action

- Audit which views use `SECURITY DEFINER` vs `SECURITY INVOKER`.
- If RLS is enabled on underlying tables, ensure the view owner is a
  **restricted role** (not `postgres` superuser).
- Alternatively, create explicit RLS policies that grant `anon` SELECT
  on the specific tables the view joins, and use `SECURITY INVOKER`.

---

## 3. event_interest policies

### Problem

The `event_interest` table (user "likes" / bookmarks) likely needs
per-user RLS policies so users can only read/write their own interests.

### Action

- Add RLS policy: `auth.uid() = user_id` for INSERT/SELECT/DELETE.
- Verify the frontend bookmark feature works after policy is added.

---

## 4. API key exposure

### Problem

The `SUPABASE_ANON_KEY` is used in the frontend and is visible in
browser network requests. This is expected by Supabase's design, but
it means RLS is the **only** access control layer.

### Action

- Ensure item 1 (RLS) is complete before considering this resolved.
- Do NOT use `SUPABASE_SERVICE_ROLE_KEY` in any frontend code.

---

## 5. Service role key rotation

### Action (low priority)

- Rotate `SUPABASE_SERVICE_ROLE_KEY` periodically.
- Ensure all pipeline/crawler scripts use environment variables
  (not hardcoded keys). Currently correct — verify it stays that way.

---

## Priority

| Item | Priority | Blocks |
|------|----------|--------|
| 1. RLS on tables | High | Public launch |
| 2. View security model | High | Depends on #1 |
| 3. event_interest policies | Medium | User features |
| 4. API key audit | Low | Already correct by design |
| 5. Key rotation | Low | Ops hygiene |
