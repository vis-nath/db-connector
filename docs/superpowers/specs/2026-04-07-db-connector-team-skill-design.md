# Design: db-connector Team Skill + GitHub Distribution

**Date:** 2026-04-07
**Status:** Approved
**Author:** natanahelbaruch

---

## Problem

The team needs to query Databricks from Claude Code without API tokens or OAuth access. A working browser-session-based connector exists locally. The goal is to make it replicable by all teammates (non-technical analysts on WSL2) with minimal friction.

---

## Constraints

- No Databricks API tokens available (admin permission required)
- No OAuth setup possible
- All teammates use Claude Code CLI on WSL2 (Windows 11 with WSLg)
- Each person manages their own `~/.claude/plugins/` — no shared plugin repo
- Team members are non-technical — instructions must be in plain Spanish
- Databricks HOST and WAREHOUSE_ID must NOT be in the public repo

---

## Distribution Architecture

```
Public GitHub repo: vis-nath/db-connector
  └─ Pure Python code — no URLs, no IDs, no sessions
  └─ HOST + WAREHOUSE_ID read from ~/.databricks_connector/config.json

Skill file: databricks-setup.md  (shared internally via Slack/email)
  └─ Contains HOST + WAREHOUSE_ID (Kavak-internal, never committed)
  └─ Full setup instructions in Spanish
  └─ Query trigger logic
  └─ Session expiry re-auth flow
```

## What Goes in the Repo vs the Skill

| Item | Repo | Skill |
|---|---|---|
| `databricks_connector/` Python package | ✅ | — |
| `setup_auth.py` | ✅ | — |
| `requirements.txt` | ✅ | — |
| Databricks HOST URL | ❌ | ✅ |
| Warehouse ID | ❌ | ✅ |
| Setup instructions (Spanish) | — | ✅ |
| Query trigger logic | — | ✅ |

---

## Python Package Config

`browser_auth.py` reads from `~/.databricks_connector/config.json`:

```json
{
  "host": "https://dbc-XXXXXXXX.cloud.databricks.com",
  "warehouse_id": "XXXXXXXXXXXXXXXX"
}
```

Written by the skill during setup. Never committed.

---

## Skill: Two Modes

### Mode 1 — `/databricks-setup`
Full installation for a new teammate. Steps:
1. Clone repo to `~/projects/databricks_connector`
2. Install Python deps + Playwright Chromium
3. Write `~/.databricks_connector/config.json` with HOST + WAREHOUSE_ID
4. Run `python3 setup_auth.py` — Chromium opens, user logs in via Kavak SSO
5. ⚠️ User must press Enter in terminal AFTER workspace fully loads
6. Run `SELECT 1` verification query

### Mode 2 — Auto-trigger on query intent
Triggers when: user says *query, consulta, datos, tabla, Databricks, SQL* or asks a data question.

Skill asks: *"¿Los datos que necesitas están en Databricks? Si es así, puedo consultarlos directamente."*

If yes → writes and runs query → returns `pandas.DataFrame` → Claude handles it.

**DataFrame contract:**
- `query("SELECT ...")` returns `pandas.DataFrame`
- Claude can immediately use `.head()`, `.groupby()`, `.shape`, `.describe()`, etc.
- Results live in the session — no files saved unless user asks

---

## Non-Technical UX Principles

- Steps labeled **"Paso 1 de 5"**
- Every command is a copy-paste block
- Browser login step explains what they'll see in plain language
- Enter step is highlighted with ⚠️ warning
- Session expiry message in Spanish with exact re-auth command
- No jargon — "ventana de Chrome", not "browser context"

---

## Session Expiry Handling

- `AuthRequiredError` caught by the skill
- Spanish message: *"Tu sesión expiró. Solo necesitas volver a iniciar sesión."*
- Re-auth command: `python3 ~/projects/databricks_connector/setup_auth.py`
- No reinstall needed — only the login step repeats

---

## Scraping Strategy (for future reference)

See `vault-claude-kavak/Projects/databricks_connector/scraping_strategy.md` for the full technical breakdown of how the browser session auth was reverse-engineered.

Summary:
- **Public REST API** (`/api/2.0/`) → requires Bearer token — blocked
- **Internal web API** (`/ajax-api/2.0/`) → uses cookies + CSRF token — works
- CSRF token fetched from `/auth/session/info` on each request
- Playwright headless Chromium loads saved `storage_state`, navigates to HOST, runs `fetch()` in-page

---

## Files to Create/Modify

| File | Action |
|---|---|
| `databricks_connector/browser_auth.py` | Modify — read HOST+WAREHOUSE from config.json |
| `databricks_connector/browser_query.py` | No change needed |
| `setup_auth.py` | Modify — write config.json during setup |
| `.gitignore` | Verify config.json + session.json excluded |
| `~/.claude/plugins/skills/databricks-setup.md` | Create — the skill |
| `vault-claude-kavak/Projects/databricks_connector/` | Update all 4 notes |
