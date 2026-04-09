# Databricks OAuth Connector — Design Spec
**Date:** 2026-04-09
**Status:** Approved

---

## Goal

Replace the current `databricks_connector` package engine with one that:
- Uses the official Databricks SQL API instead of the internal `/ajax-api/` endpoint
- Authenticates via OAuth (stable, won't break on Databricks updates)
- Keeps the same public interface: `query(sql)`, `AuthRequiredError`, `DatabricksQueryError`
- Requires no user interaction after initial setup (automatic headless re-auth via saved Google SSO cookies)
- Handles Google cookie expiry gracefully by opening a visible browser for manual re-login

Rollback: previous version is stored in GitHub and can be restored if the new engine fails.

---

## Public Interface (unchanged)

```python
from databricks_connector import query, DatabricksQueryError, AuthRequiredError

df = query("SELECT * FROM prd_refined.schema.table LIMIT 100")
```

`AuthRequiredError` is raised only when Google cookies have also expired and the user cancels or does not complete the visible browser login.

---

## Configuration

All connection values are read from `~/.databricks_connector/config.json`. Nothing is hardcoded.

```json
{
  "host": "dbc-6f0786a7-8ba5.cloud.databricks.com",
  "http_path": "/sql/1.0/warehouses/3de9aee76c2f16f1"
}
```

- `host` — Databricks workspace hostname (no `https://` prefix)
- `http_path` — full SQL warehouse path; `warehouse_id` is derived as `http_path.split("/")[-1]`

The `databricks-setup` skill writes this file and must be updated to write `http_path` instead of `warehouse_id`.

---

## Session Files

```
~/.databricks_connector/config.json          ← host + http_path (written by databricks-setup skill)
~/.databricks_connector/google_session.json  ← Google SSO cookies (replaces session.json)
~/.databricks/token-cache.json               ← OAuth access + refresh tokens (managed by SDK)
```

---

## Auth Flow

```
query(sql)
  │
  ├─ 1. Read access token from ~/.databricks/token-cache.json
  │       └─ if valid → sql.connect(access_token=token) → DataFrame ✓
  │
  ├─ 2. Token expired → SDK silently refreshes via refresh token (auto-rotated)
  │       └─ if refresh token valid → new tokens cached → retry ✓
  │
  └─ 3. Refresh token also expired → full re-auth needed
          └─ headless_reauth():
                Load google_session.json into headless Playwright (Chromium)
                Navigate to Databricks OAuth PKCE authorization URL
                Google cookies → SSO auto-completes → OAuth redirect captured
                New access + refresh tokens written to token-cache.json
                Retry query ✓

                If Google cookies expired:
                  Open VISIBLE browser (same UX as current setup_auth.py)
                  User logs in with Google manually
                  Google cookies re-saved to google_session.json
                  OAuth flow completes → tokens cached
                  Retry query ✓
```

**Key improvement over current connector:** Playwright is only launched during re-auth (steps 2–3), not on every query. Normal queries (step 1) are direct HTTP calls via the official connector — faster and more stable.

---

## File Structure

Replacing files 1-for-1. Package shape stays identical.

```
databricks_connector/
├── __init__.py          exports: query, DatabricksQueryError, AuthRequiredError (unchanged)
├── auth.py              replaces browser_auth.py
│                          - config.json loading: get_host(), get_http_path()
│                          - warehouse_id derived from http_path
│                          - google_session.json path + existence check
│                          - token-cache.json read/write helpers
│                          - AuthRequiredError definition
├── google_auth.py       NEW — handles all re-auth logic
│                          - headless_reauth(): Playwright + Google cookies → OAuth PKCE flow
│                          - visible_browser_login(): fallback when Google cookies expired
│                          - writes new tokens to token-cache.json
├── query.py             replaces browser_query.py
│                          - query(sql, http_path=None): public function
│                          - uses databricks.sql.connect(access_token=token)
│                          - on 401/AuthRequiredError: calls google_auth.headless_reauth() + retries once
│                          - DatabricksQueryError definition
└── session_check.py     updated — checks token-cache.json validity instead of cookie validity

setup_auth.py            updated — visible browser login, saves Google cookies + initial OAuth token
check_session.py         updated — checks token-cache.json instead of session.json
```

**Deleted:** `browser_auth.py`, `browser_query.py`

---

## OAuth PKCE Flow (headless)

Used in `google_auth.headless_reauth()`:

1. Generate PKCE `code_verifier` + `code_challenge` (SHA-256, base64url)
2. Build Databricks authorization URL:
   `https://{host}/oidc/v1/authorize?response_type=code&client_id=databricks-cli&redirect_uri=http://localhost:{port}/callback&scope=sql+offline_access&code_challenge={challenge}&code_challenge_method=S256&state={state}`
3. Launch headless Chromium with `google_session.json` loaded as storage state
4. Navigate to authorization URL → Google SSO auto-completes via cookies
5. Intercept redirect to `localhost:{port}/callback` via `page.wait_for_url()`
6. Parse `code` from redirect URL
7. POST to `https://{host}/oidc/v1/token` with code + verifier → get `access_token` + `refresh_token`
8. Write tokens to `~/.databricks/token-cache.json`

---

## setup_auth.py (updated)

1. Verify `config.json` exists (same check as today)
2. Remove stale `google_session.json` if present
3. Open **visible** Playwright browser → navigate to Databricks login page
4. User logs in with Google (`@kavak.com` SSO)
5. Wait for successful redirect to SQL warehouses page (same success signal as today)
6. Save Google domain cookies to `google_session.json`
7. Immediately run `headless_reauth()` to get the first OAuth token cached
8. Print confirmation

---

## Dependencies

No new dependencies added. Existing ones:
- `playwright` — kept for headless re-auth (no longer needed per-query)
- `databricks-sql-connector` — replaces the JS fetch approach
- `databricks-sdk` — OAuth PKCE + token cache management
- `pandas` — unchanged

---

## Error Handling

| Situation | Behavior |
|---|---|
| Token valid | Silent, fast query |
| Token expired, refresh token valid | Silent token refresh via SDK, retry |
| Both tokens expired, Google cookies valid | Headless Playwright re-auth, retry |
| Google cookies expired | Visible browser login prompt, then retry |
| Query SQL error | Raise `DatabricksQueryError` with message |
| config.json missing | Raise `RuntimeError` with setup instructions |

---

## What Does NOT Change

- `config.json` location and format (except `warehouse_id` → `http_path`)
- Public API: `query()`, `AuthRequiredError`, `DatabricksQueryError`
- `setup_auth.py` entry point and UX (visible browser, same flow for user)
- `check_session.py` entry point
- Tests structure (`tests/` directory)
- The `databricks-setup` skill flow (minor update to write `http_path`)
