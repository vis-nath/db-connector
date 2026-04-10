# Databricks SDK Auth Connector — Design Spec
**Date:** 2026-04-10
**Status:** Approved
**Supersedes:** `2026-04-09-databricks-oauth-connector-design.md`

---

## Goal

Simplify the `databricks_connector` auth engine by delegating entirely to the Databricks SDK's built-in `external-browser` OAuth flow. The previous design used custom PKCE + Playwright + Google cookies, which failed because the `databricks-cli` client_id on this workspace does not accept arbitrary `localhost:{port}` redirect URIs.

The SDK handles PKCE, the callback server, token caching, and silent refresh internally — no custom auth code needed.

---

## Public Interface (unchanged)

```python
from databricks_connector import query, DatabricksQueryError, AuthRequiredError

df = query("SELECT * FROM prd_refined.schema.table LIMIT 100")
```

---

## Configuration

Read from `~/.databricks_connector/config.json` (unchanged):

```json
{
  "host": "dbc-6f0786a7-8ba5.cloud.databricks.com",
  "http_path": "/sql/1.0/warehouses/3de9aee76c2f16f1"
}
```

---

## Session Files

```
~/.databricks_connector/config.json   ← host + http_path (written by databricks-setup skill)
~/.databricks/token-cache.json        ← OAuth access + refresh tokens (managed entirely by SDK)
```

`google_session.json` is no longer used and is not created.

---

## Auth Flow

```
setup_auth.py (run once, or when refresh token expires)
  └─ WorkspaceClient(host=host, auth_type="external-browser")
       SDK starts local callback server on a registered port
       Opens user's default browser → Databricks login page
       User logs in with Google SSO (@kavak.com)
       Browser redirects to SDK callback → code exchanged for tokens
       Tokens written to ~/.databricks/token-cache.json

query(sql)
  └─ sql.connect(server_hostname, http_path, auth_type="external-browser")
       ├─ Token in cache, valid → run query directly ✓
       ├─ Access token expired, refresh token valid → SDK silently refreshes → run query ✓
       └─ Both expired → sdk raises → AuthRequiredError("Run setup_auth.py") raised
```

Playwright is no longer used anywhere in the auth flow.

---

## File Structure

```
databricks_connector/
├── __init__.py          exports: query, DatabricksQueryError, AuthRequiredError (unchanged)
├── auth.py              config loading only: get_host(), get_http_path(), get_warehouse_id()
│                          AuthRequiredError definition
├── query.py             query(sql, http_path=None)
│                          sql.connect(auth_type="external-browser")
│                          DatabricksQueryError definition
├── session_check.py     check_session() → reads ~/.databricks/token-cache.json
│                          returns True if access token present and not expired
└── cache.py             unchanged — query result caching

setup_auth.py            WorkspaceClient(auth_type="external-browser") → primes token cache
check_session.py         entry point → calls session_check()
```

**Deleted:** `google_auth.py` (entire file — custom PKCE, Playwright, Google cookie logic)

---

## setup_auth.py

1. Verify `config.json` exists
2. Call `WorkspaceClient(config=Config(host=f"https://{host}", auth_type="external-browser"))`
   — SDK opens browser, user logs in, tokens cached
3. Verify login succeeded by calling `w.current_user.me()`
4. Print confirmation with username

No Playwright. No custom HTTP server. No Google cookies.

---

## query.py

```python
def query(sql_query, http_path=None):
    host = get_host()
    http_path = http_path or get_http_path()
    try:
        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            auth_type="external-browser",
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [d[0] for d in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=columns)
    except Exception as auth_err:
        # Catch SDK auth errors (exact exception type confirmed during implementation
        # by inspecting what databricks-sdk raises when external-browser flow fails
        # in a non-interactive context — e.g., PermissionDenied, requests.HTTPError,
        # or a databricks.sdk.errors class). Only re-raise as AuthRequiredError
        # if the error is auth-related; otherwise fall through to DatabricksQueryError.
        if _is_auth_error(auth_err):
            raise AuthRequiredError("Run setup_auth.py to re-authenticate.") from auth_err
        raise DatabricksQueryError(str(auth_err)) from auth_err
    except Exception as e:
        raise DatabricksQueryError(str(e)) from e
```

---

## Error Handling

| Situation | Behavior |
|---|---|
| Token in cache, valid | Silent, fast query |
| Access token expired, refresh valid | SDK silently refreshes, no browser |
| Both tokens expired | `AuthRequiredError` raised — user runs `setup_auth.py` |
| SQL error | `DatabricksQueryError` with message |
| `config.json` missing | `RuntimeError` with setup instructions |

---

## Dependencies

Removed: Playwright is no longer a required dependency for auth (can be removed from `requirements.txt` if not used elsewhere).

Kept:
- `databricks-sql-connector>=3.0.0`
- `databricks-sdk`
- `pandas`

---

## What Does NOT Change

- `config.json` location and format
- Public API: `query()`, `AuthRequiredError`, `DatabricksQueryError`
- `setup_auth.py` entry point (simplified internally)
- `check_session.py` entry point
- `cache.py`
- `auth.py` (config loading functions)
- Tests structure (`tests/` directory)
