# SDK Auth Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace custom PKCE + Playwright auth with the Databricks SDK's built-in `external-browser` OAuth flow.

**Architecture:** `setup_auth.py` calls `WorkspaceClient(auth_type="external-browser")` once to prime the SDK token cache. All subsequent `query()` calls use `sql.connect(auth_type="external-browser")`, which the SDK resolves from cache silently. No custom PKCE, no Playwright, no Google cookie management.

**Tech Stack:** `databricks-sql-connector>=3.0.0`, `databricks-sdk>=0.20.0`, `pandas`, `pytest`

---

## File Map

| File | Action | Reason |
|---|---|---|
| `databricks_connector/query.py` | Rewrite | Remove google_auth imports; use `auth_type="external-browser"` |
| `databricks_connector/google_auth.py` | Delete | Entire custom PKCE + Playwright auth replaced by SDK |
| `databricks_connector/auth.py` | Strip | Remove session file constants and functions no longer used |
| `databricks_connector/session_check.py` | Rewrite | Point to SDK token cache at `~/.databricks/token-cache.json` |
| `setup_auth.py` | Rewrite | Use SDK WorkspaceClient instead of Playwright browser |
| `requirements.txt` | Update | Remove `playwright`, `requests` (only used in deleted code) |
| `tests/test_query.py` | Rewrite | New interface: mock `sql.connect` directly |
| `tests/test_google_auth.py` | Delete | Tests for deleted module |
| `tests/test_auth.py` | Trim | Remove 4 tests for deleted functions |
| `tests/test_session_check.py` | Rewrite | Patch `_SDK_TOKEN_CACHE` instead of `TOKEN_CACHE_FILE` |
| `databricks_connector/client.py` | No change | Already imports from `query.py` correctly |
| `databricks_connector/__init__.py` | No change | Exports remain the same |
| `check_session.py` | No change | Entry point unchanged |

**Expected test count:** starts at 30 → ends at 18 (8 google_auth tests deleted, 4 auth tests trimmed, counts otherwise unchanged).

---

## Task 1: Rewrite `query.py` and `test_query.py`

**Files:**
- Modify: `databricks_connector/query.py`
- Modify: `tests/test_query.py`

- [ ] **Step 1: Write the new failing tests**

Replace the entire contents of `tests/test_query.py`:

```python
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from databricks.sql.exc import Error as SqlError

from databricks_connector.query import query, DatabricksQueryError
from databricks_connector.auth import AuthRequiredError


def _make_cursor(rows, columns):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = [(col, None, None, None, None, None, None) for col in columns]
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _make_connection(cursor):
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


def test_query_returns_dataframe_on_success():
    cursor = _make_cursor([("a", 1), ("b", 2)], ["name", "value"])
    conn = _make_connection(cursor)
    with patch("databricks_connector.query.sql.connect", return_value=conn), \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"):
        df = query("SELECT 1")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["name", "value"]
    assert len(df) == 2


def test_query_passes_auth_type_external_browser():
    cursor = _make_cursor([], ["col"])
    conn = _make_connection(cursor)
    with patch("databricks_connector.query.sql.connect", return_value=conn) as mock_connect, \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"):
        query("SELECT 1")
    assert mock_connect.call_args[1]["auth_type"] == "external-browser"


def test_query_uses_custom_http_path():
    cursor = _make_cursor([], ["col"])
    conn = _make_connection(cursor)
    with patch("databricks_connector.query.sql.connect", return_value=conn) as mock_connect, \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/default"):
        query("SELECT 1", http_path="/custom/path")
    assert mock_connect.call_args[1]["http_path"] == "/custom/path"


def test_query_raises_auth_error_on_401():
    with patch("databricks_connector.query.sql.connect", side_effect=SqlError("401 Unauthorized")), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"):
        with pytest.raises(AuthRequiredError):
            query("SELECT 1")


def test_query_raises_databricks_query_error_on_sql_failure():
    with patch("databricks_connector.query.sql.connect", side_effect=SqlError("Table not found")), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"):
        with pytest.raises(DatabricksQueryError, match="Table not found"):
            query("SELECT * FROM nonexistent")
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python3 -m pytest tests/test_query.py -v
```

Expected: FAIL — old `query.py` uses `get_valid_token`/`reauth`, new tests don't mock those.

- [ ] **Step 3: Write the new `query.py`**

Replace the entire contents of `databricks_connector/query.py`:

```python
"""
Execute Databricks SQL queries using the official databricks-sql-connector.

Auth is delegated to the Databricks SDK's external-browser OAuth flow.
Tokens are cached at ~/.databricks/token-cache.json and refreshed silently by the SDK.
"""

import pandas as pd
from databricks import sql
from databricks.sql.exc import Error as _SqlError

from .auth import AuthRequiredError, get_host, get_http_path


class DatabricksQueryError(Exception):
    pass


_AUTH_KEYWORDS = (
    "401", "403",
    "unauthorized", "unauthenticated",
    "invalid token", "token expired", "token is expired",
    "access denied", "permission_denied",
)


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


def query(sql_query: str, http_path: str | None = None) -> pd.DataFrame:
    """
    Execute a SQL query on Databricks and return a pandas DataFrame.

    Auth is handled by the Databricks SDK:
    - Cached token → silent query (no browser)
    - Expired access token + valid refresh token → silent refresh (no browser)
    - Both tokens expired → raises AuthRequiredError (run setup_auth.py)

    Raises:
        AuthRequiredError: authentication failed — run setup_auth.py
        DatabricksQueryError: SQL query failed for non-auth reasons

    Example:
        from databricks_connector import query
        df = query("SELECT * FROM prd_refined.schema.table LIMIT 100")
    """
    host = get_host()
    if http_path is None:
        http_path = get_http_path()
    hostname = host.replace("https://", "").replace("http://", "").rstrip("/")

    try:
        with sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            auth_type="external-browser",
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)
    except Exception as e:
        if _is_auth_error(e):
            raise AuthRequiredError(
                f"Autenticación fallida: {e}\n"
                "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
            ) from e
        raise DatabricksQueryError(str(e)) from e
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python3 -m pytest tests/test_query.py -v
```

Expected: 5 PASS.

- [ ] **Step 5: Run all tests to confirm nothing else broke**

```bash
python3 -m pytest tests/ -v
```

Expected: test_query 5 PASS, test_google_auth 8 PASS (still exists), test_auth 10 PASS, test_cache 4 PASS, test_session_check 3 PASS = 30 PASS.

- [ ] **Step 6: Commit**

```bash
git add databricks_connector/query.py tests/test_query.py
git commit -m "refactor: replace custom token auth with SDK external-browser in query.py"
```

---

## Task 2: Delete `google_auth.py` and `tests/test_google_auth.py`

**Files:**
- Delete: `databricks_connector/google_auth.py`
- Delete: `tests/test_google_auth.py`

- [ ] **Step 1: Delete both files**

```bash
rm databricks_connector/google_auth.py
rm tests/test_google_auth.py
```

- [ ] **Step 2: Confirm tests still pass**

```bash
python3 -m pytest tests/ -v
```

Expected: 22 PASS (30 minus 8 deleted google_auth tests). No import errors — `query.py` no longer imports from `google_auth`.

- [ ] **Step 3: Commit**

```bash
git add -u databricks_connector/google_auth.py tests/test_google_auth.py
git commit -m "chore: delete google_auth.py and its tests — replaced by SDK external-browser auth"
```

---

## Task 3: Strip `auth.py` and trim `test_auth.py`

**Files:**
- Modify: `databricks_connector/auth.py`
- Modify: `tests/test_auth.py`

`auth.py` currently exports `GOOGLE_SESSION_FILE`, `TOKEN_CACHE_FILE`, `get_google_session_file()`, `read_token_cache()`, `write_token_cache()`. These were used only by `google_auth.py` (deleted) and the old `setup_auth.py` (being rewritten). Remove them.

- [ ] **Step 1: Write failing tests (trim test_auth.py)**

Replace the entire contents of `tests/test_auth.py` — keep the 6 config tests, remove the 4 session-file tests:

```python
import json
import pytest
from pathlib import Path
from unittest.mock import patch
import databricks_connector.auth as auth_module


@pytest.fixture
def config_file(tmp_path):
    cfg = {"host": "example.cloud.databricks.com", "http_path": "/sql/1.0/warehouses/abc123"}
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg))
    return p


def test_get_host_returns_value_from_config(config_file):
    with patch.object(auth_module, "_CONFIG_FILE", config_file):
        assert auth_module.get_host() == "example.cloud.databricks.com"


def test_get_http_path_returns_value_from_config(config_file):
    with patch.object(auth_module, "_CONFIG_FILE", config_file):
        assert auth_module.get_http_path() == "/sql/1.0/warehouses/abc123"


def test_get_warehouse_id_derived_from_http_path(config_file):
    with patch.object(auth_module, "_CONFIG_FILE", config_file):
        assert auth_module.get_warehouse_id() == "abc123"


def test_get_warehouse_id_raises_when_http_path_malformed(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps({"host": "x.com", "http_path": "/"}))
    with patch.object(auth_module, "_CONFIG_FILE", p):
        with pytest.raises(RuntimeError, match="warehouse ID"):
            auth_module.get_warehouse_id()


def test_get_host_raises_when_config_missing(tmp_path):
    with patch.object(auth_module, "_CONFIG_FILE", tmp_path / "config.json"):
        with pytest.raises(RuntimeError, match="config.json no encontrado"):
            auth_module.get_host()


def test_get_host_raises_when_fields_missing(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps({"host": "x.com"}))  # http_path missing
    with patch.object(auth_module, "_CONFIG_FILE", p):
        with pytest.raises(RuntimeError, match="le faltan campos"):
            auth_module.get_http_path()
```

- [ ] **Step 2: Run tests to confirm the 4 removed tests are gone**

```bash
python3 -m pytest tests/test_auth.py -v
```

Expected: 6 PASS (the 4 google_session/token_cache tests no longer exist — that's expected).

- [ ] **Step 3: Write the stripped `auth.py`**

Replace the entire contents of `databricks_connector/auth.py`:

```python
"""
Config loading for Databricks connector.

Reads connection settings from ~/.databricks_connector/config.json.
Auth tokens are managed entirely by the Databricks SDK (external-browser OAuth).
"""

import json
from pathlib import Path

_CONFIG_FILE = Path.home() / ".databricks_connector" / "config.json"


class AuthRequiredError(Exception):
    """Databricks auth failed or tokens expired. Run setup_auth.py."""


def _load_config() -> dict:
    if not _CONFIG_FILE.exists():
        raise RuntimeError(
            "config.json no encontrado. Ejecuta el skill /databricks-setup en Claude Code primero.\n"
            f"Ruta esperada: {_CONFIG_FILE}"
        )
    with open(_CONFIG_FILE) as f:
        cfg = json.load(f)
    missing = [k for k in ("host", "http_path") if not cfg.get(k)]
    if missing:
        raise RuntimeError(f"config.json le faltan campos: {missing}")
    return cfg


def get_host() -> str:
    """Return Databricks workspace hostname (no https://)."""
    return _load_config()["host"]


def get_http_path() -> str:
    """Return full SQL warehouse HTTP path, e.g. /sql/1.0/warehouses/abc123."""
    return _load_config()["http_path"]


def get_warehouse_id() -> str:
    """Return warehouse ID derived from http_path (last path segment)."""
    warehouse_id = get_http_path().rstrip("/").split("/")[-1]
    if not warehouse_id:
        raise RuntimeError(
            "No se pudo extraer el warehouse ID de http_path. "
            "Verifica config.json."
        )
    return warehouse_id
```

- [ ] **Step 4: Run all tests**

```bash
python3 -m pytest tests/ -v
```

Expected: 18 PASS (test_auth:6, test_cache:4, test_query:5, test_session_check:3).

- [ ] **Step 5: Commit**

```bash
git add databricks_connector/auth.py tests/test_auth.py
git commit -m "refactor: strip auth.py to config-only — remove session file helpers replaced by SDK"
```

---

## Task 4: Update `session_check.py` and `test_session_check.py`

**Files:**
- Modify: `databricks_connector/session_check.py`
- Modify: `tests/test_session_check.py`

The old `session_check.py` read from `~/.databricks_connector/token-cache.json` (our custom cache, now unused). The SDK writes tokens to `~/.databricks/token-cache.json`. Update to read from there.

- [ ] **Step 1: Write the failing tests**

Replace the entire contents of `tests/test_session_check.py`:

```python
import json
import pytest
from unittest.mock import patch
import databricks_connector.session_check as sc_module


def test_check_session_returns_false_when_no_cache(tmp_path):
    fake_cache = tmp_path / "token-cache.json"
    with patch.object(sc_module, "_SDK_TOKEN_CACHE", fake_cache):
        from databricks_connector.session_check import check_session
        assert check_session() is False


def test_check_session_returns_true_when_cache_has_content(tmp_path):
    fake_cache = tmp_path / "token-cache.json"
    fake_cache.write_text(json.dumps({"https://host.com": {"access_token": "tok"}}))
    with patch.object(sc_module, "_SDK_TOKEN_CACHE", fake_cache):
        from databricks_connector.session_check import check_session
        assert check_session() is True


def test_check_session_returns_false_when_cache_empty(tmp_path):
    fake_cache = tmp_path / "token-cache.json"
    fake_cache.write_text("{}")
    with patch.object(sc_module, "_SDK_TOKEN_CACHE", fake_cache):
        from databricks_connector.session_check import check_session
        assert check_session() is False
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
python3 -m pytest tests/test_session_check.py -v
```

Expected: FAIL — old `session_check.py` uses `read_token_cache` from `auth`, not `_SDK_TOKEN_CACHE`.

- [ ] **Step 3: Write the new `session_check.py`**

Replace the entire contents of `databricks_connector/session_check.py`:

```python
"""
Check whether the Databricks SDK token cache has credentials for our workspace.
No network calls — purely checks file presence and content.
"""

import json
from pathlib import Path

_SDK_TOKEN_CACHE = Path.home() / ".databricks" / "token-cache.json"


def check_session() -> bool:
    """
    Return True if the Databricks SDK token cache exists and is non-empty.
    Returns False if the file is missing, empty, or unreadable.
    False means setup_auth.py needs to be run.
    """
    if not _SDK_TOKEN_CACHE.exists():
        return False
    try:
        with open(_SDK_TOKEN_CACHE) as f:
            data = json.load(f)
        return bool(data)
    except Exception:
        return False
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python3 -m pytest tests/test_session_check.py -v
```

Expected: 3 PASS.

- [ ] **Step 5: Run all tests**

```bash
python3 -m pytest tests/ -v
```

Expected: 18 PASS total.

- [ ] **Step 6: Commit**

```bash
git add databricks_connector/session_check.py tests/test_session_check.py
git commit -m "refactor: session_check reads SDK token cache at ~/.databricks/token-cache.json"
```

---

## Task 5: Rewrite `setup_auth.py`

**Files:**
- Modify: `setup_auth.py`

No unit tests — this is an interactive script that opens a browser. The only verification is running it manually.

- [ ] **Step 1: Write the new `setup_auth.py`**

Replace the entire contents of `setup_auth.py`:

```python
#!/usr/bin/env python3
"""
One-time Databricks session setup.

Run from your terminal:
    python3 setup_auth.py

Your default browser opens. Log in with your @kavak.com account (use Google SSO).
Tokens are cached at ~/.databricks/token-cache.json — no re-authentication needed
for daily use. The SDK refreshes tokens silently until the refresh token expires
(typically 30–90 days).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks_connector.auth import get_host


def main():
    config_file = Path.home() / ".databricks_connector" / "config.json"
    if not config_file.exists():
        print("\n" + "=" * 60)
        print("ERROR: config.json no encontrado.")
        print("Ejecuta el skill de configuración en Claude Code:\n\n  /databricks-setup\n")
        print("=" * 60)
        sys.exit(1)

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config

        host = get_host()

        print("\n" + "=" * 60)
        print("DATABRICKS SESSION SETUP")
        print("=" * 60)
        print("\nSe abrirá tu navegador predeterminado.")
        print("Inicia sesión con tu correo @kavak.com (usa 'Continuar con Google').\n")

        config = Config(host=f"https://{host}", auth_type="external-browser")
        w = WorkspaceClient(config=config)

        # First authenticated API call triggers the browser OAuth flow.
        # Tokens are cached to ~/.databricks/token-cache.json on success.
        me = w.current_user.me()

        config_file.chmod(0o600)

        print(f"\n✓ Autenticado como: {me.user_name}")
        print("\n" + "=" * 60)
        print("Setup completo. Puedes ejecutar queries.")
        print("=" * 60)

    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Confirm all tests still pass**

```bash
python3 -m pytest tests/ -v
```

Expected: 18 PASS.

- [ ] **Step 3: Commit**

```bash
git add setup_auth.py
git commit -m "refactor: setup_auth.py uses SDK WorkspaceClient — removes Playwright entirely"
```

---

## Task 6: Update `requirements.txt`

**Files:**
- Modify: `requirements.txt`

Remove `playwright>=1.40.0` — used only in deleted `google_auth.py` and old `setup_auth.py`.
Remove `requests>=2.31.0` — used only in deleted `google_auth.py`.

- [ ] **Step 1: Write the new `requirements.txt`**

Replace the entire contents of `requirements.txt`:

```
databricks-sdk>=0.20.0
databricks-sql-connector>=3.0.0
pandas>=2.0.0
pytz>=2024.1
pyyaml>=6.0.1
python-dotenv>=1.0.0
pytest>=8.0.0
pytest-mock>=3.12.0
```

- [ ] **Step 2: Confirm all tests pass**

```bash
python3 -m pytest tests/ -v
```

Expected: 18 PASS.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: remove playwright and requests from requirements — no longer used"
```
