# Databricks OAuth Connector — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `databricks_connector` package engine to use the official Databricks SQL API with OAuth authentication, keeping the same `query()` interface while eliminating the fragile internal `/ajax-api/` dependency.

**Architecture:** Auth is layered — cached token first, silent refresh second, headless Google SSO OAuth third, visible browser fallback last. Playwright is only invoked during re-auth, not per-query. All config lives in `~/.databricks_connector/config.json`; all session state in `~/.databricks_connector/`.

**Tech Stack:** `databricks-sql-connector`, `databricks-sdk` (for OIDC endpoint discovery), `playwright`, `requests`, `pandas`, `pytest`

---

## File Map

| Action | File | Responsibility |
|---|---|---|
| CREATE | `databricks_connector/auth.py` | Config loading (`host`, `http_path`), session file paths, token cache read/write |
| CREATE | `databricks_connector/google_auth.py` | PKCE helpers, OIDC discovery, headless OAuth flow, visible browser fallback, token refresh |
| REPLACE | `databricks_connector/query.py` | Public `query()` using official `sql.connect()`, auth orchestration |
| UPDATE | `databricks_connector/session_check.py` | Token cache validity check (replaces Playwright JS check) |
| UPDATE | `databricks_connector/__init__.py` | Point imports at `auth` and `query` |
| UPDATE | `setup_auth.py` | Save Google cookies + bootstrap first OAuth token |
| UPDATE | `check_session.py` | Messaging only (no logic change needed) |
| DELETE | `databricks_connector/browser_auth.py` | Replaced by `auth.py` |
| DELETE | `databricks_connector/browser_query.py` | Replaced by `query.py` |
| REPLACE | `tests/test_auth.py` | Replaces `test_browser_auth.py` |
| REPLACE | `tests/test_query.py` | Replaces `test_browser_query_errors.py` |
| UPDATE | `tests/test_session_check.py` | Update patches to new module |

**Unchanged:** `cache.py`, `tests/test_cache.py`, `test.py`, `tests/__init__.py`

---

## Task 1: `auth.py` — config loading + token cache

**Files:**
- Create: `databricks_connector/auth.py`
- Create: `tests/test_auth.py`

### Step 1.1: Write failing tests

```python
# tests/test_auth.py
import json
import time
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


def test_get_google_session_file_raises_when_missing(tmp_path):
    with patch.object(auth_module, "GOOGLE_SESSION_FILE", tmp_path / "google_session.json"):
        with pytest.raises(auth_module.AuthRequiredError):
            auth_module.get_google_session_file()


def test_get_google_session_file_returns_path_when_present(tmp_path):
    p = tmp_path / "google_session.json"
    p.write_text("{}")
    with patch.object(auth_module, "GOOGLE_SESSION_FILE", p):
        assert auth_module.get_google_session_file() == p


def test_read_token_cache_returns_none_when_missing(tmp_path):
    with patch.object(auth_module, "TOKEN_CACHE_FILE", tmp_path / "token-cache.json"):
        assert auth_module.read_token_cache() is None


def test_write_and_read_token_cache(tmp_path):
    cache_file = tmp_path / "token-cache.json"
    data = {"access_token": "tok", "refresh_token": "ref", "expires_at": time.time() + 3600}
    with patch.object(auth_module, "TOKEN_CACHE_FILE", cache_file):
        auth_module.write_token_cache(data)
        result = auth_module.read_token_cache()
    assert result["access_token"] == "tok"
    assert result["refresh_token"] == "ref"
```

- [ ] **Step 1.2: Run tests to confirm they fail**

```bash
cd ~/projects/databricks_connector
python -m pytest tests/test_auth.py -v
```
Expected: `ModuleNotFoundError` or `ImportError` — `auth` module does not exist yet.

- [ ] **Step 1.3: Write `auth.py`**

```python
# databricks_connector/auth.py
"""
Config loading, session file paths, and token cache I/O.

No business logic — pure file reading/writing.
Config written by the databricks-setup Claude Code skill.
"""

import json
from pathlib import Path

_CONFIG_FILE = Path.home() / ".databricks_connector" / "config.json"
GOOGLE_SESSION_FILE = Path.home() / ".databricks_connector" / "google_session.json"
TOKEN_CACHE_FILE = Path.home() / ".databricks_connector" / "token-cache.json"


class AuthRequiredError(Exception):
    """Google session or OAuth tokens missing/expired. Run setup_auth.py."""


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
    return get_http_path().rstrip("/").split("/")[-1]


def get_google_session_file() -> Path:
    """Return path to Google session file, or raise AuthRequiredError if missing."""
    if not GOOGLE_SESSION_FILE.exists():
        raise AuthRequiredError(
            "No hay sesión de Google guardada.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )
    return GOOGLE_SESSION_FILE


def read_token_cache() -> dict | None:
    """Return cached token dict or None if file is missing or unreadable."""
    if not TOKEN_CACHE_FILE.exists():
        return None
    try:
        with open(TOKEN_CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def write_token_cache(data: dict) -> None:
    """Write token dict to cache file (creates parent dir if needed)."""
    TOKEN_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_CACHE_FILE, "w") as f:
        json.dump(data, f)
    TOKEN_CACHE_FILE.chmod(0o600)
```

- [ ] **Step 1.4: Run tests — expect all pass**

```bash
python -m pytest tests/test_auth.py -v
```
Expected: all 10 tests PASS.

- [ ] **Step 1.5: Commit**

```bash
git add databricks_connector/auth.py tests/test_auth.py
git commit -m "feat: add auth.py — config loading and token cache I/O"
```

---

## Task 2: `google_auth.py` — PKCE helpers + OIDC discovery + token refresh

**Files:**
- Create: `databricks_connector/google_auth.py` (partial — helpers only)
- Create: `tests/test_google_auth.py`

- [ ] **Step 2.1: Write failing tests for PKCE + OIDC helpers**

```python
# tests/test_google_auth.py
import base64
import hashlib
import time
import json
import pytest
from unittest.mock import patch, MagicMock
import databricks_connector.google_auth as ga


# ── PKCE ─────────────────────────────────────────────────────────────────────

def test_generate_pkce_returns_verifier_and_challenge():
    verifier, challenge = ga._generate_pkce()
    # verifier is base64url, 43 chars (32 bytes)
    assert len(verifier) == 43
    # challenge = base64url(sha256(verifier))
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    assert challenge == expected


def test_generate_pkce_produces_different_values_each_call():
    v1, _ = ga._generate_pkce()
    v2, _ = ga._generate_pkce()
    assert v1 != v2


# ── OIDC discovery ───────────────────────────────────────────────────────────

def test_get_oidc_endpoints_returns_auth_and_token_urls():
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "authorization_endpoint": "https://example.com/oidc/v1/authorize",
        "token_endpoint": "https://example.com/oidc/v1/token",
    }
    mock_response.raise_for_status = MagicMock()
    with patch("databricks_connector.google_auth.requests.get", return_value=mock_response):
        endpoints = ga._get_oidc_endpoints("example.cloud.databricks.com")
    assert endpoints["authorization_endpoint"] == "https://example.com/oidc/v1/authorize"
    assert endpoints["token_endpoint"] == "https://example.com/oidc/v1/token"


# ── Token refresh ─────────────────────────────────────────────────────────────

def test_get_valid_token_returns_access_token_when_cache_fresh(tmp_path):
    import databricks_connector.auth as auth_module
    cache_file = tmp_path / "token-cache.json"
    data = {"access_token": "fresh_token", "refresh_token": "ref", "expires_at": time.time() + 7200}
    cache_file.write_text(json.dumps(data))
    with patch.object(auth_module, "TOKEN_CACHE_FILE", cache_file):
        token = ga.get_valid_token()
    assert token == "fresh_token"


def test_get_valid_token_returns_none_when_no_cache(tmp_path):
    import databricks_connector.auth as auth_module
    with patch.object(auth_module, "TOKEN_CACHE_FILE", tmp_path / "token-cache.json"):
        with patch("databricks_connector.google_auth._get_oidc_endpoints"):
            token = ga.get_valid_token()
    assert token is None


def test_get_valid_token_refreshes_silently_when_expired(tmp_path):
    import databricks_connector.auth as auth_module
    cache_file = tmp_path / "token-cache.json"
    data = {"access_token": "old", "refresh_token": "ref_tok", "expires_at": time.time() - 100}
    cache_file.write_text(json.dumps(data))

    new_token_response = {
        "access_token": "new_token",
        "refresh_token": "new_ref",
        "expires_in": 3600,
    }
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = new_token_response

    endpoints = {"authorization_endpoint": "x", "token_endpoint": "https://x/token"}

    with patch.object(auth_module, "TOKEN_CACHE_FILE", cache_file), \
         patch("databricks_connector.google_auth._get_oidc_endpoints", return_value=endpoints), \
         patch("databricks_connector.google_auth.requests.post", return_value=mock_resp), \
         patch("databricks_connector.google_auth.get_host", return_value="example.com"):
        token = ga.get_valid_token()

    assert token == "new_token"
```

- [ ] **Step 2.2: Run tests to confirm they fail**

```bash
python -m pytest tests/test_google_auth.py -v
```
Expected: `ImportError` — `google_auth` module does not exist yet.

- [ ] **Step 2.3: Write `google_auth.py` partial — helpers + `get_valid_token()`**

```python
# databricks_connector/google_auth.py
"""
Google SSO cookie-based OAuth re-authentication for Databricks.

Flow:
  1. get_valid_token()  — cached access token or silent refresh via refresh token
  2. reauth()           — headless Playwright + Google cookies → full OAuth PKCE flow
                          falls back to visible browser if Google cookies have expired
"""

import asyncio
import base64
import hashlib
import json
import secrets
import socket
import time
import urllib.parse

import requests

from .auth import (
    AuthRequiredError,
    get_host,
    get_warehouse_id,
    get_google_session_file,
    read_token_cache,
    write_token_cache,
)

_DATABRICKS_CLIENT_ID = "databricks-cli"
_TOKEN_REFRESH_BUFFER_SECS = 60  # refresh token this many seconds before expiry


# ── PKCE ──────────────────────────────────────────────────────────────────────

def _generate_pkce() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) as base64url strings."""
    verifier_bytes = secrets.token_bytes(32)
    verifier = base64.urlsafe_b64encode(verifier_bytes).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


def _find_free_port() -> int:
    """Return an available localhost port."""
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


# ── OIDC discovery ────────────────────────────────────────────────────────────

def _get_oidc_endpoints(host: str) -> dict:
    """Return {'authorization_endpoint': ..., 'token_endpoint': ...} from OIDC discovery."""
    host = host.replace("https://", "").replace("http://", "").rstrip("/")
    url = f"https://{host}/oidc/.well-known/oauth-authorization-server"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return {
        "authorization_endpoint": data["authorization_endpoint"],
        "token_endpoint": data["token_endpoint"],
    }


# ── Token cache helpers ───────────────────────────────────────────────────────

def _save_tokens(token_response: dict) -> None:
    """Write token response dict to cache (computes expires_at from expires_in)."""
    expires_in = token_response.get("expires_in", 3600)
    write_token_cache({
        "access_token": token_response["access_token"],
        "refresh_token": token_response.get("refresh_token", ""),
        "expires_at": time.time() + expires_in,
    })


def _do_refresh(token_endpoint: str, refresh_token: str) -> dict | None:
    """POST refresh_token grant. Returns token response dict or None on failure."""
    resp = requests.post(
        token_endpoint,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": _DATABRICKS_CLIENT_ID,
        },
        timeout=30,
    )
    if not resp.ok:
        return None
    return resp.json()


# ── Public: get valid token ───────────────────────────────────────────────────

def get_valid_token() -> str | None:
    """
    Return a valid access token, refreshing silently if expired.
    Returns None if full re-auth is needed (call reauth() then retry).
    """
    cache = read_token_cache()
    if not cache:
        return None

    access_token = cache.get("access_token")
    refresh_token = cache.get("refresh_token")
    expires_at = cache.get("expires_at", 0)

    # Access token still valid
    if access_token and expires_at - time.time() > _TOKEN_REFRESH_BUFFER_SECS:
        return access_token

    # Try silent refresh
    if refresh_token:
        try:
            endpoints = _get_oidc_endpoints(get_host())
            new_tokens = _do_refresh(endpoints["token_endpoint"], refresh_token)
            if new_tokens:
                _save_tokens(new_tokens)
                return new_tokens["access_token"]
        except Exception:
            pass  # network error or invalid refresh token — fall through to None

    return None
```

- [ ] **Step 2.4: Run tests — expect all pass**

```bash
python -m pytest tests/test_google_auth.py -v
```
Expected: all 6 tests PASS.

- [ ] **Step 2.5: Commit**

```bash
git add databricks_connector/google_auth.py tests/test_google_auth.py
git commit -m "feat: add google_auth.py — PKCE helpers, OIDC discovery, silent token refresh"
```

---

## Task 3: `google_auth.py` — headless OAuth flow + visible browser fallback

**Files:**
- Modify: `databricks_connector/google_auth.py` (add `reauth()` and helpers)
- Modify: `tests/test_google_auth.py` (add tests)

- [ ] **Step 3.1: Write failing tests for `reauth()`**

Append to `tests/test_google_auth.py`:

```python
# Append to tests/test_google_auth.py
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock


def test_exchange_code_returns_token_dict():
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {
        "access_token": "dapi_new",
        "refresh_token": "new_ref",
        "expires_in": 3600,
    }
    with patch("databricks_connector.google_auth.requests.post", return_value=mock_resp) as mock_post:
        result = ga._exchange_code(
            token_endpoint="https://host/oidc/v1/token",
            code="AUTH_CODE",
            verifier="VERIFIER",
            redirect_uri="http://localhost:8888/callback",
        )
    mock_post.assert_called_once()
    call_data = mock_post.call_args[1]["data"]
    assert call_data["grant_type"] == "authorization_code"
    assert call_data["code"] == "AUTH_CODE"
    assert call_data["code_verifier"] == "VERIFIER"
    assert result["access_token"] == "dapi_new"


def test_reauth_calls_headless_then_saves_tokens(tmp_path):
    import databricks_connector.auth as auth_module

    mock_token_resp = {
        "access_token": "dapi_headless",
        "refresh_token": "ref_headless",
        "expires_in": 3600,
    }
    endpoints = {
        "authorization_endpoint": "https://host/oidc/v1/authorize",
        "token_endpoint": "https://host/oidc/v1/token",
    }
    google_session = tmp_path / "google_session.json"
    google_session.write_text("{}")
    token_cache = tmp_path / "token-cache.json"

    with patch.object(auth_module, "GOOGLE_SESSION_FILE", google_session), \
         patch.object(auth_module, "TOKEN_CACHE_FILE", token_cache), \
         patch("databricks_connector.google_auth.get_host", return_value="host.com"), \
         patch("databricks_connector.google_auth._get_oidc_endpoints", return_value=endpoints), \
         patch("databricks_connector.google_auth._headless_oauth", return_value=mock_token_resp):
        ga.reauth()

    saved = json.loads(token_cache.read_text())
    assert saved["access_token"] == "dapi_headless"
```

- [ ] **Step 3.2: Run tests to confirm they fail**

```bash
python -m pytest tests/test_google_auth.py::test_exchange_code_returns_token_dict tests/test_google_auth.py::test_reauth_calls_headless_then_saves_tokens -v
```
Expected: `AttributeError` — `_exchange_code` and `reauth` not defined yet.

- [ ] **Step 3.3: Add `_exchange_code`, `_headless_oauth`, and `reauth()` to `google_auth.py`**

Append to `databricks_connector/google_auth.py`:

```python
# ── Code exchange ─────────────────────────────────────────────────────────────

def _exchange_code(
    token_endpoint: str, code: str, verifier: str, redirect_uri: str
) -> dict:
    """Exchange authorization code for access + refresh tokens."""
    resp = requests.post(
        token_endpoint,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": verifier,
            "redirect_uri": redirect_uri,
            "client_id": _DATABRICKS_CLIENT_ID,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ── Headless OAuth flow ───────────────────────────────────────────────────────

async def _headless_oauth(
    auth_url: str, redirect_uri: str, port: int, google_session_file: str
) -> str:
    """
    Load Google cookies into headless Chromium, navigate to auth_url, and
    intercept the OAuth callback via Playwright route interception.
    Returns the authorization code string.
    Raises AuthRequiredError on timeout (Google cookies likely expired).
    """
    from playwright.async_api import async_playwright

    code_holder: dict = {}

    async def _intercept(route):
        url = route.request.url
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            code_holder["code"] = params["code"][0]
        await route.fulfill(
            status=200,
            body="Login exitoso. Puedes cerrar esta ventana.",
            content_type="text/html",
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(storage_state=google_session_file)
        await context.route(f"http://localhost:{port}/**", _intercept)
        page = await context.new_page()

        await page.goto(auth_url, wait_until="domcontentloaded")

        # Poll up to 60 seconds for the callback to be intercepted
        for _ in range(60):
            if code_holder.get("code"):
                break
            await asyncio.sleep(1)

        # Refresh Google cookies so they stay alive longer
        await context.storage_state(path=google_session_file)
        await browser.close()

    if not code_holder.get("code"):
        raise AuthRequiredError(
            "OAuth headless flow timed out — las cookies de Google pueden haber expirado.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )

    return code_holder["code"]


# ── Visible browser fallback ──────────────────────────────────────────────────

async def _visible_browser_login(auth_url: str, redirect_uri: str, port: int) -> str:
    """
    Open a visible browser for manual Google login when cookies are expired.
    Returns the authorization code once the user completes login.
    Raises AuthRequiredError on timeout (5 minutes).
    """
    from playwright.async_api import async_playwright

    print("\n" + "=" * 60)
    print("RE-AUTENTICACIÓN REQUERIDA")
    print("=" * 60)
    print("Se abrirá una ventana de Chrome. Inicia sesión con tu cuenta @kavak.com.")
    print("La ventana se cerrará automáticamente al completar el login.\n")

    code_holder: dict = {}

    async def _intercept(route):
        params = urllib.parse.parse_qs(urllib.parse.urlparse(route.request.url).query)
        if "code" in params:
            code_holder["code"] = params["code"][0]
        await route.fulfill(
            status=200,
            body="Login exitoso. Puedes cerrar esta ventana.",
            content_type="text/html",
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()
        await context.route(f"http://localhost:{port}/**", _intercept)
        page = await context.new_page()
        await page.goto(auth_url, wait_until="domcontentloaded")

        # Wait up to 5 minutes for manual login
        for _ in range(300):
            if code_holder.get("code"):
                break
            await asyncio.sleep(1)

        # Save new Google cookies for future headless use
        from .auth import GOOGLE_SESSION_FILE
        await context.storage_state(path=str(GOOGLE_SESSION_FILE))
        await browser.close()

    if not code_holder.get("code"):
        raise AuthRequiredError("Login cancelado o timeout. Vuelve a intentarlo.")

    print("Login exitoso. Sesión de Google guardada.\n")
    return code_holder["code"]


# ── Public: full re-auth ──────────────────────────────────────────────────────

def reauth() -> None:
    """
    Full OAuth re-authentication.
    1. Tries headless Playwright with saved Google cookies.
    2. Falls back to visible browser if Google cookies are expired.
    Saves new tokens to token cache on success.
    Raises AuthRequiredError only if user cancels the visible browser login.
    """
    asyncio.run(_do_reauth())


async def _do_reauth() -> None:
    host = get_host()
    endpoints = _get_oidc_endpoints(host)
    verifier, challenge = _generate_pkce()
    state = secrets.token_urlsafe(16)
    port = _find_free_port()
    redirect_uri = f"http://localhost:{port}/callback"

    params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": _DATABRICKS_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "scope": "sql offline_access",
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })
    auth_url = f"{endpoints['authorization_endpoint']}?{params}"

    # Try headless first (uses saved Google cookies)
    try:
        google_session = str(get_google_session_file())
        code = await _headless_oauth(auth_url, redirect_uri, port, google_session)
    except AuthRequiredError:
        # Google cookies expired — fall back to visible browser
        code = await _visible_browser_login(auth_url, redirect_uri, port)

    token_response = _exchange_code(
        endpoints["token_endpoint"], code, verifier, redirect_uri
    )
    _save_tokens(token_response)
```

- [ ] **Step 3.4: Run all google_auth tests**

```bash
python -m pytest tests/test_google_auth.py -v
```
Expected: all tests PASS.

- [ ] **Step 3.5: Commit**

```bash
git add databricks_connector/google_auth.py tests/test_google_auth.py
git commit -m "feat: add headless OAuth flow and visible browser fallback to google_auth.py"
```

---

## Task 4: `query.py` — official SQL connector + auth orchestration

**Files:**
- Replace: `databricks_connector/query.py`
- Create: `tests/test_query.py`

- [ ] **Step 4.1: Write failing tests**

```python
# tests/test_query.py
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, call

from databricks_connector.query import query, DatabricksQueryError
from databricks_connector.auth import AuthRequiredError


def _make_cursor(rows, columns):
    """Helper: mock cursor with fetchall + description."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = [(col, None, None, None, None, None, None) for col in columns]
    return cursor


def test_query_returns_dataframe_when_token_valid():
    cursor = _make_cursor([("a", 1), ("b", 2)], ["name", "value"])
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = cursor

    with patch("databricks_connector.query.get_valid_token", return_value="valid_tok"), \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"), \
         patch("databricks_connector.query.sql.connect", return_value=mock_conn):
        df = query("SELECT 1")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["name", "value"]
    assert len(df) == 2


def test_query_triggers_reauth_when_no_token():
    cursor = _make_cursor([], ["col"])
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = cursor

    # First call returns None (no token), second returns a token after reauth
    token_calls = iter([None, "new_tok"])

    with patch("databricks_connector.query.get_valid_token", side_effect=token_calls), \
         patch("databricks_connector.query.reauth") as mock_reauth, \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"), \
         patch("databricks_connector.query.sql.connect", return_value=mock_conn):
        query("SELECT 1")

    mock_reauth.assert_called_once()


def test_query_raises_auth_error_when_reauth_fails():
    with patch("databricks_connector.query.get_valid_token", return_value=None), \
         patch("databricks_connector.query.reauth"), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"):
        # Both token attempts return None → AuthRequiredError
        with patch("databricks_connector.query.get_valid_token", return_value=None):
            with pytest.raises(AuthRequiredError):
                query("SELECT 1")


def test_query_retries_once_on_server_auth_rejection():
    from databricks.sql.exc import Error as SqlError

    cursor = _make_cursor([], ["col"])
    mock_conn_ok = MagicMock()
    mock_conn_ok.__enter__ = MagicMock(return_value=mock_conn_ok)
    mock_conn_ok.__exit__ = MagicMock(return_value=False)
    mock_conn_ok.cursor.return_value = cursor

    call_count = {"n": 0}

    def connect_side_effect(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise SqlError("401 Unauthorized")
        return mock_conn_ok

    with patch("databricks_connector.query.get_valid_token", return_value="tok"), \
         patch("databricks_connector.query.reauth"), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"), \
         patch("databricks_connector.query.sql.connect", side_effect=connect_side_effect):
        df = query("SELECT 1")

    assert call_count["n"] == 2


def test_query_raises_databricks_query_error_on_sql_failure():
    from databricks.sql.exc import Error as SqlError

    with patch("databricks_connector.query.get_valid_token", return_value="tok"), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"), \
         patch("databricks_connector.query.sql.connect", side_effect=SqlError("Table not found")):
        with pytest.raises(DatabricksQueryError, match="Table not found"):
            query("SELECT * FROM nonexistent")
```

- [ ] **Step 4.2: Run tests to confirm they fail**

```bash
python -m pytest tests/test_query.py -v
```
Expected: `ImportError` — `query.py` exports nothing useful yet.

- [ ] **Step 4.3: Write `query.py`**

```python
# databricks_connector/query.py
"""
Execute Databricks SQL queries using the official databricks-sql-connector.

Auth flow:
  1. Try cached access token (or silent refresh via refresh token)
  2. If unavailable: full re-auth via Google SSO cookies (headless or visible browser)
  3. On server-side auth rejection: re-auth once and retry
"""

import pandas as pd
from databricks import sql
from databricks.sql.exc import Error as _SqlError

from .auth import AuthRequiredError, get_host, get_http_path
from .google_auth import get_valid_token, reauth


class DatabricksQueryError(Exception):
    pass


_AUTH_KEYWORDS = ("401", "403", "unauthorized", "unauthenticated", "invalid token")


def _is_auth_error(exc: _SqlError) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


def _execute(host: str, http_path: str, access_token: str, sql_query: str) -> pd.DataFrame:
    hostname = host.replace("https://", "").replace("http://", "").rstrip("/")
    with sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(result, columns=columns)


def query(sql_query: str, http_path: str | None = None) -> pd.DataFrame:
    """
    Execute a SQL query on Databricks and return a pandas DataFrame.

    Handles auth automatically:
    - Uses cached OAuth token when valid
    - Silently refreshes via refresh token when expired
    - Re-authenticates via headless Google SSO cookies when needed
    - Opens a visible browser only when Google cookies have also expired

    Raises:
        AuthRequiredError: if re-authentication is impossible (user cancelled login)
        DatabricksQueryError: if the SQL query fails for non-auth reasons

    Example:
        from databricks_connector import query
        df = query("SELECT * FROM prd_refined.salesforce_latam_refined.vehicle LIMIT 100")
    """
    if http_path is None:
        http_path = get_http_path()
    host = get_host()

    # Step 1: ensure we have a valid token (refresh silently if needed)
    access_token = get_valid_token()
    if access_token is None:
        reauth()
        access_token = get_valid_token()
        if access_token is None:
            raise AuthRequiredError(
                "No se pudo obtener un token válido tras re-autenticación.\n"
                "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
            )

    # Step 2: execute query
    try:
        return _execute(host, http_path, access_token, sql_query)
    except _SqlError as e:
        if _is_auth_error(e):
            # Token was valid locally but server rejected it — re-auth and retry once
            reauth()
            access_token = get_valid_token()
            if access_token is None:
                raise AuthRequiredError("Re-autenticación fallida tras rechazo del servidor.") from e
            try:
                return _execute(host, http_path, access_token, sql_query)
            except _SqlError as e2:
                if _is_auth_error(e2):
                    raise AuthRequiredError(str(e2)) from e2
                raise DatabricksQueryError(str(e2)) from e2
        raise DatabricksQueryError(str(e)) from e
```

- [ ] **Step 4.4: Run tests — expect all pass**

```bash
python -m pytest tests/test_query.py -v
```
Expected: all 5 tests PASS.

- [ ] **Step 4.5: Commit**

```bash
git add databricks_connector/query.py tests/test_query.py
git commit -m "feat: add query.py — official SQL connector with OAuth auth orchestration"
```

---

## Task 5: `session_check.py` — token validity check

**Files:**
- Modify: `databricks_connector/session_check.py`
- Modify: `tests/test_session_check.py`

- [ ] **Step 5.1: Write updated failing tests**

Replace the contents of `tests/test_session_check.py`:

```python
# tests/test_session_check.py
import json
import time
import pytest
from unittest.mock import patch
from pathlib import Path
import databricks_connector.auth as auth_module


def test_check_session_returns_false_when_no_cache(tmp_path):
    with patch.object(auth_module, "TOKEN_CACHE_FILE", tmp_path / "token-cache.json"):
        from databricks_connector.session_check import check_session
        assert check_session() is False


def test_check_session_returns_true_when_token_fresh(tmp_path):
    cache_file = tmp_path / "token-cache.json"
    data = {"access_token": "tok", "refresh_token": "ref", "expires_at": time.time() + 7200}
    cache_file.write_text(json.dumps(data))
    with patch.object(auth_module, "TOKEN_CACHE_FILE", cache_file):
        from databricks_connector.session_check import check_session
        assert check_session() is True


def test_check_session_returns_false_when_token_expired(tmp_path):
    cache_file = tmp_path / "token-cache.json"
    data = {"access_token": "tok", "refresh_token": "", "expires_at": time.time() - 100}
    cache_file.write_text(json.dumps(data))
    with patch.object(auth_module, "TOKEN_CACHE_FILE", cache_file):
        from databricks_connector.session_check import check_session
        assert check_session() is False
```

- [ ] **Step 5.2: Run to confirm they fail**

```bash
python -m pytest tests/test_session_check.py -v
```
Expected: tests import old Playwright-based code and fail with patch errors.

- [ ] **Step 5.3: Replace `session_check.py`**

```python
# databricks_connector/session_check.py
"""
Check whether the saved Databricks OAuth token is still valid.
No network calls — purely checks expiry from token-cache.json.
"""

import time
from .auth import read_token_cache

_BUFFER_SECS = 60  # treat token as expired this many seconds before actual expiry


def check_session() -> bool:
    """
    Return True if the cached OAuth token is present and not expired.
    Returns False if missing, expired, or unreadable.

    Example:
        from databricks_connector import check_session
        if not check_session():
            print("Token expired — run setup_auth.py")
    """
    cache = read_token_cache()
    if not cache:
        return False
    expires_at = cache.get("expires_at", 0)
    access_token = cache.get("access_token")
    return bool(access_token and expires_at - time.time() > _BUFFER_SECS)
```

- [ ] **Step 5.4: Run tests — expect all pass**

```bash
python -m pytest tests/test_session_check.py -v
```
Expected: all 3 tests PASS.

- [ ] **Step 5.5: Commit**

```bash
git add databricks_connector/session_check.py tests/test_session_check.py
git commit -m "feat: update session_check.py — token-cache.json check replaces Playwright JS check"
```

---

## Task 6: `__init__.py` — update imports + delete old files

**Files:**
- Modify: `databricks_connector/__init__.py`
- Delete: `databricks_connector/browser_auth.py`
- Delete: `databricks_connector/browser_query.py`
- Delete: `tests/test_browser_auth.py`
- Delete: `tests/test_browser_query_errors.py`

- [ ] **Step 6.1: Update `__init__.py`**

```python
# databricks_connector/__init__.py
from .query import query, DatabricksQueryError
from .auth import AuthRequiredError
from .session_check import check_session

__all__ = ["query", "DatabricksQueryError", "AuthRequiredError", "check_session"]
```

- [ ] **Step 6.2: Delete old files**

```bash
rm databricks_connector/browser_auth.py
rm databricks_connector/browser_query.py
rm tests/test_browser_auth.py
rm tests/test_browser_query_errors.py
```

- [ ] **Step 6.3: Run full test suite — expect all pass**

```bash
python -m pytest tests/ -v
```
Expected: all tests in `test_auth.py`, `test_google_auth.py`, `test_query.py`, `test_session_check.py`, `test_cache.py` PASS. No import errors.

- [ ] **Step 6.4: Commit**

```bash
git add -A
git commit -m "refactor: update __init__.py imports, delete browser_auth.py and browser_query.py"
```

---

## Task 7: `setup_auth.py` — save Google cookies + bootstrap OAuth token

**Files:**
- Modify: `setup_auth.py`

- [ ] **Step 7.1: Verify the OIDC discovery endpoint is reachable**

Before updating `setup_auth.py`, manually verify the OIDC endpoint works for this workspace:

```bash
python3 -c "
import requests
host = 'dbc-6f0786a7-8ba5.cloud.databricks.com'
url = f'https://{host}/oidc/.well-known/oauth-authorization-server'
resp = requests.get(url, timeout=10)
print(resp.status_code)
import json; data = resp.json()
print('auth endpoint:', data.get('authorization_endpoint'))
print('token endpoint:', data.get('token_endpoint'))
"
```
Expected: status 200 and two endpoint URLs printed.

If the endpoint returns 404, the workspace may use a different discovery path. Try:
```bash
python3 -c "
import requests
host = 'dbc-6f0786a7-8ba5.cloud.databricks.com'
# Alternative path for some Databricks deployments
url = f'https://{host}/.well-known/oauth-authorization-server'
resp = requests.get(url, timeout=10)
print(resp.status_code, resp.text[:200])
"
```
If 404 on both, update `_get_oidc_endpoints()` in `google_auth.py` to use the correct path before continuing.

- [ ] **Step 7.2: Replace `setup_auth.py`**

```python
#!/usr/bin/env python3
"""
One-time Databricks session setup.

Run from your terminal:
    python3 setup_auth.py

Chromium opens. Log in with your @kavak.com account (use Google SSO).
Once the SQL Warehouses page loads, the browser closes automatically.
Google cookies and a fresh OAuth token are saved for future headless use.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks_connector.auth import (
    GOOGLE_SESSION_FILE,
    TOKEN_CACHE_FILE,
    get_host,
    AuthRequiredError,
)

LOGIN_URL_TEMPLATE = "https://{host}/sql/warehouses"
SUCCESS_URL_PATTERN = "**/sql-warehouses**"
LOGIN_TIMEOUT_MS = 300_000  # 5 minutes


async def _do_login():
    from playwright.async_api import async_playwright

    host = get_host()
    login_url = LOGIN_URL_TEMPLATE.format(host=host)

    print("\n" + "=" * 60)
    print("DATABRICKS SESSION SETUP")
    print("=" * 60)
    print("\nSe abrirá una ventana de Chrome en tu pantalla.")
    print("Inicia sesión con tu correo @kavak.com (usa 'Continuar con Google').")
    print("La ventana se cerrará sola cuando el login sea exitoso.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(login_url, wait_until="domcontentloaded")
        print("Navegador abierto. Inicia sesión ahora...")
        print("(El Chrome se cerrará solo cuando el login sea exitoso)\n")

        await page.wait_for_url(SUCCESS_URL_PATTERN, timeout=LOGIN_TIMEOUT_MS)

        print("\nLogin detectado. Guardando cookies de Google...")
        import asyncio as _asyncio
        await _asyncio.sleep(2)  # let final cookies settle

        GOOGLE_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(GOOGLE_SESSION_FILE))
        GOOGLE_SESSION_FILE.chmod(0o600)
        print(f"Cookies guardadas en {GOOGLE_SESSION_FILE}")

        await browser.close()


def main():
    # Verify config.json exists
    config_file = Path.home() / ".databricks_connector" / "config.json"
    if not config_file.exists():
        print("\n" + "=" * 60)
        print("ERROR: config.json no encontrado.")
        print("\nAntes de correr este script, ejecuta el skill de configuración")
        print("en Claude Code:\n\n  /databricks-setup\n")
        print("=" * 60)
        sys.exit(1)

    # Remove stale session files
    for f in (GOOGLE_SESSION_FILE, TOKEN_CACHE_FILE):
        if f.exists():
            f.unlink()

    try:
        asyncio.run(_do_login())
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)

    # Bootstrap first OAuth token using the freshly saved Google cookies
    print("\nObteniendo token OAuth inicial...")
    try:
        from databricks_connector.google_auth import reauth
        reauth()
        print(f"Token OAuth guardado en {TOKEN_CACHE_FILE}")
    except AuthRequiredError as e:
        print(f"\nAdvertencia: no se pudo obtener token OAuth: {e}")
        print("Esto es inusual — intenta correr setup_auth.py de nuevo.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Setup completo. Puedes ejecutar queries.")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

- [ ] **Step 7.3: Run a smoke test of the import chain**

```bash
python3 -c "
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/databricks_connector'))
from databricks_connector import query, DatabricksQueryError, AuthRequiredError, check_session
print('All imports OK')
print('check_session():', check_session())
"
```
Expected output:
```
All imports OK
check_session(): False
```
(`False` because no token exists yet — setup_auth.py has not been run.)

- [ ] **Step 7.4: Commit**

```bash
git add setup_auth.py
git commit -m "feat: update setup_auth.py — saves Google cookies and bootstraps OAuth token"
```

---

## Task 8: End-to-end live test

> **Note:** This task requires interactive login. Run `setup_auth.py` manually then verify queries work.

- [ ] **Step 8.1: Run `setup_auth.py` interactively**

```bash
python3 ~/projects/databricks_connector/setup_auth.py
```
Expected:
- Chrome window opens
- You log in with your `@kavak.com` account via Google SSO
- Browser closes automatically
- Terminal prints: `Setup completo. Puedes ejecutar queries.`
- Files created: `~/.databricks_connector/google_session.json`, `~/.databricks_connector/token-cache.json`

- [ ] **Step 8.2: Verify session and token**

```bash
python3 ~/projects/databricks_connector/check_session.py
```
Expected: `Session valid`

```bash
python3 -c "
import json
from pathlib import Path
cache = json.loads((Path.home() / '.databricks_connector/token-cache.json').read_text())
import time
print('access_token:', cache['access_token'][:20], '...')
print('expires in:', round(cache['expires_at'] - time.time()), 'seconds')
"
```
Expected: token starts with `dapi` or `ey`, expires in ~3600 seconds.

- [ ] **Step 8.3: Run live query via `test.py`**

```bash
cd ~/projects/databricks_connector && python3 test.py
```
Expected: first 5 rows of `prd_refined.salesforce_latam_refined.vehicle` printed, no errors.

- [ ] **Step 8.4: Verify `extract_event.py` still works**

```bash
cd ~/projects/eeas-queries && python3 extract_event.py
```
Expected: `Saved N rows x M cols → event.csv`

- [ ] **Step 8.5: Final commit**

```bash
cd ~/projects/databricks_connector
git add -A
git commit -m "feat: complete OAuth connector replacement — Google SSO cookies + official SQL API"
```

---

## Self-Review Notes

- **Spec coverage:** All spec sections covered — config.json (`http_path`), three session layers, full auth flow with all fallbacks, file structure 1-for-1 replacement, `setup_auth.py` update, unchanged public API.
- **No placeholders:** All steps contain actual code.
- **Type consistency:** `get_valid_token()` defined in Task 2, imported in Task 4. `reauth()` defined in Task 3, imported in Task 4. `AuthRequiredError` defined in Task 1 (`auth.py`), used consistently in Tasks 3 and 4. `DatabricksQueryError` defined in Task 4 (`query.py`), exported in Task 6.
- **OIDC verification step (Task 7.1):** Added to catch the `client_id` / discovery URL issue flagged in the spec self-review before the actual browser login is attempted.
