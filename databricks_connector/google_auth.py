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
import secrets
import socket
import time
import urllib.parse

import requests

from .auth import (
    AuthRequiredError,
    get_host,
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
    cache_entry = {
        "access_token": token_response["access_token"],
        "expires_at": time.time() + expires_in,
    }
    if token_response.get("refresh_token"):
        cache_entry["refresh_token"] = token_response["refresh_token"]
    write_token_cache(cache_entry)


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
        except (requests.RequestException, OSError, ValueError, KeyError):
            pass  # network error, disk I/O, malformed JSON, or missing key in response

    return None


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
        params = urllib.parse.parse_qs(urllib.parse.urlparse(route.request.url).query)
        if "error" in params:
            code_holder["error"] = params["error"][0]
        elif "code" in params:
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

    if code_holder.get("error"):
        raise AuthRequiredError(
            f"OAuth rechazado: {code_holder['error']}.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )
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
        if "error" in params:
            code_holder["error"] = params["error"][0]
        elif "code" in params:
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
        GOOGLE_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(GOOGLE_SESSION_FILE))
        await browser.close()

    if code_holder.get("error"):
        raise AuthRequiredError(
            f"OAuth rechazado: {code_holder['error']}.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )
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
