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
