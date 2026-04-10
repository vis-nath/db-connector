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


# ── _exchange_code ────────────────────────────────────────────────────────────

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
         patch("databricks_connector.google_auth._headless_oauth", return_value="AUTH_CODE_123"), \
         patch("databricks_connector.google_auth._exchange_code", return_value=mock_token_resp):
        ga.reauth()

    import json
    saved = json.loads(token_cache.read_text())
    assert saved["access_token"] == "dapi_headless"
