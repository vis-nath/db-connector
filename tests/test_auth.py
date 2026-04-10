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
    assert oct(cache_file.stat().st_mode)[-3:] == "600"
