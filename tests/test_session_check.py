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
