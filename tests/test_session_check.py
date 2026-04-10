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
