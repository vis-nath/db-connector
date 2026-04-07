"""
Tests for browser_auth.py — config loading and session file guards.

These tests verify the two error guards that protect users from confusing
failures when config.json or session.json are missing.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from databricks_connector.browser_auth import (
    get_host,
    get_warehouse_id,
    get_session_file,
    AuthRequiredError,
)


@pytest.fixture
def config_file(tmp_path):
    """Write a valid config.json to a temp directory and patch _CONFIG_FILE."""
    cfg = {"host": "https://example.cloud.databricks.com", "warehouse_id": "abc123"}
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg))
    return p


def test_get_host_returns_value_from_config(config_file):
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "_CONFIG_FILE", config_file):
        assert get_host() == "https://example.cloud.databricks.com"


def test_get_warehouse_id_returns_value_from_config(config_file):
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "_CONFIG_FILE", config_file):
        assert get_warehouse_id() == "abc123"


def test_get_host_raises_runtime_error_when_config_missing(tmp_path):
    missing = tmp_path / "config.json"
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "_CONFIG_FILE", missing):
        with pytest.raises(RuntimeError, match="config.json no encontrado"):
            get_host()


def test_get_warehouse_id_raises_runtime_error_when_config_missing(tmp_path):
    missing = tmp_path / "config.json"
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "_CONFIG_FILE", missing):
        with pytest.raises(RuntimeError, match="config.json no encontrado"):
            get_warehouse_id()


def test_get_host_raises_runtime_error_when_field_missing(tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps({"warehouse_id": "abc"}))  # host missing
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "_CONFIG_FILE", p):
        with pytest.raises(RuntimeError, match="le faltan campos"):
            get_host()


def test_get_session_file_raises_auth_required_when_missing(tmp_path):
    missing = tmp_path / "session.json"
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "SESSION_FILE", missing):
        with pytest.raises(AuthRequiredError):
            get_session_file()


def test_get_session_file_returns_path_when_present(tmp_path):
    p = tmp_path / "session.json"
    p.write_text("{}")
    import databricks_connector.browser_auth as ba
    with patch.object(ba, "SESSION_FILE", p):
        result = get_session_file()
        assert result == p
