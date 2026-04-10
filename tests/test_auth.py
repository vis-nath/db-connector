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
