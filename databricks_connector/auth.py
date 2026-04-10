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
