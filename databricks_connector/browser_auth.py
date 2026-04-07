"""
Session management for Databricks browser auth.

No tokens. No OAuth. Just:
  1. setup_auth.py  → opens Chromium, user logs in via SSO, session saved.
  2. get_session_file() → returns path to saved session (cookies).
  3. Queries run fetch() inside the browser, authenticated via cookies.

Config is read from ~/.databricks_connector/config.json.
Write that file by running the databricks-setup skill in Claude Code.
"""

import json
from pathlib import Path

SESSION_FILE = Path.home() / ".databricks_connector" / "session.json"
_CONFIG_FILE = Path.home() / ".databricks_connector" / "config.json"


def _load_config() -> dict:
    """Load HOST and WAREHOUSE_ID from ~/.databricks_connector/config.json."""
    if not _CONFIG_FILE.exists():
        raise RuntimeError(
            "config.json no encontrado. Ejecuta el skill /databricks-setup en Claude Code primero.\n"
            f"Ruta esperada: {_CONFIG_FILE}"
        )
    with open(_CONFIG_FILE) as f:
        cfg = json.load(f)
    missing = [k for k in ("host", "warehouse_id") if not cfg.get(k)]
    if missing:
        raise RuntimeError(f"config.json le faltan campos: {missing}")
    return cfg


def get_host() -> str:
    return _load_config()["host"]


def get_warehouse_id() -> str:
    return _load_config()["warehouse_id"]


class AuthRequiredError(Exception):
    """No valid session. Run: python3 ~/projects/databricks_connector/setup_auth.py"""


def get_session_file() -> Path:
    """Return session file path, or raise AuthRequiredError if missing."""
    if not SESSION_FILE.exists():
        raise AuthRequiredError(
            "No hay sesión guardada de Databricks.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )
    return SESSION_FILE
