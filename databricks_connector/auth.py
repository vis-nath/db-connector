"""
Config loading, session file paths, and token cache I/O.

No business logic — pure file reading/writing.
Config written by the databricks-setup Claude Code skill.
"""

import json
from pathlib import Path

_CONFIG_FILE = Path.home() / ".databricks_connector" / "config.json"
GOOGLE_SESSION_FILE = Path.home() / ".databricks_connector" / "google_session.json"
TOKEN_CACHE_FILE = Path.home() / ".databricks_connector" / "token-cache.json"


class AuthRequiredError(Exception):
    """Google session or OAuth tokens missing/expired. Run setup_auth.py."""


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


def get_google_session_file() -> Path:
    """Return path to Google session file, or raise AuthRequiredError if missing."""
    if not GOOGLE_SESSION_FILE.exists():
        raise AuthRequiredError(
            "No hay sesión de Google guardada.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )
    return GOOGLE_SESSION_FILE


def read_token_cache() -> dict | None:
    """Return cached token dict or None if file is missing or unreadable."""
    if not TOKEN_CACHE_FILE.exists():
        return None
    try:
        with open(TOKEN_CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def write_token_cache(data: dict) -> None:
    """Write token dict to cache file (creates parent dir if needed)."""
    TOKEN_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_CACHE_FILE, "w") as f:
        json.dump(data, f)
    TOKEN_CACHE_FILE.chmod(0o600)
