"""
Check whether credentials are available for Databricks queries.
No network calls — purely checks file presence and content.

Returns True if EITHER:
  - API token configured in ~/.databricks_connector/.env (preferred)
  - OAuth token cache exists at ~/.databricks/token-cache.json
"""

import json
from pathlib import Path

from .auth import get_token

_SDK_TOKEN_CACHE = Path.home() / ".databricks" / "token-cache.json"


def check_session() -> bool:
    """Return True if any valid auth credential is present."""
    if get_token():
        return True
    if not _SDK_TOKEN_CACHE.exists():
        return False
    try:
        with open(_SDK_TOKEN_CACHE) as f:
            data = json.load(f)
        return bool(data)
    except Exception:
        return False
