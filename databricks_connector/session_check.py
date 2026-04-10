"""
Check whether the Databricks SDK token cache has credentials for our workspace.
No network calls — purely checks file presence and content.
"""

import json
from pathlib import Path

_SDK_TOKEN_CACHE = Path.home() / ".databricks" / "token-cache.json"


def check_session() -> bool:
    """
    Return True if the Databricks SDK token cache exists and is non-empty.
    Returns False if the file is missing, empty, or unreadable.
    False means setup_auth.py needs to be run.
    """
    if not _SDK_TOKEN_CACHE.exists():
        return False
    try:
        with open(_SDK_TOKEN_CACHE) as f:
            data = json.load(f)
        return bool(data)
    except Exception:
        return False
