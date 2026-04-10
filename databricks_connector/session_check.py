# databricks_connector/session_check.py
"""
Check whether the saved Databricks OAuth token is still valid.
No network calls — purely checks expiry from token-cache.json.
"""

import time
from .auth import read_token_cache

_BUFFER_SECS = 60  # treat token as expired this many seconds before actual expiry


def check_session() -> bool:
    """
    Return True if the cached OAuth token is present and not expired.
    Returns False if missing, expired, or unreadable.

    Example:
        from databricks_connector import check_session
        if not check_session():
            print("Token expired — run setup_auth.py")
    """
    cache = read_token_cache()
    if not cache:
        return False
    expires_at = cache.get("expires_at", 0)
    access_token = cache.get("access_token")
    return bool(access_token and expires_at - time.time() > _BUFFER_SECS)
