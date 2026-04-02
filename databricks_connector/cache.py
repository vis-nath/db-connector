import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

CACHE_DIR = Path.home() / ".databricks_connector" / "cache"


def _ensure_cache_dir() -> None:
    """Create cache directory if it doesn't exist. Raises OSError on failure."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Cannot create cache directory {CACHE_DIR}: {e}") from e


def _cache_path(cache_key: str) -> Path:
    """Return the cache file path for a given key (Mexico City date suffix)."""
    tz = pytz.timezone("America/Mexico_City")
    date_str = datetime.now(tz).strftime("%Y-%m-%d")
    return CACHE_DIR / f"{cache_key}_{date_str}.csv"


def read_cache(cache_key: str, ttl_hours: float) -> pd.DataFrame | None:
    """Return cached DataFrame if it exists and is within TTL, else None."""
    if ttl_hours <= 0:
        return None
    path = _cache_path(cache_key)
    if not path.exists():
        return None
    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > ttl_hours * 3600:
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def write_cache(cache_key: str, df: pd.DataFrame) -> None:
    """Write DataFrame to cache as CSV."""
    _ensure_cache_dir()
    path = _cache_path(cache_key)
    df.to_csv(path, index=False)
