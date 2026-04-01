import os
import time
import pandas as pd
import pytest
import databricks_connector.cache as cache_module


@pytest.fixture(autouse=True)
def temp_cache(tmp_path, monkeypatch):
    """Redirect cache writes to a temp dir for every test."""
    monkeypatch.setattr(cache_module, "CACHE_DIR", tmp_path)


def test_write_and_read_returns_same_dataframe():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    cache_module.write_cache("test_key", df)
    result = cache_module.read_cache("test_key", ttl_hours=1)
    pd.testing.assert_frame_equal(df, result)


def test_missing_cache_key_returns_none():
    result = cache_module.read_cache("nonexistent_key", ttl_hours=1)
    assert result is None


def test_expired_cache_returns_none():
    df = pd.DataFrame({"a": [1]})
    cache_module.write_cache("test_key", df)
    path = cache_module._cache_path("test_key")
    expired_time = time.time() - 7201  # 2 hours ago
    os.utime(path, (expired_time, expired_time))
    result = cache_module.read_cache("test_key", ttl_hours=1)
    assert result is None


def test_zero_ttl_never_uses_cache():
    df = pd.DataFrame({"a": [1]})
    cache_module.write_cache("test_key", df)
    result = cache_module.read_cache("test_key", ttl_hours=0)
    assert result is None
