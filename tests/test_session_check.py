import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path


def test_check_session_returns_false_when_file_missing(tmp_path):
    fake_session = tmp_path / "session.json"  # does not exist
    with patch("databricks_connector.session_check.SESSION_FILE", fake_session):
        from databricks_connector.session_check import check_session
        assert check_session() is False


def test_check_session_returns_true_when_session_info_ok(tmp_path):
    fake_session = tmp_path / "session.json"
    fake_session.write_text("{}")  # must exist for the file check

    js_result = {"ok": True, "status": 200, "hasToken": True}

    with patch("databricks_connector.session_check.SESSION_FILE", fake_session), \
         patch("databricks_connector.session_check._check_async", new_callable=AsyncMock, return_value=True):
        from databricks_connector.session_check import check_session
        assert check_session() is True


def test_check_session_returns_false_when_session_info_fails(tmp_path):
    fake_session = tmp_path / "session.json"
    fake_session.write_text("{}")

    with patch("databricks_connector.session_check.SESSION_FILE", fake_session), \
         patch("databricks_connector.session_check._check_async", new_callable=AsyncMock, return_value=False):
        from databricks_connector.session_check import check_session
        assert check_session() is False
