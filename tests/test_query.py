import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, call

from databricks_connector.query import query, DatabricksQueryError
from databricks_connector.auth import AuthRequiredError


def _make_cursor(rows, columns):
    """Helper: mock cursor with fetchall + description."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = [(col, None, None, None, None, None, None) for col in columns]
    return cursor


def test_query_returns_dataframe_when_token_valid():
    cursor = _make_cursor([("a", 1), ("b", 2)], ["name", "value"])
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = cursor

    with patch("databricks_connector.query.get_valid_token", return_value="valid_tok"), \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"), \
         patch("databricks_connector.query.sql.connect", return_value=mock_conn):
        df = query("SELECT 1")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["name", "value"]
    assert len(df) == 2


def test_query_triggers_reauth_when_no_token():
    cursor = _make_cursor([], ["col"])
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = cursor

    # First call returns None (no token), second returns a token after reauth
    token_calls = iter([None, "new_tok"])

    with patch("databricks_connector.query.get_valid_token", side_effect=token_calls), \
         patch("databricks_connector.query.reauth") as mock_reauth, \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"), \
         patch("databricks_connector.query.sql.connect", return_value=mock_conn):
        query("SELECT 1")

    mock_reauth.assert_called_once()


def test_query_raises_auth_error_when_reauth_fails():
    with patch("databricks_connector.query.get_valid_token", return_value=None), \
         patch("databricks_connector.query.reauth"), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"):
        # Both token attempts return None → AuthRequiredError
        with patch("databricks_connector.query.get_valid_token", return_value=None):
            with pytest.raises(AuthRequiredError):
                query("SELECT 1")


def test_query_retries_once_on_server_auth_rejection():
    from databricks.sql.exc import Error as SqlError

    cursor = _make_cursor([], ["col"])
    mock_conn_ok = MagicMock()
    mock_conn_ok.__enter__ = MagicMock(return_value=mock_conn_ok)
    mock_conn_ok.__exit__ = MagicMock(return_value=False)
    mock_conn_ok.cursor.return_value = cursor

    call_count = {"n": 0}

    def connect_side_effect(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise SqlError("401 Unauthorized")
        return mock_conn_ok

    with patch("databricks_connector.query.get_valid_token", return_value="tok"), \
         patch("databricks_connector.query.reauth"), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"), \
         patch("databricks_connector.query.sql.connect", side_effect=connect_side_effect):
        df = query("SELECT 1")

    assert call_count["n"] == 2


def test_query_raises_databricks_query_error_on_sql_failure():
    from databricks.sql.exc import Error as SqlError

    with patch("databricks_connector.query.get_valid_token", return_value="tok"), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"), \
         patch("databricks_connector.query.sql.connect", side_effect=SqlError("Table not found")):
        with pytest.raises(DatabricksQueryError, match="Table not found"):
            query("SELECT * FROM nonexistent")
