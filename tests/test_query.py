import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from databricks.sql.exc import Error as SqlError

from databricks_connector.query import query, DatabricksQueryError
from databricks_connector.auth import AuthRequiredError


def _make_cursor(rows, columns):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = [(col, None, None, None, None, None, None) for col in columns]
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _make_connection(cursor):
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


def test_query_returns_dataframe_on_success():
    cursor = _make_cursor([("a", 1), ("b", 2)], ["name", "value"])
    conn = _make_connection(cursor)
    with patch("databricks_connector.query.sql.connect", return_value=conn), \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"):
        df = query("SELECT 1")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["name", "value"]
    assert len(df) == 2


def test_query_passes_auth_type_external_browser():
    cursor = _make_cursor([], ["col"])
    conn = _make_connection(cursor)
    with patch("databricks_connector.query.sql.connect", return_value=conn) as mock_connect, \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/sql/1.0/warehouses/abc"):
        query("SELECT 1")
    assert mock_connect.call_args[1]["auth_type"] == "external-browser"


def test_query_uses_custom_http_path():
    cursor = _make_cursor([], ["col"])
    conn = _make_connection(cursor)
    with patch("databricks_connector.query.sql.connect", return_value=conn) as mock_connect, \
         patch("databricks_connector.query.get_host", return_value="host.com"), \
         patch("databricks_connector.query.get_http_path", return_value="/default"):
        query("SELECT 1", http_path="/custom/path")
    assert mock_connect.call_args[1]["http_path"] == "/custom/path"


def test_query_raises_auth_error_on_401():
    with patch("databricks_connector.query.sql.connect", side_effect=SqlError("401 Unauthorized")), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"):
        with pytest.raises(AuthRequiredError):
            query("SELECT 1")


def test_query_raises_databricks_query_error_on_sql_failure():
    with patch("databricks_connector.query.sql.connect", side_effect=SqlError("Table not found")), \
         patch("databricks_connector.query.get_host", return_value="h"), \
         patch("databricks_connector.query.get_http_path", return_value="/p"):
        with pytest.raises(DatabricksQueryError, match="Table not found"):
            query("SELECT * FROM nonexistent")
