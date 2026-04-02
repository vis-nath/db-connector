import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
import databricks_connector.client as client_module
import databricks_connector.cache as cache_module


@pytest.fixture(autouse=True)
def reset_client():
    client_module._client = None
    yield
    client_module._client = None


@pytest.fixture(autouse=True)
def temp_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_module, "CACHE_DIR", tmp_path)


def _make_mock_client(cols: list[str], rows: list[list]) -> MagicMock:
    """Build a mock WorkspaceClient that returns given columns and rows."""
    mock_client = MagicMock()
    response = MagicMock()
    response.status.state.name = "SUCCEEDED"
    def _col(c):
        m = MagicMock()
        m.name = c
        return m
    response.manifest.schema.columns = [_col(c) for c in cols]
    response.result.data_array = rows
    mock_client.statement_execution.execute_statement.return_value = response
    return mock_client


def test_query_returns_dataframe_with_correct_columns_and_rows():
    from databricks_connector.query import query
    mock_client = _make_mock_client(["id", "name"], [["1", "foo"], ["2", "bar"]])
    with patch("databricks_connector.query.get_client", return_value=mock_client), \
         patch("databricks_connector.query._load_warehouse_id", return_value="wh_abc"):
        df = query("SELECT id, name FROM test_table")
    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2
    assert df.iloc[0]["name"] == "foo"


def test_query_raises_databricks_query_error_on_failure():
    from databricks_connector.query import query, DatabricksQueryError
    mock_client = MagicMock()
    response = MagicMock()
    response.status.state.name = "FAILED"
    response.status.error = "Table not found"
    mock_client.statement_execution.execute_statement.return_value = response
    with patch("databricks_connector.query.get_client", return_value=mock_client), \
         patch("databricks_connector.query._load_warehouse_id", return_value="wh_abc"):
        with pytest.raises(DatabricksQueryError, match="Table not found"):
            query("SELECT * FROM nonexistent_table")


def test_query_returns_cached_result_without_calling_databricks():
    from databricks_connector.query import query
    cached_df = pd.DataFrame({"x": [99]})
    cache_module.write_cache("my_key", cached_df)
    mock_client = MagicMock()
    with patch("databricks_connector.query.get_client", return_value=mock_client), \
         patch("databricks_connector.query._load_warehouse_id", return_value="wh_abc"):
        result = query("SELECT x FROM t", cache_key="my_key", cache_ttl_hours=1)
    mock_client.statement_execution.execute_statement.assert_not_called()
    pd.testing.assert_frame_equal(cached_df, result)


def test_query_writes_result_to_cache_when_cache_key_given():
    from databricks_connector.query import query
    mock_client = _make_mock_client(["val"], [["42"]])
    with patch("databricks_connector.query.get_client", return_value=mock_client), \
         patch("databricks_connector.query._load_warehouse_id", return_value="wh_abc"):
        query("SELECT val FROM t", cache_key="save_me", cache_ttl_hours=6)
    result = cache_module.read_cache("save_me", ttl_hours=6)
    assert result is not None
    assert result.iloc[0]["val"] == "42"


def test_query_raises_when_warehouse_id_not_configured():
    from databricks_connector.query import query
    with patch("databricks_connector.query.get_client"), \
         patch("databricks_connector.query._load_warehouse_id",
               side_effect=ValueError("warehouse_id not set")):
        with pytest.raises(ValueError, match="warehouse_id not set"):
            query("SELECT 1")
