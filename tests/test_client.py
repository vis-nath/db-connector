import pytest
from unittest.mock import patch, MagicMock
import databricks_connector.client as client_module


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton before each test."""
    client_module._client = None
    yield
    client_module._client = None


def test_get_client_creates_workspace_client_with_correct_params():
    with patch("databricks_connector.client.WorkspaceClient") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        result = client_module.get_client()
        MockClient.assert_called_once_with(
            host="https://dbc-6f0786a7-8ba5.cloud.databricks.com",
            auth_type="external-browser",
        )
        assert result is mock_instance


def test_get_client_returns_same_instance_on_repeated_calls():
    with patch("databricks_connector.client.WorkspaceClient") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        r1 = client_module.get_client()
        r2 = client_module.get_client()
        assert r1 is r2
        assert MockClient.call_count == 1
