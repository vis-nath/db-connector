import pytest
from databricks_connector.browser_query import _classify_http_error, _classify_query_state, DatabricksQueryError
from databricks_connector.browser_auth import AuthRequiredError


def test_403_with_error_code_raises_query_error():
    body = {"error_code": "PERMISSION_DENIED", "message": "User does not have SELECT privilege"}
    with pytest.raises(DatabricksQueryError, match="Permission denied"):
        _classify_http_error(403, body)


def test_403_with_empty_error_code_raises_query_error():
    body = {"error_code": "", "message": "Something went wrong"}
    with pytest.raises(DatabricksQueryError):
        _classify_http_error(403, body)


def test_403_with_no_api_structure_raises_auth_error():
    body = {}
    with pytest.raises(AuthRequiredError):
        _classify_http_error(403, body)


def test_401_with_no_api_structure_raises_auth_error():
    body = {}
    with pytest.raises(AuthRequiredError):
        _classify_http_error(401, body)


def test_403_warehouse_not_authorized_raises_query_error():
    body = {"error_code": "UNAUTHORIZED", "message": "user is not authorized to use this warehouse"}
    with pytest.raises(DatabricksQueryError, match="Permission denied"):
        _classify_http_error(403, body)


def test_500_raises_query_error():
    body = {"error": "Internal Server Error"}
    with pytest.raises(DatabricksQueryError, match="HTTP 500"):
        _classify_http_error(500, body)


def test_query_failed_with_message():
    body = {
        "status": {
            "state": "FAILED",
            "error": {"message": "Table not found: prd_refined.foo.bar", "error_code": "TABLE_NOT_FOUND"}
        }
    }
    with pytest.raises(DatabricksQueryError, match="Table not found"):
        _classify_query_state(body)


def test_query_failed_empty_error():
    body = {"status": {"state": "FAILED", "error": {}}}
    with pytest.raises(DatabricksQueryError, match="FAILED"):
        _classify_query_state(body)
