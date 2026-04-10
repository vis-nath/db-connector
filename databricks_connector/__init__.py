# databricks_connector/__init__.py
from .query import query, DatabricksQueryError
from .auth import AuthRequiredError
from .session_check import check_session

__all__ = ["query", "DatabricksQueryError", "AuthRequiredError", "check_session"]
