"""
Execute Databricks SQL queries using the official databricks-sql-connector.

Auth is delegated to the Databricks SDK's external-browser OAuth flow.
Tokens are cached at ~/.databricks/token-cache.json and refreshed silently by the SDK.
"""

import pandas as pd
from databricks import sql
from databricks.sql.exc import Error as _SqlError

from .auth import AuthRequiredError, get_host, get_http_path


class DatabricksQueryError(Exception):
    pass


_AUTH_KEYWORDS = (
    "401", "403",
    "unauthorized", "unauthenticated",
    "invalid token", "token expired", "token is expired",
    "access denied", "permission_denied",
)


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


def query(sql_query: str, http_path: str | None = None) -> pd.DataFrame:
    """
    Execute a SQL query on Databricks and return a pandas DataFrame.

    Auth is handled by the Databricks SDK:
    - Cached token → silent query (no browser)
    - Expired access token + valid refresh token → silent refresh (no browser)
    - Both tokens expired → raises AuthRequiredError (run setup_auth.py)

    Raises:
        AuthRequiredError: authentication failed — run setup_auth.py
        DatabricksQueryError: SQL query failed for non-auth reasons

    Example:
        from databricks_connector import query
        df = query("SELECT * FROM prd_refined.schema.table LIMIT 100")
    """
    host = get_host()
    if http_path is None:
        http_path = get_http_path()
    hostname = host.replace("https://", "").replace("http://", "").rstrip("/")

    try:
        with sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            auth_type="external-browser",
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)
    except Exception as e:
        if _is_auth_error(e):
            raise AuthRequiredError(
                f"Autenticación fallida: {e}\n"
                "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
            ) from e
        raise DatabricksQueryError(str(e)) from e
