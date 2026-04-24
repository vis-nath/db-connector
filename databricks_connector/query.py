"""
Execute Databricks SQL queries using the official databricks-sql-connector.

If DATABRICKS_TOKEN is set in ~/.databricks_connector/.env, API key auth is used.
Otherwise, auth falls back to the Databricks SDK's external-browser OAuth flow.
"""

from pathlib import Path

import pandas as pd
from databricks import sql
from databricks.sql.exc import Error as _SqlError

from .auth import AuthRequiredError, get_host, get_http_path, get_token


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


def read_sql(path: str) -> str:
    """
    Read a .sql file and return its contents as a string.

    Intended as a pre-processing step before passing the query to query():
        df = query(read_sql("databricks_query/joined_data.sql"))

    Raises:
        FileNotFoundError: if the path does not exist
        ValueError: if the file does not have a .sql extension
    """
    p = Path(path)
    if p.suffix.lower() != ".sql":
        raise ValueError(f"Expected a .sql file, got: {path}")
    return p.read_text(encoding="utf-8")


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

    token = get_token()
    connect_kwargs = dict(server_hostname=hostname, http_path=http_path)
    if token:
        connect_kwargs["access_token"] = token
    else:
        connect_kwargs["auth_type"] = "external-browser"

    try:
        with sql.connect(**connect_kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=columns)
    except Exception as e:
        if _is_auth_error(e):
            raise AuthRequiredError(
                f"Autenticación fallida: {e}\n"
                "Verifica que DATABRICKS_TOKEN en ~/.databricks_connector/.env sea válido,\n"
                "o ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
            ) from e
        raise DatabricksQueryError(str(e)) from e
