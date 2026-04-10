"""
Execute Databricks SQL queries using the official databricks-sql-connector.

Auth flow:
  1. Try cached access token (or silent refresh via refresh token)
  2. If unavailable: full re-auth via Google SSO cookies (headless or visible browser)
  3. On server-side auth rejection: re-auth once and retry
"""

import pandas as pd
from databricks import sql
from databricks.sql.exc import Error as _SqlError

from .auth import AuthRequiredError, get_host, get_http_path
from .google_auth import get_valid_token, reauth


class DatabricksQueryError(Exception):
    pass


_AUTH_KEYWORDS = (
    "401", "403",
    "unauthorized", "unauthenticated",
    "invalid token", "token expired", "token is expired",
    "access denied", "permission_denied",
)


def _is_auth_error(exc: _SqlError) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


def _execute(host: str, http_path: str, access_token: str, sql_query: str) -> pd.DataFrame:
    hostname = host.replace("https://", "").replace("http://", "").rstrip("/")
    with sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(result, columns=columns)


def query(sql_query: str, http_path: str | None = None) -> pd.DataFrame:
    """
    Execute a SQL query on Databricks and return a pandas DataFrame.

    Handles auth automatically:
    - Uses cached OAuth token when valid
    - Silently refreshes via refresh token when expired
    - Re-authenticates via headless Google SSO cookies when needed
    - Opens a visible browser only when Google cookies have also expired

    Raises:
        AuthRequiredError: if re-authentication is impossible (user cancelled login)
        DatabricksQueryError: if the SQL query fails for non-auth reasons

    Example:
        from databricks_connector import query
        df = query("SELECT * FROM prd_refined.salesforce_latam_refined.vehicle LIMIT 100")
    """
    if http_path is None:
        http_path = get_http_path()
    host = get_host()

    # Step 1: ensure we have a valid token (refresh silently if needed)
    access_token = get_valid_token()
    if access_token is None:
        reauth()
        access_token = get_valid_token()
        if access_token is None:
            raise AuthRequiredError(
                "No se pudo obtener un token válido tras re-autenticación.\n"
                "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
            )

    # Step 2: execute query
    try:
        return _execute(host, http_path, access_token, sql_query)
    except _SqlError as e:
        if _is_auth_error(e):
            # Token was valid locally but server rejected it — re-auth and retry once
            reauth()
            access_token = get_valid_token()
            if access_token is None:
                raise AuthRequiredError("Re-autenticación fallida tras rechazo del servidor.") from e
            try:
                return _execute(host, http_path, access_token, sql_query)
            except _SqlError as e2:
                if _is_auth_error(e2):
                    raise AuthRequiredError(str(e2)) from e2
                raise DatabricksQueryError(str(e2)) from e2
        raise DatabricksQueryError(str(e)) from e
