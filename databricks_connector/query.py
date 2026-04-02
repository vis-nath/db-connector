import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

from .cache import read_cache, write_cache
from .client import get_client

load_dotenv()


class DatabricksQueryError(Exception):
    """Raised when a Databricks SQL statement fails."""


def _load_warehouse_id() -> str:
    """Read warehouse_id from env var or config.yaml. Raises ValueError if missing."""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
    if warehouse_id:
        return warehouse_id
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    warehouse_id = config.get("databricks", {}).get("warehouse_id", "").strip()
    if not warehouse_id:
        raise ValueError(
            "warehouse_id not set. Add it to config.yaml or set "
            "DATABRICKS_WAREHOUSE_ID in .env"
        )
    return warehouse_id


def query(
    sql: str,
    cache_key: str = None,
    cache_ttl_hours: float = 0,
) -> pd.DataFrame:
    """
    Execute a SQL statement against the Databricks SQL Warehouse.

    Args:
        sql: SQL query string.
        cache_key: If provided, check/write pickle cache at ~/.databricks_connector/cache/.
        cache_ttl_hours: Max age of cached result in hours. 0 disables cache reads.

    Returns:
        pandas.DataFrame with query results.

    Raises:
        DatabricksQueryError: If the SQL statement fails.
        ValueError: If warehouse_id is not configured.
    """
    if cache_key and cache_ttl_hours > 0:
        cached = read_cache(cache_key, cache_ttl_hours)
        if cached is not None:
            return cached

    w = get_client()
    warehouse_id = _load_warehouse_id()

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )

    if response.status.state.name != "SUCCEEDED":
        raise DatabricksQueryError(
            f"Query failed: {response.status.error}"
        )

    cols = [col.name for col in response.manifest.schema.columns]
    rows = [list(row) for row in (response.result.data_array or [])]
    df = pd.DataFrame(rows, columns=cols)

    if cache_key:
        write_cache(cache_key, df)

    return df
