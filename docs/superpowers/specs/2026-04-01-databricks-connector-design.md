# Databricks Connector — Design Spec
**Date:** 2026-04-01
**Status:** Approved
**Author:** natanahelbaruch

---

## Context

Kavak's data warehouse is migrating from AWS Redshift to Databricks within the next month. The existing analysis projects (`eeas_beta_analysis/kuna_analysis/`) use a direct psycopg2 connection to Redshift. Rather than rewriting each project individually, this spec defines a **general-purpose, reusable Databricks connector module** that any current or future project can import.

The connector is intentionally scope-limited to SQL reads now, with a clean extension point for job/notebook execution later.

**Constraints:**
- No Databricks Personal Access Token (PAT) — authenticate via OAuth only
- No Anthropic API key or claude.ai Databricks connector
- Do NOT modify `eeas_beta_analysis/` — Redshift connection stays active during transition
- Must return `pandas.DataFrame` to fit existing analysis pipeline

**Databricks workspace:** `https://dbc-6f0786a7-8ba5.cloud.databricks.com/`

---

## Architecture

### Project Layout

```
/home/natanahelbaruch/projects/databricks_connector/
├── databricks_connector/
│   ├── __init__.py          ← public API: query(), get_client()
│   ├── client.py            ← WorkspaceClient singleton + U2M OAuth init
│   ├── query.py             ← SQL execution → pandas DataFrame
│   └── cache.py             ← optional CSV cache with TTL
├── config.yaml              ← host + warehouse_id (no secrets, committed)
├── .env                     ← warehouse_id override (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

### Dependencies

```
databricks-sdk>=0.20.0
pandas>=2.0.0
python-dotenv
pyyaml
```

Python version: 3.12 (matches existing projects).

---

## Components

### `client.py` — WorkspaceClient Singleton

Initializes a single `WorkspaceClient` using U2M OAuth. On first call, the SDK opens a browser window for the user to authenticate against `dbc-6f0786a7-8ba5.cloud.databricks.com`. Tokens are cached by the SDK at `~/.databricks/token-cache.json` and refreshed silently on subsequent calls.

```python
# Initialization (called once per process)
from databricks.sdk import WorkspaceClient

_client = None

def get_client() -> WorkspaceClient:
    global _client
    if _client is None:
        _client = WorkspaceClient(host="https://dbc-6f0786a7-8ba5.cloud.databricks.com")
    return _client
```

No credentials are stored in the project. The SDK manages the OAuth token lifecycle.

### `query.py` — SQL Execution

Uses `WorkspaceClient.statement_execution` to run SQL against a configured SQL Warehouse. Always returns a `pandas.DataFrame`.

Key behaviors:
- Reads `warehouse_id` from `config.yaml` (or `.env` override)
- Raises a clear `DatabricksQueryError` on failure (not a raw SDK exception)
- Supports an optional `cache_key` + `cache_ttl_hours` argument — delegates to `cache.py`

```python
def query(sql: str, cache_key: str = None, cache_ttl_hours: float = 0) -> pd.DataFrame:
    ...
```

### `cache.py` — CSV Cache

Identical pattern to `kuna_analysis/cache/`. Saves query results as CSV files named by `cache_key` + date (Mexico City timezone). On subsequent calls within `cache_ttl_hours`, reads from CSV instead of hitting Databricks.

Cache directory: `~/.databricks_connector/cache/` (fixed path, not relative to caller — ensures consistent cache location regardless of which project imports the connector).

### `config.yaml` — Non-Secret Configuration

```yaml
databricks:
  host: https://dbc-6f0786a7-8ba5.cloud.databricks.com
  warehouse_id: ""   # fill in after warehouse provisioned
```

`warehouse_id` can be overridden via `.env`:
```
DATABRICKS_WAREHOUSE_ID=your_warehouse_id
```

### `__init__.py` — Public API

```python
from .query import query
from .client import get_client

__all__ = ["query", "get_client"]
```

Consumers only need to import from the top-level package.

---

## Data Flow

```
Caller (any project)
  └─ query("SELECT ...")
       ├─ cache hit? → return DataFrame from CSV
       └─ cache miss → get_client()
                         └─ WorkspaceClient (U2M OAuth)
                              └─ statement_execution.execute()
                                   └─ result → pandas DataFrame → (optionally cached) → return
```

---

## Authentication Flow

1. **First run:** `get_client()` triggers SDK OAuth U2M flow → browser opens → user logs in to `dbc-6f0786a7-8ba5.cloud.databricks.com`
2. **Token cached** at `~/.databricks/token-cache.json` (SDK-managed)
3. **Subsequent runs:** silent token refresh, no browser interaction
4. **Token expiry:** SDK handles refresh automatically

No credentials in `.env`, `config.yaml`, or committed files.

---

## Future Extension — Jobs & Notebooks

When ready to add job execution, create `databricks_connector/jobs.py`:

```python
from .client import get_client

def run_job(job_id: int, params: dict = None):
    w = get_client()
    return w.jobs.run_now(job_id=job_id, notebook_params=params)
```

Export from `__init__.py`. No changes to `query.py` or `client.py` required.

---

## Usage Example (from any project)

```python
from databricks_connector import query

# Simple query
df = query("SELECT * FROM catalog.schema.eeas_funnel WHERE fecha >= '2026-01-01'")

# Cached query (6-hour TTL)
df = query(
    "SELECT dealer_id, pix, bookings FROM catalog.schema.inventory",
    cache_key="inventory_daily",
    cache_ttl_hours=6
)
```

---

## What This Spec Does NOT Cover

- Databricks catalog/schema/table names — these are project-specific and will be added per project that imports this connector
- Unity Catalog permissions — assumed to be configured by Databricks admin
- Write operations — read-only for now
- Multi-workspace support — single workspace only

---

## Verification

After implementation:
1. `python -c "from databricks_connector import query"` — imports without error
2. First run triggers browser OAuth flow successfully
3. `query("SELECT 1 AS test")` returns `pd.DataFrame({'test': [1]})`
4. Second run with same `cache_key` reads from CSV without hitting Databricks
5. `get_client()` accessible from another project via `sys.path` append or pip install
