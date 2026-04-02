# databricks_connector

General-purpose Databricks SQL connector for local Python analysis.
Authenticates via U2M OAuth (browser login once, silent refresh after).
Returns `pandas.DataFrame`. Optional pickle cache.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set your SQL Warehouse ID in `config.yaml`:
   ```yaml
   databricks:
     host: https://dbc-6f0786a7-8ba5.cloud.databricks.com
     warehouse_id: YOUR_WAREHOUSE_ID
   ```
   Get it from: Databricks workspace → SQL Warehouses → your warehouse → Connection Details → HTTP Path (last segment).

   Or set `DATABRICKS_WAREHOUSE_ID` in `.env`.

3. On first use, a browser window opens for Databricks OAuth login.
   Tokens are cached at `~/.databricks/token-cache.json` — subsequent runs are silent.

## Usage

```python
from databricks_connector import query

# Simple query
df = query("SELECT * FROM catalog.schema.table LIMIT 100")

# With cache (skip query if result is <6 hours old)
df = query("SELECT ...", cache_key="my_query", cache_ttl_hours=6)
```

## Future: Job Execution

`get_client()` returns the raw `WorkspaceClient` for jobs/notebooks:

```python
from databricks_connector import get_client
w = get_client()
w.jobs.run_now(job_id=123)
```

## Adding to Another Project

```python
import sys
sys.path.insert(0, "/home/natanahelbaruch/projects/databricks_connector")
from databricks_connector import query
```
