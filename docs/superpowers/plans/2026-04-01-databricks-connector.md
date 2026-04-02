# Databricks Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable Python package that executes SQL queries against a Databricks SQL Warehouse using U2M OAuth and returns `pandas.DataFrame` results, with optional CSV caching.

**Architecture:** A 4-module package (`client`, `query`, `cache`, `__init__`) wraps the `databricks-sdk` WorkspaceClient. Auth is handled entirely by the SDK's U2M OAuth flow — no credentials stored in the project. Cache writes to `~/.databricks_connector/cache/` so it works across any importing project.

**Tech Stack:** Python 3.12, `databricks-sdk>=0.20.0`, `pandas>=2.0.0`, `pytz`, `pyyaml`, `python-dotenv`, `pytest`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `databricks_connector/__init__.py` | Create | Public API: `query()`, `get_client()` |
| `databricks_connector/client.py` | Create | WorkspaceClient singleton + U2M OAuth |
| `databricks_connector/query.py` | Create | SQL execution → DataFrame + cache integration |
| `databricks_connector/cache.py` | Create | CSV cache read/write with TTL |
| `config.yaml` | Create | Non-secret config: host, warehouse_id |
| `.env` | Create | warehouse_id override (gitignored) |
| `.gitignore` | Create | Exclude .env, __pycache__, .venv, cache |
| `requirements.txt` | Create | All dependencies pinned |
| `tests/__init__.py` | Create | Empty, marks tests as package |
| `tests/test_cache.py` | Create | Unit tests for cache module |
| `tests/test_client.py` | Create | Unit tests for client singleton |
| `tests/test_query.py` | Create | Unit tests for query execution + cache integration |

---

## Task 1: Project Scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `.gitignore`
- Create: `config.yaml`
- Create: `.env`
- Create: `databricks_connector/__init__.py` (empty placeholder)
- Create: `tests/__init__.py`

- [ ] **Step 1: Init git repo and create project structure**

```bash
cd /home/natanahelbaruch/projects/databricks_connector
git init
mkdir -p databricks_connector tests
touch databricks_connector/__init__.py tests/__init__.py
```

- [ ] **Step 2: Create `requirements.txt`**

```
databricks-sdk>=0.20.0
pandas>=2.0.0
pytz>=2024.1
pyyaml>=6.0.1
python-dotenv>=1.0.0
pytest>=8.0.0
pytest-mock>=3.12.0
```

- [ ] **Step 3: Create `.gitignore`**

```
.env
.venv/
__pycache__/
*.pyc
*.egg-info/
dist/
.pytest_cache/
~/.databricks_connector/
```

- [ ] **Step 4: Create `config.yaml`**

```yaml
databricks:
  host: https://dbc-6f0786a7-8ba5.cloud.databricks.com
  warehouse_id: ""   # fill in after SQL Warehouse HTTP path is provisioned
```

- [ ] **Step 5: Create `.env`** (gitignored — for local warehouse_id override)

```
# Uncomment and fill in to override config.yaml
# DATABRICKS_WAREHOUSE_ID=your_warehouse_id_here
```

- [ ] **Step 6: Create virtual environment and install dependencies**

```bash
cd /home/natanahelbaruch/projects/databricks_connector
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Expected: All packages install without errors. `databricks-sdk` version printed by `pip show databricks-sdk`.

- [ ] **Step 7: Initial commit**

```bash
git add requirements.txt .gitignore config.yaml .env databricks_connector/__init__.py tests/__init__.py
git commit -m "chore: project scaffolding"
```

---

## Task 2: Cache Module

**Files:**
- Create: `databricks_connector/cache.py`
- Create: `tests/test_cache.py`

- [ ] **Step 1: Write failing tests for `cache.py`**

Create `tests/test_cache.py`:

```python
import os
import time
import pandas as pd
import pytest
import databricks_connector.cache as cache_module


@pytest.fixture(autouse=True)
def temp_cache(tmp_path, monkeypatch):
    """Redirect cache writes to a temp dir for every test."""
    monkeypatch.setattr(cache_module, "CACHE_DIR", tmp_path)


def test_write_and_read_returns_same_dataframe():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    cache_module.write_cache("test_key", df)
    result = cache_module.read_cache("test_key", ttl_hours=1)
    pd.testing.assert_frame_equal(df, result)


def test_missing_cache_key_returns_none():
    result = cache_module.read_cache("nonexistent_key", ttl_hours=1)
    assert result is None


def test_expired_cache_returns_none():
    df = pd.DataFrame({"a": [1]})
    cache_module.write_cache("test_key", df)
    path = cache_module._cache_path("test_key")
    expired_time = time.time() - 7201  # 2 hours ago
    os.utime(path, (expired_time, expired_time))
    result = cache_module.read_cache("test_key", ttl_hours=1)
    assert result is None


def test_zero_ttl_never_uses_cache():
    df = pd.DataFrame({"a": [1]})
    cache_module.write_cache("test_key", df)
    result = cache_module.read_cache("test_key", ttl_hours=0)
    assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
source .venv/bin/activate
pytest tests/test_cache.py -v
```

Expected: `ModuleNotFoundError` or `ImportError` — `cache.py` does not exist yet.

- [ ] **Step 3: Implement `databricks_connector/cache.py`**

```python
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

CACHE_DIR = Path.home() / ".databricks_connector" / "cache"


def _cache_path(cache_key: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tz = pytz.timezone("America/Mexico_City")
    date_str = datetime.now(tz).strftime("%Y-%m-%d")
    return CACHE_DIR / f"{cache_key}_{date_str}.csv"


def read_cache(cache_key: str, ttl_hours: float) -> pd.DataFrame | None:
    """Return cached DataFrame if it exists and is within TTL, else None."""
    if ttl_hours <= 0:
        return None
    path = _cache_path(cache_key)
    if not path.exists():
        return None
    age_seconds = time.time() - path.stat().st_mtime
    if age_seconds > ttl_hours * 3600:
        return None
    return pd.read_csv(path)


def write_cache(cache_key: str, df: pd.DataFrame) -> None:
    """Write DataFrame to cache as CSV."""
    path = _cache_path(cache_key)
    df.to_csv(path, index=False)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_cache.py -v
```

Expected: 4 tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add databricks_connector/cache.py tests/test_cache.py
git commit -m "feat: add CSV cache module with TTL"
```

---

## Task 3: Client Module

**Files:**
- Create: `databricks_connector/client.py`
- Create: `tests/test_client.py`

- [ ] **Step 1: Write failing tests for `client.py`**

Create `tests/test_client.py`:

```python
import pytest
from unittest.mock import patch, MagicMock
import databricks_connector.client as client_module


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton before each test."""
    client_module._client = None
    yield
    client_module._client = None


def test_get_client_creates_workspace_client_with_correct_host():
    with patch("databricks_connector.client.WorkspaceClient") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        result = client_module.get_client()
        MockClient.assert_called_once_with(
            host="https://dbc-6f0786a7-8ba5.cloud.databricks.com"
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_client.py -v
```

Expected: `ImportError` — `client.py` does not exist yet.

- [ ] **Step 3: Implement `databricks_connector/client.py`**

```python
from databricks.sdk import WorkspaceClient

_HOST = "https://dbc-6f0786a7-8ba5.cloud.databricks.com"
_client: WorkspaceClient | None = None


def get_client() -> WorkspaceClient:
    """
    Return a singleton WorkspaceClient authenticated via U2M OAuth.

    First call opens a browser window for login.
    Subsequent calls return the cached instance silently.
    Token refresh is handled automatically by the SDK.
    """
    global _client
    if _client is None:
        _client = WorkspaceClient(host=_HOST)
    return _client
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_client.py -v
```

Expected: 2 tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add databricks_connector/client.py tests/test_client.py
git commit -m "feat: add WorkspaceClient singleton with U2M OAuth"
```

---

## Task 4: Query Module

**Files:**
- Create: `databricks_connector/query.py`
- Create: `tests/test_query.py`

- [ ] **Step 1: Write failing tests for `query.py`**

Create `tests/test_query.py`:

```python
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
    response.manifest.schema.columns = [MagicMock(name=c) for c in cols]
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_query.py -v
```

Expected: `ImportError` — `query.py` does not exist yet.

- [ ] **Step 3: Implement `databricks_connector/query.py`**

```python
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
        cache_key: If provided, check/write CSV cache at ~/.databricks_connector/cache/.
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_query.py -v
```

Expected: 5 tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add databricks_connector/query.py tests/test_query.py
git commit -m "feat: add SQL query execution with cache integration"
```

---

## Task 5: Public API + Full Test Suite

**Files:**
- Modify: `databricks_connector/__init__.py`

- [ ] **Step 1: Implement `databricks_connector/__init__.py`**

```python
from .query import query, DatabricksQueryError
from .client import get_client

__all__ = ["query", "get_client", "DatabricksQueryError"]
```

- [ ] **Step 2: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: 11 tests PASSED, 0 failed.

- [ ] **Step 3: Verify import works from outside the package**

```bash
cd /tmp
python3.12 -c "
import sys
sys.path.insert(0, '/home/natanahelbaruch/projects/databricks_connector')
from databricks_connector import query, get_client, DatabricksQueryError
print('Import OK:', query, get_client, DatabricksQueryError)
"
```

Expected: `Import OK: <function query ...> <function get_client ...> <class 'DatabricksQueryError'>`

- [ ] **Step 4: Commit**

```bash
cd /home/natanahelbaruch/projects/databricks_connector
git add databricks_connector/__init__.py
git commit -m "feat: expose public API from package root"
```

---

## Task 6: README + OAuth Smoke Test

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create `README.md`**

```markdown
# databricks_connector

General-purpose Databricks SQL connector for local Python analysis.
Authenticates via U2M OAuth (browser login once, silent refresh after).
Returns `pandas.DataFrame`. Optional CSV caching.

## Setup

1. Install dependencies:
   ```bash
   python3.12 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Set your SQL Warehouse ID in `config.yaml`:
   ```yaml
   databricks:
     host: https://dbc-6f0786a7-8ba5.cloud.databricks.com
     warehouse_id: YOUR_WAREHOUSE_ID
   ```
   Or set `DATABRICKS_WAREHOUSE_ID` in `.env`.

3. On first use, a browser window opens for Databricks OAuth login.
   After login, tokens are cached at `~/.databricks/token-cache.json`.

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
```

- [ ] **Step 2: Commit README**

```bash
git add README.md
git commit -m "docs: add README with setup and usage"
```

- [ ] **Step 3: OAuth smoke test (manual — requires Databricks access)**

> ⚠️ This step requires a valid `warehouse_id` in `config.yaml`.
> Get it from your Databricks workspace: SQL Warehouses → your warehouse → Connection Details → HTTP Path.
> The `warehouse_id` is the last segment of the HTTP path (e.g. `/sql/1.0/warehouses/abc123` → `abc123`).

```bash
source .venv/bin/activate
python3.12 -c "
from databricks_connector import query
df = query('SELECT 1 AS test_col')
print(df)
print('SUCCESS — OAuth and warehouse connection working')
"
```

Expected:
1. Browser opens to `dbc-6f0786a7-8ba5.cloud.databricks.com` for login
2. After login, terminal prints:
```
   test_col
0         1
SUCCESS — OAuth and warehouse connection working
```

- [ ] **Step 4: Final commit**

```bash
git add .
git commit -m "chore: project complete — databricks_connector v0.1.0"
```

---

## Vault: Add Project to Context

After the smoke test passes:

- [ ] Create `vault-claude-kavak/Projects/databricks_connector/` with 4 notes (overview, data_patterns, ui_design_system, file_map)
- [ ] Update `vault-claude-kavak/CLAUDE.md` active projects table
- [ ] Update `~/.claude/CLAUDE.md` active projects list
