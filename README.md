# db-connector

Query Databricks SQL from Python using a browser session — no API tokens or OAuth required.

Authenticates via SSO browser login (once). Returns `pandas.DataFrame`.

---

## Prerequisites

- WSL2 on Windows 11 (WSLg required for browser window)
- Python 3.12
- Git + SSH key configured for GitHub

---

## Installation

```bash
git clone git@github.com:vis-nath/db-connector.git ~/projects/databricks_connector
cd ~/projects/databricks_connector
pip install -r requirements.txt --break-system-packages
playwright install chromium
```

---

## Setup (first time only)

You need a `~/.databricks_connector/config.json` with your workspace's host and warehouse ID.
Get these values from your team's internal setup guide.

Once you have `config.json`, run:

```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

A browser window will open. Log in with your company SSO account.
When the workspace is fully loaded, **come back to the terminal and press Enter**.

---

## Usage

```python
from databricks_connector import query

df = query("SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
print(df.shape)
print(df.head())
```

`query()` returns a `pandas.DataFrame`. The warehouse starts automatically if stopped.

---

## Session expiry

Sessions last 8–24 hours. When a query raises `AuthRequiredError`, just re-login:

```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

No reinstall needed.

---

## How it works

Uses a Playwright headless browser with saved session cookies to call Databricks'
internal web API (`/ajax-api/2.0/sql/statements`) — the same endpoint the SQL editor uses.
No Bearer tokens required.

See `databricks_connector/browser_query.py` for implementation details.
