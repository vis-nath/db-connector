"""
Execute Databricks SQL queries via a Playwright browser session.

How it works:
  1. Loads saved session cookies into a headless Chromium browser.
  2. Navigates to the Databricks workspace (establishes same-origin context).
  3. Fetches a CSRF token from /auth/session/info.
  4. POSTs to /ajax-api/2.0/sql/statements — the internal cookie-authenticated
     endpoint (not the public /api/2.0/ which needs a PAT).
  5. Polls for results if the query is still running.
  6. Returns a pandas DataFrame.

No API tokens required — only the saved browser session.
"""

import asyncio

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from .browser_auth import get_host, get_warehouse_id, get_session_file, AuthRequiredError


class DatabricksQueryError(Exception):
    pass


# ── JS executed inside the browser ───────────────────────────────────────────

_JS_QUERY = """
async (args) => {
    try {
        // 1. Get CSRF token from session info
        const sessionR = await fetch('/auth/session/info', { credentials: 'include' });
        if (!sessionR.ok) return { httpStatus: sessionR.status, body: null, error: 'session/info failed' };
        const csrf = (await sessionR.json()).csrfToken;
        const headers = { 'Content-Type': 'application/json', 'X-Csrf-Token': csrf };

        // 2. Submit the SQL statement
        const r = await fetch('/ajax-api/2.0/sql/statements', {
            method: 'POST',
            headers: headers,
            credentials: 'include',
            body: JSON.stringify({
                warehouse_id: args.warehouse_id,
                statement:    args.sql,
                wait_timeout: '50s',
                disposition:  'INLINE',
                format:       'JSON_ARRAY',
            })
        });
        let body = await r.json();

        // 3. Poll while PENDING / RUNNING
        let polls = 0;
        while (body.status && (body.status.state === 'PENDING' || body.status.state === 'RUNNING') && polls < 60) {
            await new Promise(res => setTimeout(res, 3000));
            const poll = await fetch('/ajax-api/2.0/sql/statements/' + body.statement_id, {
                headers: headers,
                credentials: 'include'
            });
            body = await poll.json();
            polls++;
        }

        return { httpStatus: r.status, body: body, error: null };
    } catch (e) {
        return { httpStatus: -1, body: null, error: String(e) };
    }
}
"""

# ── async core ────────────────────────────────────────────────────────────────

async def _run(sql: str, warehouse_id: str) -> pd.DataFrame:
    session_file = str(get_session_file())  # raises AuthRequiredError if missing

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(storage_state=session_file)
        page = await context.new_page()

        # Navigate to the workspace so fetch() runs in the correct origin
        try:
            await page.goto(get_host(), wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except PlaywrightTimeout:
            pass

        result = await page.evaluate(_JS_QUERY, {"sql": sql, "warehouse_id": warehouse_id})

        # Refresh saved session
        await context.storage_state(path=session_file)
        await browser.close()

    # ── Error handling ────────────────────────────────────────────────────────
    if result.get("error"):
        raise DatabricksQueryError(f"JS error: {result['error']}")

    http_status = result.get("httpStatus")
    body = result.get("body") or {}

    if http_status in (401, 403) and "CSRF" not in str(body):
        raise AuthRequiredError(
            "Session expired.\n"
            "Run:  python3 /home/natanahelbaruch/projects/databricks_connector/setup_auth.py"
        )
    if http_status not in (200, 201):
        raise DatabricksQueryError(f"HTTP {http_status}: {body}")

    state = body.get("status", {}).get("state")
    if state != "SUCCEEDED":
        error_info = body.get("status", {}).get("error", body)
        raise DatabricksQueryError(f"Query {state}: {error_info}")

    # ── Build DataFrame ───────────────────────────────────────────────────────
    cols = [c["name"] for c in body["manifest"]["schema"]["columns"]]
    rows = (body.get("result") or {}).get("data_array") or []
    return pd.DataFrame(rows, columns=cols)


# ── public API ────────────────────────────────────────────────────────────────

def query(sql: str, warehouse_id: str | None = None) -> pd.DataFrame:
    """
    Execute a SQL query on Databricks and return a pandas DataFrame.

    Uses the saved browser session — no API tokens needed.
    Raises AuthRequiredError if the session has expired (re-run setup_auth.py).

    Example:
        from databricks_connector import query
        df = query("SELECT * FROM prd_refined.salesforce_latam_refined.vehicle LIMIT 100")
    """
    if warehouse_id is None:
        warehouse_id = get_warehouse_id()
    return asyncio.run(_run(sql, warehouse_id))
