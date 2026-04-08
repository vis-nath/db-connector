"""
Check whether the saved Databricks browser session is still valid.

Hits /auth/session/info inside the browser (no warehouse needed — fast, <10s).
Returns True if session is valid, False if expired or missing.
"""

import asyncio

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from .browser_auth import get_host, SESSION_FILE


_JS_SESSION_CHECK = """
async () => {
    try {
        const r = await fetch('/auth/session/info', { credentials: 'include' });
        const body = await r.json().catch(() => ({}));
        return { ok: r.ok, status: r.status, hasToken: !!body.csrfToken };
    } catch (e) {
        return { ok: false, status: -1, hasToken: false };
    }
}
"""


async def _check_async() -> bool:
    if not SESSION_FILE.exists():
        return False

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        page = await context.new_page()
        try:
            await page.goto(get_host(), wait_until="domcontentloaded", timeout=20_000)
        except PlaywrightTimeout:
            pass
        result = await page.evaluate(_JS_SESSION_CHECK)
        await browser.close()

    return bool(result.get("ok") and result.get("hasToken"))


def check_session() -> bool:
    """
    Return True if the saved Databricks session is still valid, False if expired.

    Example:
        from databricks_connector import check_session
        if not check_session():
            print("Session expired — run setup_auth.py")
    """
    return asyncio.run(_check_async())
