"""
One-time Databricks session setup.

Run from your terminal:

    python3 setup_auth.py

Chromium opens. Log in completely. When you see the Databricks workspace,
come back to this terminal and press Enter. The session is then saved and
the browser closes.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks_connector.browser_auth import SESSION_FILE, get_host


async def _do_login():
    from playwright.async_api import async_playwright

    print("\n" + "=" * 60)
    print("DATABRICKS SESSION SETUP")
    print("=" * 60)
    print("\nChromium will open on your Windows desktop.")
    print("1. Log in completely with your @kavak.com SSO account.")
    print("2. Wait until the Databricks WORKSPACE is fully visible.")
    print("3. Come back here and press Enter.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(get_host(), wait_until="domcontentloaded")
        print("Browser opened. Log in now...\n")

        # Wait for the user to signal they are done — no automatic close
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: input(">>> Press Enter AFTER you are fully logged into the workspace: ")
        )

        print("\nSaving session...")
        await asyncio.sleep(2)  # brief pause so any final cookies settle

        SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(SESSION_FILE))
        SESSION_FILE.chmod(0o600)

        with open(SESSION_FILE) as f:
            saved = json.load(f)
        cookie_names = [c["name"] for c in saved.get("cookies", [])]
        print(f"Saved {len(cookie_names)} cookies: {cookie_names}")

        await browser.close()


def main():
    # Verify config.json exists — it is written by the databricks-setup skill
    config_file = Path.home() / ".databricks_connector" / "config.json"
    if not config_file.exists():
        print("\n" + "=" * 60)
        print("ERROR: config.json no encontrado.")
        print()
        print("Antes de correr este script, ejecuta el skill de configuración")
        print("en Claude Code:")
        print()
        print("  /databricks-setup")
        print()
        print("El skill creará el archivo config.json con los valores correctos")
        print("y luego te pedirá correr este script.")
        print("=" * 60)
        sys.exit(1)

    # Remove stale session silently so there's no interactive prompt
    if SESSION_FILE.exists():
        SESSION_FILE.unlink()

    try:
        asyncio.run(_do_login())
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)

    print(f"\nDone! Session saved to {SESSION_FILE}")
    print("You can now run queries.")


if __name__ == "__main__":
    main()
