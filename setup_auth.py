"""
One-time Databricks session setup.

Run from your terminal:

    python3 setup_auth.py

Chromium opens. Log in with your @kavak.com account. The browser navigates
to the SQL Warehouses page — once it lands there, the session is saved
and the browser closes automatically. No terminal interaction needed.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks_connector.browser_auth import SESSION_FILE, get_host

LOGIN_URL = "https://dbc-6f0786a7-8ba5.cloud.databricks.com/sql/warehouses"
SUCCESS_URL_PATTERN = "**/sql-warehouses**"
LOGIN_TIMEOUT_MS = 300_000  # 5 minutes — enough time for SSO


async def _do_login():
    from playwright.async_api import async_playwright

    print("\n" + "=" * 60)
    print("DATABRICKS SESSION SETUP")
    print("=" * 60)
    print("\nSe abrirá una ventana de Chrome en tu pantalla.")
    print("Inicia sesión con tu correo @kavak.com.")
    print("La sesión se guardará automáticamente — no necesitas volver a esta terminal.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        print("Navegador abierto. Inicia sesión ahora...")
        print("(El Chrome se cerrará solo cuando el login sea exitoso)\n")

        # Wait until SSO redirects back to the SQL warehouses page
        await page.wait_for_url(SUCCESS_URL_PATTERN, timeout=LOGIN_TIMEOUT_MS)

        print("\nLogin detectado. Guardando sesión...")
        await asyncio.sleep(2)  # let any final cookies settle

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
