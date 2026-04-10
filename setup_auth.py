#!/usr/bin/env python3
"""
One-time Databricks session setup.

Run from your terminal:
    python3 setup_auth.py

Chromium opens. Log in with your @kavak.com account (use Google SSO).
Once the SQL Warehouses page loads, the browser closes automatically.
Google cookies and a fresh OAuth token are saved for future headless use.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks_connector.auth import (
    GOOGLE_SESSION_FILE,
    TOKEN_CACHE_FILE,
    get_host,
    AuthRequiredError,
)

LOGIN_URL_TEMPLATE = "https://{host}/sql/warehouses"
SUCCESS_URL_PATTERN = "**/sql-warehouses**"
LOGIN_TIMEOUT_MS = 300_000  # 5 minutes


async def _do_login():
    from playwright.async_api import async_playwright

    host = get_host()
    login_url = LOGIN_URL_TEMPLATE.format(host=host)

    print("\n" + "=" * 60)
    print("DATABRICKS SESSION SETUP")
    print("=" * 60)
    print("\nSe abrirá una ventana de Chrome en tu pantalla.")
    print("Inicia sesión con tu correo @kavak.com (usa 'Continuar con Google').")
    print("La ventana se cerrará sola cuando el login sea exitoso.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(login_url, wait_until="domcontentloaded")
        print("Navegador abierto. Inicia sesión ahora...")
        print("(El Chrome se cerrará solo cuando el login sea exitoso)\n")

        await page.wait_for_url(SUCCESS_URL_PATTERN, timeout=LOGIN_TIMEOUT_MS)

        print("\nLogin detectado. Guardando cookies de Google...")
        await asyncio.sleep(2)  # let final cookies settle

        GOOGLE_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        await context.storage_state(path=str(GOOGLE_SESSION_FILE))
        GOOGLE_SESSION_FILE.chmod(0o600)
        print(f"Cookies guardadas en {GOOGLE_SESSION_FILE}")

        await browser.close()


def main():
    # Verify config.json exists
    config_file = Path.home() / ".databricks_connector" / "config.json"
    if not config_file.exists():
        print("\n" + "=" * 60)
        print("ERROR: config.json no encontrado.")
        print("\nAntes de correr este script, ejecuta el skill de configuración")
        print("en Claude Code:\n\n  /databricks-setup\n")
        print("=" * 60)
        sys.exit(1)

    # Remove stale session files
    for f in (GOOGLE_SESSION_FILE, TOKEN_CACHE_FILE):
        if f.exists():
            f.unlink()

    try:
        asyncio.run(_do_login())
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)

    # Bootstrap first OAuth token using the freshly saved Google cookies
    print("\nObteniendo token OAuth inicial...")
    try:
        from databricks_connector.google_auth import reauth
        reauth()
        print(f"Token OAuth guardado en {TOKEN_CACHE_FILE}")
    except AuthRequiredError as e:
        print(f"\nAdvertencia: no se pudo obtener token OAuth: {e}")
        print("Esto es inusual — intenta correr setup_auth.py de nuevo.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Setup completo. Puedes ejecutar queries.")
    print("=" * 60)


if __name__ == "__main__":
    main()
