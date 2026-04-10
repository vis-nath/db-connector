#!/usr/bin/env python3
"""
One-time Databricks session setup.

Run from your terminal:
    python3 setup_auth.py

Your default browser opens. Log in with your @kavak.com account (use Google SSO).
Tokens are cached at ~/.databricks/token-cache.json — no re-authentication needed
for daily use. The SDK refreshes tokens silently until the refresh token expires
(typically 30–90 days).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from databricks_connector.auth import get_host


def main():
    config_file = Path.home() / ".databricks_connector" / "config.json"
    if not config_file.exists():
        print("\n" + "=" * 60)
        print("ERROR: config.json no encontrado.")
        print("Ejecuta el skill de configuración en Claude Code:\n\n  /databricks-setup\n")
        print("=" * 60)
        sys.exit(1)

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config

        host = get_host()

        print("\n" + "=" * 60)
        print("DATABRICKS SESSION SETUP")
        print("=" * 60)
        print("\nSe abrirá tu navegador predeterminado.")
        print("Inicia sesión con tu correo @kavak.com (usa 'Continuar con Google').\n")

        config = Config(host=f"https://{host}", auth_type="external-browser")
        w = WorkspaceClient(config=config)

        # First authenticated API call triggers the browser OAuth flow.
        # Tokens are cached to ~/.databricks/token-cache.json on success.
        me = w.current_user.me()

        config_file.chmod(0o600)

        print(f"\n✓ Autenticado como: {me.user_name}")
        print("\n" + "=" * 60)
        print("Setup completo. Puedes ejecutar queries.")
        print("=" * 60)

    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
