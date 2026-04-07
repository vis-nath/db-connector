# db-connector Team Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Distribute the Databricks browser-session connector to the full Kavak team via a public GitHub repo + a self-contained Claude Code skill that handles setup, querying, and session expiry — no tokens, no OAuth, no admin permissions required.

**Architecture:** Python package code lives in a public repo (no credentials). Kavak-specific values (HOST, WAREHOUSE_ID) live only in the skill file and are written to `~/.databricks_connector/config.json` at setup time. A Playwright headless browser session handles all queries via `/ajax-api/2.0/sql/statements` + CSRF.

**Tech Stack:** Python 3.12, Playwright (Chromium), pandas, requests, GitHub SSH, Claude Code skills

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `databricks_connector/browser_auth.py` | Modify | Read HOST + WAREHOUSE_ID from `~/.databricks_connector/config.json` instead of hardcoded |
| `setup_auth.py` | Modify | Verify config.json exists before proceeding; clear error if not |
| `.gitignore` | Modify | Ensure `config.json` and `session.json` are excluded |
| `README.md` | Create | Public-safe install + usage docs (no sensitive values) |
| `~/.claude/plugins/skills/databricks-setup.md` | Create | The full team skill — contains HOST, WAREHOUSE_ID, Spanish setup instructions, query trigger |

---

## Task 1: Read HOST and WAREHOUSE_ID from config.json

**Files:**
- Modify: `databricks_connector/browser_auth.py`

- [ ] **Step 1: Read current browser_auth.py**

```bash
cat databricks_connector/browser_auth.py
```

- [ ] **Step 2: Replace hardcoded constants with config loader**

Replace the top of `databricks_connector/browser_auth.py` so it reads from `~/.databricks_connector/config.json`:

```python
"""
Session management for Databricks browser auth.

No tokens. No OAuth. Just:
  1. setup_auth.py  → opens Chromium, user logs in via SSO, session saved.
  2. get_session_file() → returns path to saved session (cookies).
  3. Queries run fetch() inside the browser, authenticated via cookies.

Config is read from ~/.databricks_connector/config.json.
Write that file by running the databricks-setup skill in Claude Code.
"""

import json
from pathlib import Path

SESSION_FILE = Path.home() / ".databricks_connector" / "session.json"
_CONFIG_FILE = Path.home() / ".databricks_connector" / "config.json"


def _load_config() -> dict:
    """Load HOST and WAREHOUSE_ID from ~/.databricks_connector/config.json."""
    if not _CONFIG_FILE.exists():
        raise RuntimeError(
            "config.json no encontrado. Ejecuta el skill /databricks-setup en Claude Code primero.\n"
            f"Ruta esperada: {_CONFIG_FILE}"
        )
    with open(_CONFIG_FILE) as f:
        cfg = json.load(f)
    missing = [k for k in ("host", "warehouse_id") if not cfg.get(k)]
    if missing:
        raise RuntimeError(f"config.json le faltan campos: {missing}")
    return cfg


def _get_host() -> str:
    return _load_config()["host"]


def _get_warehouse_id() -> str:
    return _load_config()["warehouse_id"]


# Lazy-loaded module-level values used by browser_query.py
def get_host() -> str:
    return _get_host()


def get_warehouse_id() -> str:
    return _get_warehouse_id()


class AuthRequiredError(Exception):
    """No valid session. Run: python3 ~/projects/databricks_connector/setup_auth.py"""


def get_session_file() -> Path:
    """Return session file path, or raise AuthRequiredError if missing."""
    if not SESSION_FILE.exists():
        raise AuthRequiredError(
            "No hay sesión guardada de Databricks.\n"
            "Ejecuta: python3 ~/projects/databricks_connector/setup_auth.py"
        )
    return SESSION_FILE
```

- [ ] **Step 3: Update browser_query.py to use get_host() and get_warehouse_id()**

In `databricks_connector/browser_query.py`, replace the import line:

Old:
```python
from .browser_auth import HOST, WAREHOUSE_ID, get_session_file, AuthRequiredError
```

New:
```python
from .browser_auth import get_host, get_warehouse_id, get_session_file, AuthRequiredError
```

Then replace every use of `HOST` with `get_host()` and `WAREHOUSE_ID` with `get_warehouse_id()` in the function body of `_run` and `query`.

In `_run`:
```python
async def _run(sql: str, warehouse_id: str) -> pd.DataFrame:
    session_file = str(get_session_file())

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(storage_state=session_file)
        page = await context.new_page()

        try:
            await page.goto(get_host(), wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except PlaywrightTimeout:
            pass

        result = await page.evaluate(_JS_QUERY, {"sql": sql, "warehouse_id": warehouse_id})

        await context.storage_state(path=session_file)
        await browser.close()
    # ... rest unchanged
```

In `query`:
```python
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
```

- [ ] **Step 4: Smoke test — should fail with clear message when config.json absent**

```bash
mv ~/.databricks_connector/config.json ~/.databricks_connector/config.json.bak 2>/dev/null || true
python3 -c "
import sys; sys.path.insert(0, '.')
from databricks_connector import query
try:
    query('SELECT 1')
except RuntimeError as e:
    print('PASS — got expected error:')
    print(e)
"
mv ~/.databricks_connector/config.json.bak ~/.databricks_connector/config.json 2>/dev/null || true
```

Expected output:
```
PASS — got expected error:
config.json no encontrado. Ejecuta el skill /databricks-setup en Claude Code primero.
```

- [ ] **Step 5: Smoke test — should work with config.json present**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from databricks_connector import query
df = query('SELECT 1 AS test')
print('PASS — shape:', df.shape)
print(df)
"
```

Expected:
```
PASS — shape: (1, 1)
  test
0    1
```

- [ ] **Step 6: Commit**

```bash
cd /home/natanahelbaruch/projects/databricks_connector
git add databricks_connector/browser_auth.py databricks_connector/browser_query.py
git commit -m "feat: read HOST and WAREHOUSE_ID from ~/.databricks_connector/config.json"
```

---

## Task 2: Update setup_auth.py to verify config before login

**Files:**
- Modify: `setup_auth.py`

- [ ] **Step 1: Add config check at the top of main()**

In `setup_auth.py`, add this check at the start of `main()` before any existing logic:

```python
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
    # ... rest of existing main() unchanged
```

Also add the `Path` import at the top if not already there:
```python
from pathlib import Path
```

- [ ] **Step 2: Test — should exit cleanly with clear Spanish message when config absent**

```bash
mv ~/.databricks_connector/config.json ~/.databricks_connector/config.json.bak
python3 setup_auth.py 2>&1; echo "Exit code: $?"
mv ~/.databricks_connector/config.json.bak ~/.databricks_connector/config.json
```

Expected:
```
ERROR: config.json no encontrado.
...
Exit code: 1
```

- [ ] **Step 3: Commit**

```bash
git add setup_auth.py
git commit -m "feat: setup_auth.py verifies config.json exists before proceeding"
```

---

## Task 3: Harden .gitignore

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Read current .gitignore**

```bash
cat .gitignore
```

- [ ] **Step 2: Ensure these lines are present**

Open `.gitignore` and verify (add if missing):

```gitignore
# Databricks local credentials — never commit
~/.databricks_connector/
config.json
session.json
auth.json
```

- [ ] **Step 3: Verify no sensitive files are tracked**

```bash
git status
git ls-files | grep -E "config|session|auth\.json"
```

Expected: no output from the grep (none of those files are tracked).

- [ ] **Step 4: Commit**

```bash
git add .gitignore
git commit -m "chore: ensure config.json and session.json excluded from repo"
```

---

## Task 4: Create README.md for public repo

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create README.md**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add public README"
```

---

## Task 5: Create GitHub repo and push

**Files:** none — git operations only

- [ ] **Step 1: Create the repo on GitHub.com**

Open `https://github.com/new` in your browser:
- Repository name: `db-connector`
- Visibility: **Public**
- Do NOT initialize with README (we already have one)
- Click **Create repository**

- [ ] **Step 2: Add remote and push**

```bash
cd /home/natanahelbaruch/projects/databricks_connector
git remote add origin git@github.com:vis-nath/db-connector.git
git branch -M main
git push -u origin main
```

Expected: branch pushed, GitHub shows all files.

- [ ] **Step 3: Verify sensitive files are NOT in the repo**

Open `https://github.com/vis-nath/db-connector` and confirm:
- No `config.json`
- No `session.json`
- No `auth.json`
- No hardcoded `dbc-6f0786a7` URL anywhere in `.py` files

```bash
grep -r "dbc-6f0786a7\|424412b3fb27e042\|3de9aee76c2f16f1" \
  --include="*.py" --include="*.yaml" --include="*.json" \
  /home/natanahelbaruch/projects/databricks_connector/ \
  2>/dev/null | grep -v __pycache__
```

Expected: no output.

---

## Task 6: Create the databricks-setup skill

**Files:**
- Create: `~/.claude/plugins/skills/databricks-setup.md`

This is the most important file. It contains the Kavak-specific values and all user-facing instructions.

- [ ] **Step 1: Create skill directory if needed**

```bash
mkdir -p ~/.claude/plugins/skills
```

- [ ] **Step 2: Create the skill file**

Create `~/.claude/plugins/skills/databricks-setup.md` with this exact content:

````markdown
---
name: databricks-setup
description: >
  Configuración e instalación del conector de Databricks para el equipo de Kavak.
  Úsalo cuando:
  - El usuario ejecute /databricks-setup
  - El usuario mencione "query", "consulta", "datos", "tabla", "SQL", "Databricks",
    "vehicle", "salesforce", "prd_refined", o pregunte por datos que podrían estar
    en Databricks
  - El usuario pida información de alguna tabla o catálogo de datos
  - Haya un AuthRequiredError o se mencione que la sesión expiró
---

# Databricks Setup & Query Skill

## Valores de configuración de Kavak (INTERNO — no compartir públicamente)

```python
KAVAK_HOST = "https://dbc-6f0786a7-8ba5.cloud.databricks.com"
KAVAK_WAREHOUSE_ID = "3de9aee76c2f16f1"
```

---

## Modo 1: Instalación completa (`/databricks-setup`)

Sigue estos pasos EN ORDEN cuando un usuario necesite configurar el conector por primera vez.

### Paso 1 de 5 — Clonar el repositorio

Ejecuta este comando:
```bash
git clone git@github.com:vis-nath/db-connector.git ~/projects/databricks_connector
```

Si aparece un error de SSH, el usuario necesita configurar su llave SSH con GitHub primero.
Pregunta si necesita ayuda con eso.

### Paso 2 de 5 — Instalar dependencias

```bash
cd ~/projects/databricks_connector
pip install -r requirements.txt --break-system-packages
playwright install chromium
```

Esto puede tomar 2-3 minutos. Es normal ver muchas líneas de descarga.

### Paso 3 de 5 — Crear archivo de configuración

Escribe este archivo Python y ejecútalo para crear el config:

```python
import json
from pathlib import Path

config = {
    "host": "https://dbc-6f0786a7-8ba5.cloud.databricks.com",
    "warehouse_id": "3de9aee76c2f16f1"
}

config_file = Path.home() / ".databricks_connector" / "config.json"
config_file.parent.mkdir(parents=True, exist_ok=True)
with open(config_file, "w") as f:
    json.dump(config, f, indent=2)
config_file.chmod(0o600)
print(f"Config guardado en {config_file}")
```

O con el Bash tool directamente:
```bash
mkdir -p ~/.databricks_connector
cat > ~/.databricks_connector/config.json << 'EOF'
{
  "host": "https://dbc-6f0786a7-8ba5.cloud.databricks.com",
  "warehouse_id": "3de9aee76c2f16f1"
}
EOF
chmod 600 ~/.databricks_connector/config.json
echo "Config guardado"
```

### Paso 4 de 5 — Iniciar sesión en Databricks

Dile al usuario exactamente esto antes de correr el comando:

> "Voy a abrir una ventana de Chrome en tu pantalla de Windows.
> Cuando aparezca, inicia sesión con tu correo de Kavak (@kavak.com) como lo haces normalmente.
> Una vez que veas el dashboard de Databricks completamente cargado,
> **vuelve a esta terminal y presiona Enter**.
> No cierres el Chrome antes de presionar Enter."

Luego ejecuta:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

⚠️ **IMPORTANTE para el usuario:** El script dice exactamente:
```
>>> Press Enter AFTER you are fully logged into the workspace:
```
El usuario debe presionar Enter en la terminal (no en el navegador) cuando el workspace esté cargado.

### Paso 5 de 5 — Verificar que funciona

```python
import sys
sys.path.insert(0, '/home/natanahelbaruch/projects/databricks_connector')
from databricks_connector import query

df = query("SELECT 1 AS conexion_exitosa")
print(df)
```

Si aparece:
```
   conexion_exitosa
0                1
```
¡La instalación fue exitosa! 🎉

Si aparece un error, ver la sección de solución de problemas al final.

---

## Modo 2: Ejecutar consultas de Databricks

Cuando el usuario mencione datos, tablas, o haga una pregunta sobre información
que podría estar en Databricks, pregunta:

> "¿Los datos que necesitas están en Databricks? Si es así, puedo consultarlos directamente
> y traerte los resultados como una tabla."

Si la respuesta es sí, usa este patrón para todas las consultas:

```python
import sys
sys.path.insert(0, '/home/natanahelbaruch/projects/databricks_connector')
from databricks_connector import query

df = query("""
    SELECT ...
    FROM prd_refined.schema.tabla
    LIMIT 100
""")
```

### Qué hace `query()` exactamente

- Devuelve un `pandas.DataFrame` con todos los resultados
- Puedes usar `.head()`, `.shape`, `.describe()`, `.groupby()`, `.filter()`, etc.
- Los datos viven en la sesión de Claude — no se guardan en disco salvo que el usuario lo pida
- El warehouse arranca automáticamente si estaba apagado (puede tomar 1-2 minutos)

### Catálogos disponibles en Kavak

- `prd_refined.*.*` — datos de producción refinados (Salesforce, operaciones, etc.)
- Ejemplo: `prd_refined.salesforce_latam_refined.vehicle`

---

## Modo 3: Sesión expirada (`AuthRequiredError`)

Cuando aparezca `AuthRequiredError` o el usuario diga que le salió un error de sesión,
dile:

> "Tu sesión de Databricks expiró (esto es normal, pasa cada 8-24 horas).
> Solo necesitas volver a iniciar sesión — no hace falta reinstalar nada."

Pasos:
1. Igual que el Paso 4 de instalación — abrir Chrome, iniciar sesión, presionar Enter
2. Ejecutar:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```
3. Volver a intentar la consulta original

---

## Solución de problemas

### Error: "git@github.com: Permission denied"
El usuario no tiene SSH configurado con GitHub.
Guíalo para generar una llave SSH y agregarla a su cuenta de GitHub.

### Error: "config.json no encontrado"
Saltarse al Paso 3 de instalación y crear el archivo config.

### El Chrome no abre / no se ve la ventana
Verificar que WSLg esté activo:
```bash
echo $DISPLAY
```
Debe devolver `:0`. Si no, el usuario necesita reiniciar WSL.

### La consulta tarda mucho (>2 minutos)
Normal si el warehouse estaba detenido. La primera consulta del día arranca el warehouse.
Esperar hasta 3 minutos.

### Error: "user is not authorized to use this warehouse"
El warehouse_id en config.json es incorrecto. Verificar con el administrador de Databricks.
````

- [ ] **Step 3: Verify the skill file was created**

```bash
ls -la ~/.claude/plugins/skills/databricks-setup.md
head -5 ~/.claude/plugins/skills/databricks-setup.md
```

Expected: file exists, first line is `---` (frontmatter).

- [ ] **Step 4: Test skill is recognized by Claude Code**

Restart Claude Code and type `/databricks-setup` — it should activate the skill.

---

## Self-Review

**Spec coverage check:**
- ✅ HOST + WAREHOUSE_ID removed from repo → Task 1 + Task 3
- ✅ config.json written during setup → Task 2 + Skill Step 3
- ✅ Public GitHub repo → Task 5
- ✅ Skill with Spanish instructions → Task 6
- ✅ Press-Enter step documented → Task 6 Paso 4
- ✅ Query trigger → Task 6 Modo 2
- ✅ DataFrame contract documented → Task 6 Modo 2
- ✅ Session expiry in Spanish → Task 6 Modo 3
- ✅ .gitignore hardened → Task 3

**Placeholder scan:** None found.

**Type consistency:** `get_host()` and `get_warehouse_id()` used consistently across Task 1 and Task 6.
