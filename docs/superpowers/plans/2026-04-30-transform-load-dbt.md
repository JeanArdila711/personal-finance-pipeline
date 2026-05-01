# Transform + Load + dbt Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar la capa de load (SQLite con upsert idempotente) y transformación (dbt-core con staging y marts) para el pipeline de finanzas personales, siguiendo arquitectura medallion Bronze→Silver→Gold.

**Architecture:** Python loader lee JSONs de `data/raw/`, aplana campos nested, y hace upsert en tablas `raw_wallet_*` de SQLite. dbt-core (con adapter dbt-duckdb) lee ese SQLite y corre modelos staging (limpieza/tipado) y marts (dimensiones + facts analíticos). Un orquestador CLI une extract → load en un solo comando.

**Tech Stack:** Python 3.13, SQLite (stdlib), dbt-core 1.8+, dbt-duckdb, pytest (SQLite :memory: para tests), ruff

---

## File Map

| Archivo | Acción | Responsabilidad |
|---------|--------|-----------------|
| `src/extract/extract_records.py` | Modificar | Fix bug: guarda params en vez de records |
| `src/load/__init__.py` | Modificar | Exportar SqliteLoader |
| `src/load/sqlite_loader.py` | Crear | create_schema + upserts + _flatten |
| `src/load/load_records.py` | Crear | CLI orquestador: lee raw/ → llama loader |
| `tests/test_sqlite_loader.py` | Crear | Tests con SQLite :memory: |
| `requirements.txt` | Modificar | Agregar dbt-core, dbt-duckdb |
| `dbt/profiles.yml` | Crear | Conexión dbt → finance.db |
| `dbt/dbt_project.yml` | Crear | Config del proyecto dbt |
| `dbt/models/staging/stg_wallet__accounts.sql` | Crear | Silver: cuentas limpias |
| `dbt/models/staging/stg_wallet__categories.sql` | Crear | Silver: categorías limpias |
| `dbt/models/staging/stg_wallet__records.sql` | Crear | Silver: transacciones limpias |
| `dbt/models/staging/schema.yml` | Crear | Tests dbt staging |
| `dbt/models/marts/dim_accounts.sql` | Crear | Gold: dimensión cuentas |
| `dbt/models/marts/dim_categories.sql` | Crear | Gold: dimensión categorías |
| `dbt/models/marts/fact_transactions.sql` | Crear | Gold: tabla de hechos |
| `dbt/models/marts/gastos_por_categoria_mensual.sql` | Crear | Gold: agregado analítico |
| `dbt/models/marts/schema.yml` | Crear | Tests dbt marts |

---

## Task 1: Fix bug en extract_records.py

El archivo `records_*.json` guarda los filtros de fecha en vez de las transacciones. El bug está en que `get_records()` retorna los `params` en vez del resultado de la API.

**Files:**
- Modify: `src/extract/extract_records.py`
- Test: `tests/test_wallet_client.py` (ya existe, verificar que pasa)

- [ ] **Step 1: Leer el archivo para identificar el bug**

```bash
# En src/extract/extract_records.py, buscar la sección de records:
# records = client.get_records(...)
# El bug: get_records recibe date_from/date_to y construye params internamente,
# pero probablemente está retornando params en vez del resultado paginado.
# Abrir src/extract/wallet_client.py y verificar get_records().
```

- [ ] **Step 2: Corregir get_records en wallet_client.py si el bug está ahí**

En `src/extract/wallet_client.py`, verificar que `get_records` retorna `list(self._paginated_get(...))` y NO los `params`. El método correcto debe verse así:

```python
def get_records(
    self,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict[str, Any]]:
    """
    Trae transacciones, opcionalmente filtradas por fecha.

    Args:
        date_from: ISO date 'YYYY-MM-DD'
        date_to: ISO date 'YYYY-MM-DD'
    """
    params: list[tuple[str, Any]] = []

    if date_from:
        params.append(("recordDate", f"gte.{date_from}"))
    if date_to:
        params.append(("recordDate", f"lt.{date_to}"))

    return list(self._paginated_get("/v1/api/records", params=params))
```

- [ ] **Step 3: Verificar que los tests existentes siguen pasando**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m pytest tests/test_wallet_client.py -v
```
Expected: 6 passed

- [ ] **Step 4: Correr extract real para verificar el fix**

```bash
.venv/Scripts/python -m src.extract.extract_records --days 90
```
Expected: archivos `records_*.json` con lista de objetos de transacciones (no solo 2 strings de fecha). Si hay 0 transacciones en los últimos 90 días, el JSON debe ser `[]` (lista vacía), no `["gte...", "lt..."]`.

- [ ] **Step 5: Commit**

```bash
git add src/extract/wallet_client.py src/extract/extract_records.py
git commit -m "fix: extract_records saves date filters instead of transactions"
```

---

## Task 2: Instalar dependencias dbt

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Agregar dbt-core y dbt-duckdb a requirements.txt**

Reemplazar el contenido de `requirements.txt` con:

```text
# HTTP & API
requests>=2.32.3
python-dotenv>=1.0.1

# Data manipulation
pandas>=2.3.0

# Data warehouse & transformations
dbt-core>=1.8.0
dbt-duckdb>=1.8.0

# Testing
pytest>=8.3.3
pytest-mock>=3.14.0
responses>=0.25.3

# Code quality
ruff>=0.7.4

# Dev utilities
ipython>=8.29.0
```

- [ ] **Step 2: Instalar las nuevas dependencias**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/pip install dbt-core>=1.8.0 dbt-duckdb>=1.8.0
```
Expected: Successfully installed dbt-core-1.8.x dbt-duckdb-1.8.x (y sus deps)

- [ ] **Step 3: Verificar instalación**

```bash
.venv/Scripts/dbt --version
```
Expected: `Core: 1.8.x` en el output

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "chore: add dbt-core and dbt-duckdb dependencies"
```

---

## Task 3: Implementar SqliteLoader

**Files:**
- Create: `src/load/sqlite_loader.py`
- Modify: `src/load/__init__.py`

- [ ] **Step 1: Escribir el test que falla primero**

Crear `tests/test_sqlite_loader.py`:

```python
"""Tests para SqliteLoader. Usan SQLite :memory: — sin mocks, instantáneo."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from src.load.sqlite_loader import SqliteLoader


@pytest.fixture
def loader() -> SqliteLoader:
    """Loader con DB en memoria pa' tests."""
    return SqliteLoader(db_path=":memory:")


def test_create_schema_idempotent(loader):
    """create_schema() puede llamarse dos veces sin explotar."""
    loader.create_schema()
    loader.create_schema()  # segunda vez — no debe lanzar excepción


def test_upsert_accounts_inserts_new(loader):
    """Inserta cuentas nuevas correctamente."""
    loader.create_schema()
    accounts = [
        {
            "id": "abc-123",
            "name": "Bancolombia",
            "archived": False,
            "color": "#FFB300",
            "accountType": "CurrentAccount",
            "initialBalance": {"value": 5000000, "currencyCode": "COP"},
            "excludeFromStats": False,
            "recordStats": {"recordCount": 100},
            "createdAt": "2025-03-10T04:29:18Z",
            "updatedAt": "2026-04-27T04:13:42Z",
        }
    ]
    loader.upsert_accounts(accounts)

    row = loader._conn.execute(
        "SELECT name, account_type, initial_balance_value FROM raw_wallet_accounts WHERE id = 'abc-123'"
    ).fetchone()
    assert row is not None
    assert row[0] == "Bancolombia"
    assert row[1] == "CurrentAccount"
    assert row[2] == 5000000


def test_upsert_accounts_updates_existing(loader):
    """Upsert actualiza un registro que ya existe — no duplica."""
    loader.create_schema()
    account = {
        "id": "abc-123",
        "name": "Bancolombia",
        "archived": False,
        "color": "#FFB300",
        "accountType": "CurrentAccount",
        "initialBalance": {"value": 5000000, "currencyCode": "COP"},
        "excludeFromStats": False,
        "recordStats": {"recordCount": 100},
        "createdAt": "2025-03-10T04:29:18Z",
        "updatedAt": "2026-04-27T04:13:42Z",
    }
    loader.upsert_accounts([account])

    # Modificar nombre y volver a insertar
    account["name"] = "Bancolombia Actualizado"
    loader.upsert_accounts([account])

    rows = loader._conn.execute(
        "SELECT name FROM raw_wallet_accounts WHERE id = 'abc-123'"
    ).fetchall()
    assert len(rows) == 1  # no duplicó
    assert rows[0][0] == "Bancolombia Actualizado"


def test_upsert_categories_inserts_new(loader):
    """Inserta categorías correctamente."""
    loader.create_schema()
    categories = [
        {
            "id": "cat-001",
            "name": "Comida",
            "color": "#FF0000",
            "customCategory": True,
            "envelopeId": 1001,
            "createdAt": "2025-01-01T00:00:00Z",
            "updatedAt": "2025-01-01T00:00:00Z",
        }
    ]
    loader.upsert_categories(categories)

    row = loader._conn.execute(
        "SELECT name, envelope_id FROM raw_wallet_categories WHERE id = 'cat-001'"
    ).fetchone()
    assert row is not None
    assert row[0] == "Comida"
    assert row[1] == 1001


def test_upsert_records_inserts_new(loader):
    """Inserta transacciones correctamente."""
    loader.create_schema()
    records = [
        {
            "id": "rec-001",
            "accountId": "acc-001",
            "categoryId": "cat-001",
            "amount": {"value": -50000, "currencyCode": "COP"},
            "type": "expense",
            "note": "Almuerzo",
            "recordDate": "2026-04-01T12:00:00Z",
            "createdAt": "2026-04-01T12:00:00Z",
            "updatedAt": "2026-04-01T12:00:00Z",
        }
    ]
    loader.upsert_records(records)

    row = loader._conn.execute(
        "SELECT account_id, amount_value, record_type FROM raw_wallet_records WHERE id = 'rec-001'"
    ).fetchone()
    assert row is not None
    assert row[0] == "acc-001"
    assert row[1] == -50000
    assert row[2] == "expense"


def test_flatten_nested_balance(loader):
    """_flatten aplana initialBalance correctamente."""
    record = {
        "id": "x",
        "initialBalance": {"value": 1000, "currencyCode": "COP"},
    }
    flat = loader._flatten_account(record)
    assert flat["initial_balance_value"] == 1000
    assert flat["initial_balance_currency"] == "COP"
    assert "initialBalance" not in flat
```

- [ ] **Step 2: Correr tests para verificar que fallan**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m pytest tests/test_sqlite_loader.py -v
```
Expected: `ModuleNotFoundError: No module named 'src.load.sqlite_loader'`

- [ ] **Step 3: Implementar sqlite_loader.py**

Crear `src/load/sqlite_loader.py`:

```python
"""
Carga datos de Wallet API en SQLite usando upsert idempotente por id.

Bronze layer: raw_wallet_accounts, raw_wallet_categories, raw_wallet_records
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any


class SqliteLoader:
    """Carga registros de Wallet API en SQLite con upsert por id."""

    def __init__(self, db_path: str = "finance.db") -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row

    def create_schema(self) -> None:
        """Crea tablas raw si no existen. Idempotente."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS raw_wallet_accounts (
                id                      TEXT PRIMARY KEY,
                name                    TEXT,
                account_type            TEXT,
                archived                INTEGER,
                color                   TEXT,
                initial_balance_value   INTEGER,
                initial_balance_currency TEXT,
                exclude_from_stats      INTEGER,
                record_count            INTEGER,
                created_at              TEXT,
                updated_at              TEXT,
                _loaded_at              TEXT
            );

            CREATE TABLE IF NOT EXISTS raw_wallet_categories (
                id               TEXT PRIMARY KEY,
                name             TEXT,
                color            TEXT,
                custom_category  INTEGER,
                envelope_id      INTEGER,
                created_at       TEXT,
                updated_at       TEXT,
                _loaded_at       TEXT
            );

            CREATE TABLE IF NOT EXISTS raw_wallet_records (
                id               TEXT PRIMARY KEY,
                account_id       TEXT,
                category_id      TEXT,
                amount_value     INTEGER,
                amount_currency  TEXT,
                record_type      TEXT,
                note             TEXT,
                record_date      TEXT,
                created_at       TEXT,
                updated_at       TEXT,
                _loaded_at       TEXT
            );
        """)
        self._conn.commit()

    def upsert_accounts(self, records: list[dict[str, Any]]) -> int:
        """Inserta o actualiza cuentas. Retorna cantidad procesada."""
        loaded_at = datetime.now(timezone.utc).isoformat()
        rows = [self._flatten_account(r) | {"_loaded_at": loaded_at} for r in records]
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO raw_wallet_accounts
                (id, name, account_type, archived, color,
                 initial_balance_value, initial_balance_currency,
                 exclude_from_stats, record_count,
                 created_at, updated_at, _loaded_at)
            VALUES
                (:id, :name, :account_type, :archived, :color,
                 :initial_balance_value, :initial_balance_currency,
                 :exclude_from_stats, :record_count,
                 :created_at, :updated_at, :_loaded_at)
            """,
            rows,
        )
        self._conn.commit()
        return len(rows)

    def upsert_categories(self, records: list[dict[str, Any]]) -> int:
        """Inserta o actualiza categorías. Retorna cantidad procesada."""
        loaded_at = datetime.now(timezone.utc).isoformat()
        rows = [self._flatten_category(r) | {"_loaded_at": loaded_at} for r in records]
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO raw_wallet_categories
                (id, name, color, custom_category, envelope_id,
                 created_at, updated_at, _loaded_at)
            VALUES
                (:id, :name, :color, :custom_category, :envelope_id,
                 :created_at, :updated_at, :_loaded_at)
            """,
            rows,
        )
        self._conn.commit()
        return len(rows)

    def upsert_records(self, records: list[dict[str, Any]]) -> int:
        """Inserta o actualiza transacciones. Retorna cantidad procesada."""
        loaded_at = datetime.now(timezone.utc).isoformat()
        rows = [self._flatten_record(r) | {"_loaded_at": loaded_at} for r in records]
        self._conn.executemany(
            """
            INSERT OR REPLACE INTO raw_wallet_records
                (id, account_id, category_id, amount_value, amount_currency,
                 record_type, note, record_date,
                 created_at, updated_at, _loaded_at)
            VALUES
                (:id, :account_id, :category_id, :amount_value, :amount_currency,
                 :record_type, :note, :record_date,
                 :created_at, :updated_at, :_loaded_at)
            """,
            rows,
        )
        self._conn.commit()
        return len(rows)

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Helpers de aplanado (JSON anidado → dict plano para SQLite)
    # ------------------------------------------------------------------

    def _flatten_account(self, r: dict[str, Any]) -> dict[str, Any]:
        balance = r.get("initialBalance") or {}
        stats = r.get("recordStats") or {}
        return {
            "id": r["id"],
            "name": r.get("name"),
            "account_type": r.get("accountType"),
            "archived": int(r.get("archived", False)),
            "color": r.get("color"),
            "initial_balance_value": balance.get("value"),
            "initial_balance_currency": balance.get("currencyCode"),
            "exclude_from_stats": int(r.get("excludeFromStats", False)),
            "record_count": stats.get("recordCount"),
            "created_at": r.get("createdAt"),
            "updated_at": r.get("updatedAt"),
        }

    def _flatten_category(self, r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": r["id"],
            "name": r.get("name"),
            "color": r.get("color"),
            "custom_category": int(r.get("customCategory", False)),
            "envelope_id": r.get("envelopeId"),
            "created_at": r.get("createdAt"),
            "updated_at": r.get("updatedAt"),
        }

    def _flatten_record(self, r: dict[str, Any]) -> dict[str, Any]:
        amount = r.get("amount") or {}
        return {
            "id": r["id"],
            "account_id": r.get("accountId"),
            "category_id": r.get("categoryId"),
            "amount_value": amount.get("value"),
            "amount_currency": amount.get("currencyCode"),
            "record_type": r.get("type"),
            "note": r.get("note"),
            "record_date": r.get("recordDate"),
            "created_at": r.get("createdAt"),
            "updated_at": r.get("updatedAt"),
        }
```

- [ ] **Step 4: Actualizar src/load/__init__.py**

```python
from src.load.sqlite_loader import SqliteLoader

__all__ = ["SqliteLoader"]
```

- [ ] **Step 5: Correr tests y verificar que pasan**

```bash
.venv/Scripts/python -m pytest tests/test_sqlite_loader.py -v
```
Expected: 6 passed

- [ ] **Step 6: Correr todos los tests para verificar no hay regresiones**

```bash
.venv/Scripts/python -m pytest -v
```
Expected: 12 passed (6 anteriores + 6 nuevos)

- [ ] **Step 7: Commit**

```bash
git add src/load/sqlite_loader.py src/load/__init__.py tests/test_sqlite_loader.py
git commit -m "feat: implement SqliteLoader with idempotent upsert"
```

---

## Task 4: Implementar load_records.py (orquestador CLI)

**Files:**
- Create: `src/load/load_records.py`

- [ ] **Step 1: Crear el orquestador**

Crear `src/load/load_records.py`:

```python
"""
CLI que lee JSONs de data/raw/ y los carga en SQLite.

Uso:
    python -m src.load.load_records
    python -m src.load.load_records --db finance.db
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from src.load.sqlite_loader import SqliteLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("load")

DATA_RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def load_latest_json(prefix: str) -> list[dict]:
    """Lee el JSON más reciente con el prefijo dado de data/raw/."""
    files = sorted(DATA_RAW_DIR.glob(f"{prefix}_*.json"), reverse=True)
    if not files:
        logger.warning(f"No se encontró ningún archivo {prefix}_*.json en {DATA_RAW_DIR}")
        return []
    path = files[0]
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        logger.warning(f"{path.name} no contiene una lista — skipping")
        return []
    logger.info(f"📂 Leyendo {path.name} ({len(data)} registros)")
    return data


def main() -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Load raw JSON data into SQLite")
    parser.add_argument(
        "--db",
        default=os.getenv("FINANCE_DB_PATH", "finance.db"),
        help="Ruta al archivo SQLite (default: finance.db)",
    )
    args = parser.parse_args()

    loader = SqliteLoader(db_path=args.db)

    logger.info("🏗️  Creando schema si no existe...")
    loader.create_schema()

    accounts = load_latest_json("accounts")
    if accounts:
        n = loader.upsert_accounts(accounts)
        logger.info(f"✅ {n} cuentas cargadas → {args.db}")

    categories = load_latest_json("categories")
    if categories:
        n = loader.upsert_categories(categories)
        logger.info(f"✅ {n} categorías cargadas → {args.db}")

    records = load_latest_json("records")
    if records:
        n = loader.upsert_records(records)
        logger.info(f"✅ {n} transacciones cargadas → {args.db}")

    loader.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Correr el loader contra los datos reales**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m src.load.load_records
```
Expected output:
```
2026-04-30 ... | INFO    | load | 🏗️  Creando schema si no existe...
2026-04-30 ... | INFO    | load | 📂 Leyendo accounts_20260427_003152.json (6 registros)
2026-04-30 ... | INFO    | load | ✅ 6 cuentas cargadas → finance.db
2026-04-30 ... | INFO    | load | 📂 Leyendo categories_20260427_003152.json (64 registros)
2026-04-30 ... | INFO    | load | ✅ 64 categorías cargadas → finance.db
```

- [ ] **Step 3: Verificar que finance.db fue creado y tiene datos**

```bash
.venv/Scripts/python -c "
import sqlite3
conn = sqlite3.connect('finance.db')
for table in ['raw_wallet_accounts', 'raw_wallet_categories', 'raw_wallet_records']:
    count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    print(f'{table}: {count} rows')
"
```
Expected:
```
raw_wallet_accounts: 6 rows
raw_wallet_categories: 64 rows
raw_wallet_records: 0 rows  (hasta que se fixee el bug de extract)
```

- [ ] **Step 4: Commit**

```bash
git add src/load/load_records.py
git commit -m "feat: add load_records CLI orchestrator"
```

---

## Task 5: Setup dbt project

**Files:**
- Create: `dbt/profiles.yml`
- Create: `dbt/dbt_project.yml`
- Create: `dbt/models/staging/.gitkeep`
- Create: `dbt/models/marts/.gitkeep`

- [ ] **Step 1: Crear dbt_project.yml**

Crear `dbt/dbt_project.yml`:

```yaml
name: personal_finance
version: "1.0.0"
config-version: 2

profile: personal_finance

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  personal_finance:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

- [ ] **Step 2: Crear profiles.yml**

Crear `dbt/profiles.yml`:

```yaml
personal_finance:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('FINANCE_DB_PATH', '../finance.db') }}"
      threads: 1
```

> Nota: dbt-duckdb puede leer archivos SQLite directamente con el path. El `../finance.db` es relativo al directorio `dbt/`.

- [ ] **Step 3: Crear estructura de directorios**

```bash
mkdir -p D:/Projects/personal-finance-pipeline/dbt/models/staging
mkdir -p D:/Projects/personal-finance-pipeline/dbt/models/marts
```

- [ ] **Step 4: Verificar que dbt puede conectarse**

```bash
cd D:/Projects/personal-finance-pipeline/dbt
../. venv/Scripts/dbt debug --profiles-dir .
```
Expected: `All checks passed!`

- [ ] **Step 5: Commit**

```bash
cd D:/Projects/personal-finance-pipeline
git add dbt/dbt_project.yml dbt/profiles.yml dbt/models/
git commit -m "chore: initialize dbt project structure"
```

---

## Task 6: Modelos dbt staging (Silver)

**Files:**
- Create: `dbt/models/staging/stg_wallet__accounts.sql`
- Create: `dbt/models/staging/stg_wallet__categories.sql`
- Create: `dbt/models/staging/stg_wallet__records.sql`
- Create: `dbt/models/staging/schema.yml`

- [ ] **Step 1: Crear stg_wallet__accounts.sql**

Crear `dbt/models/staging/stg_wallet__accounts.sql`:

```sql
with source as (
    select * from {{ source('raw', 'wallet_accounts') }}
),

renamed as (
    select
        id                                          as account_id,
        name                                        as account_name,
        account_type,
        cast(archived as boolean)                   as is_archived,
        color,
        initial_balance_value                       as initial_balance_cop,
        initial_balance_currency                    as currency_code,
        cast(exclude_from_stats as boolean)         as exclude_from_stats,
        record_count,
        cast(created_at as timestamp)               as created_at,
        cast(updated_at as timestamp)               as updated_at,
        cast(_loaded_at as timestamp)               as loaded_at
    from source
)

select * from renamed
where not is_archived
```

- [ ] **Step 2: Crear stg_wallet__categories.sql**

Crear `dbt/models/staging/stg_wallet__categories.sql`:

```sql
with source as (
    select * from {{ source('raw', 'wallet_categories') }}
),

renamed as (
    select
        id                              as category_id,
        name                            as category_name,
        color,
        cast(custom_category as boolean) as is_custom,
        envelope_id,
        cast(created_at as timestamp)   as created_at,
        cast(updated_at as timestamp)   as updated_at,
        cast(_loaded_at as timestamp)   as loaded_at
    from source
)

select * from renamed
```

- [ ] **Step 3: Crear stg_wallet__records.sql**

Crear `dbt/models/staging/stg_wallet__records.sql`:

```sql
with source as (
    select * from {{ source('raw', 'wallet_records') }}
),

renamed as (
    select
        id                              as record_id,
        account_id,
        category_id,
        amount_value                    as amount_cop,
        amount_currency                 as currency_code,
        record_type,
        note,
        cast(record_date as timestamp)  as record_date,
        cast(created_at as timestamp)   as created_at,
        cast(updated_at as timestamp)   as updated_at,
        cast(_loaded_at as timestamp)   as loaded_at
    from source
)

select * from renamed
```

- [ ] **Step 4: Crear schema.yml con sources y tests**

Crear `dbt/models/staging/schema.yml`:

```yaml
version: 2

sources:
  - name: raw
    schema: main
    tables:
      - name: wallet_accounts
        identifier: raw_wallet_accounts
      - name: wallet_categories
        identifier: raw_wallet_categories
      - name: wallet_records
        identifier: raw_wallet_records

models:
  - name: stg_wallet__accounts
    description: "Cuentas de Wallet, limpias y tipadas. Excluye archivadas."
    columns:
      - name: account_id
        tests:
          - not_null
          - unique
      - name: account_name
        tests:
          - not_null
      - name: account_type
        tests:
          - not_null
          - accepted_values:
              values: ['CurrentAccount', 'SavingAccount', 'Cash', 'CreditCard', 'General']

  - name: stg_wallet__categories
    description: "Categorías de Wallet, limpias y tipadas."
    columns:
      - name: category_id
        tests:
          - not_null
          - unique
      - name: category_name
        tests:
          - not_null

  - name: stg_wallet__records
    description: "Transacciones de Wallet, limpias y tipadas."
    columns:
      - name: record_id
        tests:
          - not_null
          - unique
      - name: account_id
        tests:
          - not_null
      - name: record_type
        tests:
          - accepted_values:
              values: ['expense', 'income', 'transfer']
```

- [ ] **Step 5: Correr dbt run para staging**

```bash
cd D:/Projects/personal-finance-pipeline/dbt
../. venv/Scripts/dbt run --profiles-dir . --select staging
```
Expected: `3 of 3 OK`

- [ ] **Step 6: Correr dbt test para staging**

```bash
../. venv/Scripts/dbt test --profiles-dir . --select staging
```
Expected: todos los tests pasan (si raw_wallet_records está vacío, los tests de records son trivialmente OK)

- [ ] **Step 7: Commit**

```bash
cd D:/Projects/personal-finance-pipeline
git add dbt/models/staging/
git commit -m "feat: add dbt staging models for accounts, categories, records"
```

---

## Task 7: Modelos dbt marts (Gold)

**Files:**
- Create: `dbt/models/marts/dim_accounts.sql`
- Create: `dbt/models/marts/dim_categories.sql`
- Create: `dbt/models/marts/fact_transactions.sql`
- Create: `dbt/models/marts/gastos_por_categoria_mensual.sql`
- Create: `dbt/models/marts/schema.yml`

- [ ] **Step 1: Crear dim_accounts.sql**

Crear `dbt/models/marts/dim_accounts.sql`:

```sql
with accounts as (
    select * from {{ ref('stg_wallet__accounts') }}
)

select
    account_id,
    account_name,
    account_type,
    currency_code,
    initial_balance_cop,
    exclude_from_stats,
    record_count,
    created_at,
    updated_at
from accounts
```

- [ ] **Step 2: Crear dim_categories.sql**

Crear `dbt/models/marts/dim_categories.sql`:

```sql
with categories as (
    select * from {{ ref('stg_wallet__categories') }}
)

select
    category_id,
    category_name,
    color,
    is_custom,
    envelope_id,
    created_at,
    updated_at
from categories
```

- [ ] **Step 3: Crear fact_transactions.sql**

Crear `dbt/models/marts/fact_transactions.sql`:

```sql
with records as (
    select * from {{ ref('stg_wallet__records') }}
),

accounts as (
    select account_id from {{ ref('dim_accounts') }}
),

categories as (
    select category_id from {{ ref('dim_categories') }}
)

select
    r.record_id,
    r.account_id,
    r.category_id,
    r.amount_cop,
    r.currency_code,
    r.record_type,
    r.note,
    r.record_date,
    date_trunc('month', r.record_date)  as month,
    year(r.record_date)                 as year,
    month(r.record_date)                as month_num,
    r.created_at
from records r
left join accounts a using (account_id)
left join categories c using (category_id)
```

- [ ] **Step 4: Crear gastos_por_categoria_mensual.sql**

Crear `dbt/models/marts/gastos_por_categoria_mensual.sql`:

```sql
with fact as (
    select * from {{ ref('fact_transactions') }}
),

categories as (
    select category_id, category_name from {{ ref('dim_categories') }}
)

select
    f.month,
    f.year,
    f.month_num,
    c.category_name,
    count(*)                        as num_transacciones,
    sum(f.amount_cop)               as total_cop,
    avg(f.amount_cop)               as promedio_cop
from fact f
left join categories c using (category_id)
where f.record_type = 'expense'
group by 1, 2, 3, 4
order by 1 desc, total_cop asc
```

- [ ] **Step 5: Crear schema.yml para marts**

Crear `dbt/models/marts/schema.yml`:

```yaml
version: 2

models:
  - name: dim_accounts
    description: "Dimensión de cuentas activas (no archivadas)."
    columns:
      - name: account_id
        tests:
          - not_null
          - unique

  - name: dim_categories
    description: "Dimensión de categorías de Wallet."
    columns:
      - name: category_id
        tests:
          - not_null
          - unique

  - name: fact_transactions
    description: "Tabla de hechos con todas las transacciones."
    columns:
      - name: record_id
        tests:
          - not_null
          - unique
      - name: account_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_accounts')
              field: account_id

  - name: gastos_por_categoria_mensual
    description: "Agregado mensual de gastos por categoría."
```

- [ ] **Step 6: Correr dbt run completo**

```bash
cd D:/Projects/personal-finance-pipeline/dbt
../. venv/Scripts/dbt run --profiles-dir .
```
Expected: `7 of 7 OK` (3 staging + 4 marts)

- [ ] **Step 7: Correr dbt test completo**

```bash
../. venv/Scripts/dbt test --profiles-dir .
```
Expected: todos los tests pasan

- [ ] **Step 8: Commit**

```bash
cd D:/Projects/personal-finance-pipeline
git add dbt/models/marts/
git commit -m "feat: add dbt mart models — dims, fact_transactions, gastos_por_categoria_mensual"
```

---

## Task 8: Actualizar .gitignore y documentación final

**Files:**
- Modify: `.gitignore`
- Modify: `README.md`

- [ ] **Step 1: Agregar finance.db y artefactos dbt al .gitignore**

En `.gitignore`, agregar:

```gitignore
# Data warehouse local
finance.db

# dbt
dbt/target/
dbt/dbt_packages/
dbt/logs/
```

- [ ] **Step 2: Verificar que tests completos pasan**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m pytest -v
```
Expected: 12 passed

- [ ] **Step 3: Commit final**

```bash
git add .gitignore README.md
git commit -m "chore: update gitignore for SQLite and dbt artifacts"
```

---

## Resumen de comandos de verificación final

```bash
# 1. Todos los tests Python pasan
.venv/Scripts/python -m pytest -v

# 2. Extract funciona (records ahora sí trae transacciones)
.venv/Scripts/python -m src.extract.extract_records --days 90

# 3. Load funciona
.venv/Scripts/python -m src.load.load_records

# 4. dbt completo
cd dbt && ../. venv/Scripts/dbt run --profiles-dir . && ../. venv/Scripts/dbt test --profiles-dir .
```
