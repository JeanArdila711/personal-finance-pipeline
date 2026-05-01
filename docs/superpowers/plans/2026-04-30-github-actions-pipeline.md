# GitHub Actions cron + Pipeline Script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar `src/pipeline.py` como orquestador unificado del pipeline completo (extract → load → dbt) y un workflow de GitHub Actions que lo corre automáticamente cada lunes.

**Architecture:** `pipeline.py` llama `extract_records.main()` y `load_records.main()` directamente, luego corre `dbt run` y `dbt test` vía subprocess. GitHub Actions usa cron semanal (lunes 12pm UTC) más `workflow_dispatch` para corridas manuales. El pipeline reconstruye `finance.db` desde cero cada corrida usando `--days 365` y upsert idempotente.

**Tech Stack:** Python 3.12, subprocess, pytest + unittest.mock, GitHub Actions (ubuntu-latest), dbt-duckdb

---

## File Map

| Archivo | Acción | Responsabilidad |
|---------|--------|-----------------|
| `src/pipeline.py` | Crear | Orquestador: extract → load → dbt run → dbt test |
| `tests/test_pipeline.py` | Crear | Tests con mocks para cada paso del pipeline |
| `.github/workflows/pipeline.yml` | Crear | Cron semanal + workflow_dispatch |

---

## Task 1: Implementar src/pipeline.py (TDD)

**Files:**
- Create: `src/pipeline.py`
- Create: `tests/test_pipeline.py`

- [ ] **Step 1: Escribir los tests que fallan primero**

Crear `tests/test_pipeline.py`:

```python
"""Tests para pipeline.py. Mockean extract, load y dbt pa' no llamar APIs reales."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline import main, run_dbt


def test_pipeline_calls_extract_then_load_then_dbt(tmp_path):
    """El pipeline corre extract → load → dbt en ese orden."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 0
        mock_load.return_value = 0
        mock_dbt.return_value = 0

        result = main(["--days", "7", "--db", str(tmp_path / "test.db")])

        assert result == 0
        mock_extract.assert_called_once()
        mock_load.assert_called_once()
        mock_dbt.assert_called_once()

        # Verificar orden: extract antes que load, load antes que dbt
        assert mock_extract.call_count == 1
        assert mock_load.call_count == 1


def test_pipeline_stops_if_extract_fails(tmp_path):
    """Si extract falla, load y dbt NO se corren."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 1  # falla

        result = main(["--days", "7", "--db", str(tmp_path / "test.db")])

        assert result == 1
        mock_load.assert_not_called()
        mock_dbt.assert_not_called()


def test_pipeline_stops_if_load_fails(tmp_path):
    """Si load falla, dbt NO se corre."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 0
        mock_load.return_value = 1  # falla

        result = main(["--days", "7", "--db", str(tmp_path / "test.db")])

        assert result == 1
        mock_dbt.assert_not_called()


def test_pipeline_skip_dbt_flag(tmp_path):
    """--skip-dbt hace que dbt NO se corra."""
    with (
        patch("src.pipeline.extract_main") as mock_extract,
        patch("src.pipeline.load_main") as mock_load,
        patch("src.pipeline.run_dbt") as mock_dbt,
    ):
        mock_extract.return_value = 0
        mock_load.return_value = 0

        result = main(["--days", "7", "--db", str(tmp_path / "test.db"), "--skip-dbt"])

        assert result == 0
        mock_dbt.assert_not_called()


def test_run_dbt_returns_nonzero_on_failure():
    """run_dbt retorna exit code no-cero si dbt falla."""
    with patch("src.pipeline.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1)

        result = run_dbt(dbt_dir="dbt", profiles_dir="dbt")

        assert result != 0


def test_run_dbt_returns_zero_on_success():
    """run_dbt retorna 0 si dbt run y dbt test pasan."""
    with patch("src.pipeline.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        result = run_dbt(dbt_dir="dbt", profiles_dir="dbt")

        assert result == 0
```

- [ ] **Step 2: Correr tests para verificar que fallan**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m pytest tests/test_pipeline.py -v
```
Expected: `ModuleNotFoundError: No module named 'src.pipeline'`

- [ ] **Step 3: Implementar src/pipeline.py**

Crear `src/pipeline.py`:

```python
"""
Orquestador del pipeline completo: extract → load → dbt run → dbt test.

Uso:
    python -m src.pipeline                          # últimos 7 días
    python -m src.pipeline --days 365               # último año (para CI)
    python -m src.pipeline --skip-dbt               # solo extract + load
    python -m src.pipeline --db /ruta/finance.db    # DB custom
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from src.extract.extract_records import main as extract_main
from src.load.load_records import main as load_main

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


def run_dbt(dbt_dir: str | Path = DBT_DIR, profiles_dir: str | Path = DBT_DIR) -> int:
    """Corre dbt run y dbt test. Retorna 0 si todo pasa, 1 si algo falla."""
    dbt_bin = PROJECT_ROOT / ".venv" / "Scripts" / "dbt"
    if not dbt_bin.exists():
        dbt_bin = PROJECT_ROOT / ".venv" / "bin" / "dbt"

    for cmd in ["run", "test"]:
        logger.info(f"🔄 Corriendo dbt {cmd}...")
        result = subprocess.run(
            [str(dbt_bin), cmd, "--profiles-dir", str(profiles_dir)],
            cwd=str(dbt_dir),
        )
        if result.returncode != 0:
            logger.error(f"❌ dbt {cmd} falló con exit code {result.returncode}")
            return result.returncode
        logger.info(f"✅ dbt {cmd} completado")

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the full finance pipeline")
    parser.add_argument("--days", type=int, default=7, help="Días hacia atrás a extraer")
    parser.add_argument(
        "--db",
        default=os.getenv("FINANCE_DB_PATH", str(PROJECT_ROOT / "finance.db")),
        help="Ruta al archivo SQLite",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Saltarse dbt run y dbt test",
    )
    args = parser.parse_args(argv)

    # Inyectar --db en sys.argv para que load_records lo tome
    os.environ["FINANCE_DB_PATH"] = args.db

    # 1. Extract
    logger.info(f"🚀 Iniciando pipeline (--days={args.days})")
    logger.info("📥 Paso 1/3: Extract")
    sys.argv = ["extract_records", "--days", str(args.days)]
    code = extract_main()
    if code != 0:
        logger.error("💥 Extract falló — abortando pipeline")
        return 1

    # 2. Load
    logger.info("📦 Paso 2/3: Load")
    sys.argv = ["load_records", "--db", args.db]
    code = load_main()
    if code != 0:
        logger.error("💥 Load falló — abortando pipeline")
        return 1

    # 3. dbt
    if args.skip_dbt:
        logger.info("⏭️  dbt skipped (--skip-dbt)")
    else:
        logger.info("🔄 Paso 3/3: dbt run + test")
        code = run_dbt()
        if code != 0:
            logger.error("💥 dbt falló — pipeline incompleto")
            return 1

    logger.info("🎉 Pipeline completado exitosamente")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Correr tests y verificar que pasan**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m pytest tests/test_pipeline.py -v
```
Expected: `6 passed`

- [ ] **Step 5: Correr todos los tests para verificar no hay regresiones**

```bash
.venv/Scripts/python -m pytest -v
```
Expected: `18 passed`

- [ ] **Step 6: Commit**

```bash
git add src/pipeline.py tests/test_pipeline.py
git commit -m "feat: add pipeline orchestrator with extract, load, dbt steps"
```

---

## Task 2: Crear GitHub Actions workflow

**Files:**
- Create: `.github/workflows/pipeline.yml`

- [ ] **Step 1: Crear el directorio y el workflow**

Crear `.github/workflows/pipeline.yml`:

```yaml
name: Finance Pipeline

on:
  schedule:
    - cron: '0 12 * * 1'  # lunes 7am Colombia (UTC-5 = 12pm UTC)
  workflow_dispatch:        # trigger manual desde GitHub UI

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: 'pip'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run pipeline
        run: python -m src.pipeline --days 365
        env:
          WALLET_API_KEY: ${{ secrets.WALLET_API_KEY }}
          FINANCE_DB_PATH: finance.db
```

- [ ] **Step 2: Verificar que el YAML es válido**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -c "
import yaml, pathlib
content = pathlib.Path('.github/workflows/pipeline.yml').read_text()
parsed = yaml.safe_load(content)
print('YAML válido ✅')
print('Trigger cron:', parsed['on']['schedule'][0]['cron'])
print('Job:', list(parsed['jobs'].keys())[0])
"
```
Expected:
```
YAML válido ✅
Trigger cron: 0 12 * * 1
Job: run-pipeline
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/pipeline.yml
git commit -m "feat: add GitHub Actions weekly cron workflow"
```

---

## Task 3: Configurar GitHub Secret y verificar en GitHub

**Files:** ninguno (configuración en GitHub UI)

- [ ] **Step 1: Agregar WALLET_API_KEY como GitHub Secret**

1. Ir a `https://github.com/JeanArdila711/personal-finance-pipeline/settings/secrets/actions`
2. Click "New repository secret"
3. Name: `WALLET_API_KEY`
4. Value: el valor de `WALLET_API_KEY` del archivo `.env` local
5. Click "Add secret"

- [ ] **Step 2: Push al repo**

```bash
cd D:/Projects/personal-finance-pipeline
git push origin main
```

- [ ] **Step 3: Verificar que el workflow aparece en GitHub**

1. Ir a `https://github.com/JeanArdila711/personal-finance-pipeline/actions`
2. Verificar que aparece "Finance Pipeline" en la lista de workflows

- [ ] **Step 4: Correr manualmente con workflow_dispatch**

1. En la página de Actions, click en "Finance Pipeline"
2. Click "Run workflow" → "Run workflow"
3. Esperar que termine (~2-3 min)
4. Verificar que el job pasa (check verde)

---

## Task 4: Verificar pipeline local end-to-end

**Files:** ninguno

- [ ] **Step 1: Correr pipeline completo local**

```bash
cd D:/Projects/personal-finance-pipeline
.venv/Scripts/python -m src.pipeline --days 30
```
Expected output:
```
... | INFO    | pipeline | 🚀 Iniciando pipeline (--days=30)
... | INFO    | pipeline | 📥 Paso 1/3: Extract
... | INFO    | extract  | ✅ 6 cuentas → accounts_*.json
... | INFO    | extract  | ✅ 64 categorías → categories_*.json
... | INFO    | extract  | ✅ N transacciones → records_*.json
... | INFO    | pipeline | 📦 Paso 2/3: Load
... | INFO    | load     | ✅ 6 cuentas cargadas → finance.db
... | INFO    | load     | ✅ 64 categorías cargadas → finance.db
... | INFO    | pipeline | 🔄 Paso 3/3: dbt run + test
... | INFO    | pipeline | ✅ dbt run completado
... | INFO    | pipeline | ✅ dbt test completado
... | INFO    | pipeline | 🎉 Pipeline completado exitosamente
```

- [ ] **Step 2: Verificar --skip-dbt**

```bash
.venv/Scripts/python -m src.pipeline --days 7 --skip-dbt
```
Expected: pipeline corre sin el paso de dbt, termina con `🎉 Pipeline completado exitosamente`

- [ ] **Step 3: Correr todos los tests una última vez**

```bash
.venv/Scripts/python -m pytest -v
```
Expected: `18 passed`
