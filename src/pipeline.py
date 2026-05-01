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


DBT_TIMEOUT_SECONDS = 600


def run_dbt(
    dbt_dir: str | Path = DBT_DIR,
    profiles_dir: str | Path = DBT_DIR,
    db_path: str | Path | None = None,
) -> int:
    """Corre dbt run y dbt test. Retorna 0 si todo pasa, 1 si algo falla."""
    env = os.environ.copy()
    if db_path is not None:
        env["FINANCE_DB_PATH"] = str(Path(db_path).resolve())

    for cmd in ["run", "test"]:
        logger.info(f"🔄 Corriendo dbt {cmd}...")
        result = subprocess.run(
            ["dbt", cmd, "--profiles-dir", str(profiles_dir)],
            cwd=str(dbt_dir),
            timeout=DBT_TIMEOUT_SECONDS,
            env=env,
        )
        if result.returncode != 0:
            logger.error(f"❌ dbt {cmd} falló con exit code {result.returncode}")
            return 1
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

    os.environ["FINANCE_DB_PATH"] = args.db

    saved_argv = sys.argv[:]
    try:
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
    finally:
        sys.argv = saved_argv

    # 3. dbt
    if args.skip_dbt:
        logger.info("⏭️  dbt skipped (--skip-dbt)")
    else:
        logger.info("🔄 Paso 3/3: dbt run + test")
        code = run_dbt(db_path=args.db)
        if code != 0:
            logger.error("💥 dbt falló — pipeline incompleto")
            return 1

    logger.info("🎉 Pipeline completado exitosamente")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
