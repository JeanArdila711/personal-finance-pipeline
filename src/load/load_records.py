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
    # Filtrar items que no sean dicts (e.g. archivos corruptos de extracts anteriores)
    clean = [item for item in data if isinstance(item, dict)]
    if len(clean) < len(data):
        logger.warning(f"{path.name}: {len(data) - len(clean)} items no-dict ignorados")
    logger.info(f"📂 Leyendo {path.name} ({len(clean)} registros)")
    return clean


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
