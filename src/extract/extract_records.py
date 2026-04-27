"""
Script CLI que extrae data de Wallet API y la guarda como JSON crudo.

Uso:
    python -m src.extract.extract_records
    python -m src.extract.extract_records --days 30
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from src.extract.wallet_client import WalletClient

# Configuración de logging — formato bonito y útil
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("extract")


# Carpeta donde guardamos los JSON crudos
DATA_RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def save_json(data: object, filename: str) -> Path:
    """Guarda data como JSON formateado en data/raw/."""
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_RAW_DIR / filename
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract data from Wallet API")
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Cuántos días hacia atrás traer (default: 7)",
    )
    args = parser.parse_args()

    # Cargar variables de entorno desde .env
    load_dotenv()
    api_key = os.getenv("WALLET_API_KEY")
    base_url = os.getenv("WALLET_API_BASE_URL")

    if not api_key:
        logger.error("❌ WALLET_API_KEY no está en .env")
        return 1

    # Inicializar cliente
    client = WalletClient(api_key=api_key, base_url=base_url)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Ventana de fechas
    date_to = date.today() + timedelta(days=1)  # mañana, exclusivo
    date_from = date.today() - timedelta(days=args.days)
    logger.info(f"📅 Trayendo data desde {date_from} hasta {date_to}")

    try:
        # 1. Cuentas
        logger.info("⏳ Trayendo accounts...")
        accounts = client.get_accounts()
        path = save_json(accounts, f"accounts_{timestamp}.json")
        logger.info(f"✅ {len(accounts)} cuentas → {path.name}")

        # 2. Categorías
        logger.info("⏳ Trayendo categories...")
        categories = client.get_categories()
        path = save_json(categories, f"categories_{timestamp}.json")
        logger.info(f"✅ {len(categories)} categorías → {path.name}")

        # 3. Records (transacciones)
        logger.info(f"⏳ Trayendo records de los últimos {args.days} días...")
        records = client.get_records(
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )
        path = save_json(records, f"records_{timestamp}.json")
        logger.info(f"✅ {len(records)} transacciones → {path.name}")

        # 4. Stats de uso (opcional, no crítico si falla)
        try:
            usage = client.get_api_usage()
            logger.info(f"📊 API usage: {usage}")
        except Exception as e:
            logger.warning(
                f"⚠️ No se pudo obtener API usage stats (no crítico): {e}"
    )

        logger.info("🎉 Extract completado sin errores")
        return 0

    except Exception as e:
        logger.exception(f"💥 Error en extract: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())