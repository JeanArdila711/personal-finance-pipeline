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
