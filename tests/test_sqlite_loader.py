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
    """_flatten_account aplana initialBalance correctamente."""
    record = {
        "id": "x",
        "initialBalance": {"value": 1000, "currencyCode": "COP"},
    }
    flat = loader._flatten_account(record)
    assert flat["initial_balance_value"] == 1000
    assert flat["initial_balance_currency"] == "COP"
    assert "initialBalance" not in flat
