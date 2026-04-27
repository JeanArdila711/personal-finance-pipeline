"""Tests pa' WalletClient. Usan `responses` para mockear HTTP."""

from __future__ import annotations

import pytest
import responses

from src.extract.wallet_client import (
    WalletAPIError,
    WalletAuthError,
    WalletClient,
    WalletSyncInProgressError,
)

BASE_URL = "https://rest.budgetbakers.com/wallet"


@pytest.fixture
def client() -> WalletClient:
    """Cliente con API key falsa pa' tests."""
    return WalletClient(api_key="fake_token_for_tests")


def test_init_requires_api_key():
    """No se puede crear cliente sin api_key."""
    with pytest.raises(ValueError, match="api_key es obligatorio"):
        WalletClient(api_key="")


@responses.activate
def test_get_accounts_simple(client):
    """Trae cuentas sin paginación."""
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/api/accounts",
        json={"accounts": [
            {"id": "1", "name": "Visa Infinite", "balance": 1500000},
            {"id": "2", "name": "Amex Oro", "balance": 800000},
        ]},
        status=200,
    )

    accounts = client.get_accounts()

    assert len(accounts) == 2
    assert accounts[0]["name"] == "Visa Infinite"


@responses.activate
def test_get_records_with_pagination(client):
    """Maneja paginación correctamente."""
    # Primera página
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/api/records",
        json={
            "records": [{"id": str(i)} for i in range(100)],
            "nextOffset": 100,
        },
        status=200,
    )
    # Segunda página (última, menos items que limit)
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/api/records",
        json={"records": [{"id": str(i)} for i in range(100, 150)]},
        status=200,
    )

    records = client.get_records()

    assert len(records) == 150
    assert records[0]["id"] == "0"
    assert records[-1]["id"] == "149"


@responses.activate
def test_auth_error_on_401(client):
    """401 tira WalletAuthError."""
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/api/accounts",
        json={"error": "unauthorized"},
        status=401,
    )

    with pytest.raises(WalletAuthError, match="Token inválido"):
        client.get_accounts()


@responses.activate
def test_sync_in_progress_on_409(client):
    """409 tira WalletSyncInProgressError."""
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/api/accounts",
        json={
            "error": "init_sync_in_progress",
            "message": "Sync in progress",
            "retry_after_minutes": 5,
        },
        status=409,
    )

    with pytest.raises(WalletSyncInProgressError, match="5 min"):
        client.get_accounts()


@responses.activate
def test_authorization_header_is_sent(client):
    """Verifica que el Bearer token se manda en cada request."""
    responses.add(
        responses.GET,
        f"{BASE_URL}/v1/api/accounts",
        json={"accounts": []},
        status=200,
    )

    client.get_accounts()

    assert len(responses.calls) == 1
    assert responses.calls[0].request.headers["Authorization"] == "Bearer fake_token_for_tests"