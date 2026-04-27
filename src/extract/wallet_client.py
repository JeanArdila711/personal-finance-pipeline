"""
Cliente HTTP pa' la Wallet REST API de BudgetBakers.

Maneja:
- Autenticación con Bearer token
- Paginación automática
- Rate limiting (429) y init sync (409)
- Reintentos con backoff exponencial
- Logging estructurado
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import requests
from requests.exceptions import HTTPError, RequestException

logger = logging.getLogger(__name__)


class WalletAPIError(Exception):
    """Error genérico del cliente de Wallet API."""


class WalletAuthError(WalletAPIError):
    """Token inválido o expirado (401)."""


class WalletRateLimitError(WalletAPIError):
    """Rate limit excedido (429)."""


class WalletSyncInProgressError(WalletAPIError):
    """Sync inicial todavía corriendo (409)."""


class WalletClient:
    """Cliente pa' la Wallet REST API."""

    DEFAULT_BASE_URL = "https://rest.budgetbakers.com/wallet"
    DEFAULT_PAGE_SIZE = 100
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2  # segundos: 2, 4, 8...

    def __init__(
        self,
        api_key: str,
        base_url: str | None = None,
        timeout: int = 30,
    ) -> None:
        if not api_key:
            raise ValueError("api_key es obligatorio")

        self.base_url = (base_url or self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout

        # requests.Session reusa la conexión TCP entre requests = más rápido
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "User-Agent": "personal-finance-pipeline/0.1.0",
        })

    # ----------------------------------------------------------------------
    # Métodos públicos (los que vas a usar desde fuera)
    # ----------------------------------------------------------------------

    def get_accounts(self) -> list[dict[str, Any]]:
        """Trae todas tus cuentas (Visa Infinite, Mastercard, etc.)."""
        return list(self._paginated_get("/v1/api/accounts"))

    def get_categories(self) -> list[dict[str, Any]]:
        """Trae todas las categorías."""
        return list(self._paginated_get("/v1/api/categories"))

    def get_records(
    self,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict[str, Any]]:
        """
        Trae transacciones, opcionalmente filtradas por fecha.

        Args:
        date_from: ISO date 'YYYY-MM-DD' (inclusivo)
        date_to: ISO date 'YYYY-MM-DD' (exclusivo)
        """
        # requests acepta listas de tuples pa' parámetros repetidos
        params: list[tuple[str, Any]] = []
    
        if date_from:
            params.append(("recordDate", f"gte.{date_from}"))
        if date_to:
            params.append(("recordDate", f"lt.{date_to}"))
    
        return list(self._paginated_get("/v1/api/records", params=params))

    def get_budgets(self) -> list[dict[str, Any]]:
        """Trae todos tus presupuestos."""
        return list(self._paginated_get("/v1/api/budgets"))

    def get_api_usage(self) -> dict[str, Any]:
        """Trae stats de uso de la API (rate limit remaining, etc.)."""
        return self._request("GET", "/v1/api/api-usage/stats")

    # ----------------------------------------------------------------------
    # Métodos privados (la mecánica interna)
    # ----------------------------------------------------------------------

    def _paginated_get(
    self,
    path: str,
    params: list[tuple[str, Any]] | dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
        """
        Itera sobre todas las páginas de un endpoint paginado.

        Yields cada item individual (no la página entera).
        """
    # Normalizar a lista de tuples pa' poder agregar limit/offset
        if params is None:
            param_list: list[tuple[str, Any]] = []
        elif isinstance(params, dict):
            param_list = list(params.items())
        else:
            param_list = list(params)

        # Agregar limit si no existe
        if not any(k == "limit" for k, _ in param_list):
            param_list.append(("limit", self.DEFAULT_PAGE_SIZE))
        
        # Encontrar el limit pa' el chequeo de paginación
        limit = next((v for k, v in param_list if k == "limit"), self.DEFAULT_PAGE_SIZE)

        offset = 0

        while True:
            # Reemplazar offset (eliminar viejo, agregar nuevo)
            page_params = [(k, v) for k, v in param_list if k != "offset"]
            page_params.append(("offset", offset))

            response = self._request("GET", path, params=page_params)
            items = self._extract_items(response)

            if not items:
                break

            yield from items

            next_offset = response.get("nextOffset")
            if next_offset is None or len(items) < limit:
                break

            offset = next_offset
            logger.debug(f"Paginando {path}: offset={offset}")

    @staticmethod
    def _extract_items(response: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extrae la lista de items de la respuesta.
        Maneja distintas formas: {records: [...]}, {accounts: [...]}, etc.
        """
        # Buscar la primera key cuyo valor sea una lista
        for key, value in response.items():
            if isinstance(value, list):
                return value
        return []

    def _request(
        self,
        method: str,
        path: str,
        params: list[tuple[str, Any]] | dict[str, Any] | None = None,
) -> dict[str, Any]:
        """Hace un request HTTP con reintentos y manejo de errores."""
        url = f"{self.base_url}{path}"

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                logger.debug(f"{method} {url} (intento {attempt})")
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.timeout,
                )

                # Manejo de status codes específicos
                if response.status_code == 401:
                    raise WalletAuthError(
                        "Token inválido o expirado. Generá uno nuevo en "
                        "web.budgetbakers.com/settings/apiTokens"
                    )

                if response.status_code == 409:
                    body = response.json()
                    retry_min = body.get("retry_after_minutes", 5)
                    raise WalletSyncInProgressError(
                        f"Sync inicial en progreso. Reintentar en {retry_min} min."
                    )

                if response.status_code == 429:
                    # Rate limit: esperar y reintentar
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning(
                        f"Rate limit alcanzado, esperando {wait}s antes de reintentar"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()  # tira excepción si 4xx/5xx
                return response.json()

            except (HTTPError, RequestException) as e:
                if attempt == self.MAX_RETRIES:
                    raise WalletAPIError(
                        f"Falló después de {self.MAX_RETRIES} intentos: {e}"
                    ) from e
                wait = self.BACKOFF_FACTOR ** attempt
                logger.warning(f"Error en intento {attempt}: {e}. Reintentando en {wait}s")
                time.sleep(wait)

        raise WalletAPIError("No debería llegar acá")  # safety net