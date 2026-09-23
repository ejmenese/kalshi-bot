"""Cliente autenticado para el Trade API de Kalshi (lee balance, posiciones,
y coloca ordenes). Usa firma RSA-PSS por request, segun
docs.kalshi.com/getting_started/quick_start_authenticated_requests.

Las credenciales SIEMPRE vienen de variables de entorno -- nunca hardcodeadas
ni comiteadas al repo:
  KALSHI_API_KEY_ID       -> el UUID de la API key
  KALSHI_PRIVATE_KEY_PEM  -> el contenido completo del private key (PEM)
  KALSHI_ENV              -> "demo" (default aqui) o "prod"
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Any

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

DEMO_BASE_URL = "https://external-api.demo.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


class KalshiTradingClient:
    def __init__(self):
        env = os.environ.get("KALSHI_ENV", "demo").strip().lower()
        self.base_url = PROD_BASE_URL if env == "prod" else DEMO_BASE_URL

        # Credenciales separadas por ambiente -- asi cambiar KALSHI_ENV a
        # "prod" no deja de funcionar por accidente con llaves de demo
        # mezcladas, y viceversa.
        if env == "prod":
            api_key_id = os.environ.get("KALSHI_PROD_API_KEY_ID")
            private_key_pem = os.environ.get("KALSHI_PROD_PRIVATE_KEY_PEM")
            missing = "KALSHI_PROD_API_KEY_ID y/o KALSHI_PROD_PRIVATE_KEY_PEM"
        else:
            api_key_id = os.environ.get("KALSHI_API_KEY_ID")
            private_key_pem = os.environ.get("KALSHI_PRIVATE_KEY_PEM")
            missing = "KALSHI_API_KEY_ID y/o KALSHI_PRIVATE_KEY_PEM"

        if not api_key_id or not private_key_pem:
            raise RuntimeError(f"faltan {missing} en el entorno (ambiente actual: {env})")

        self.api_key_id = api_key_id
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None
        )
        self.session = requests.Session()

    def _headers(self, method: str, path: str) -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method}{path}".encode()
        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self.session.get(
            self.base_url + path, headers=self._headers("GET", "/trade-api/v2" + path.split("?")[0]),
            params=params, timeout=10,
        )
        resp.raise_for_status()
        return resp.json()

    def get_balance(self) -> dict[str, Any]:
        return self._get("/portfolio/balance")

    def get_positions(self) -> dict[str, Any]:
        return self._get("/portfolio/positions")

    def create_order(
        self,
        ticker: str,
        side: str,          # "yes" o "no"
        action: str,        # "buy" o "sell"
        count: int,
        price_cents: int,   # precio limite en centavos (1-99)
    ) -> dict[str, Any]:
        path = "/portfolio/orders"
        body = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4()),
            "side": side,
            "action": action,
            "type": "limit",
            "count": count,
            f"{side}_price": price_cents,
        }
        resp = self.session.post(
            self.base_url + path,
            headers={**self._headers("POST", "/trade-api/v2" + path), "Content-Type": "application/json"},
            json=body,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
