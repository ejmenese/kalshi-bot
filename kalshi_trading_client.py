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
        # Ambiente normalizado: solo "prod" o "demo". El bot lo usa para
        # etiquetar cada trade y no mezclar historiales.
        self.env = "prod" if env == "prod" else "demo"
        self.base_url = PROD_BASE_URL if self.env == "prod" else DEMO_BASE_URL

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
        time_in_force: str = "immediate_or_cancel",
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        """Coloca una orden limite usando el endpoint V2 de Kalshi
        (POST /portfolio/events/orders). El endpoint viejo /portfolio/orders
        ahora devuelve 410 Gone.

        V2 cotiza todo desde el lado YES de un solo libro:
          comprar YES a p  -> bid a p
          vender YES a p   -> ask a p
          comprar NO a p   -> ask a (1 - p)
          vender NO a p    -> bid a (1 - p)
        """
        side = side.strip().lower()
        action = action.strip().lower()
        if side not in ("yes", "no") or action not in ("buy", "sell"):
            raise ValueError(f"side/action invalidos: {side}/{action}")
        if not 1 <= int(price_cents) <= 99:
            raise ValueError(f"precio fuera de rango: {price_cents}")
        if time_in_force not in ("immediate_or_cancel", "fill_or_kill", "good_till_canceled"):
            raise ValueError(f"time_in_force invalido: {time_in_force}")
        if reduce_only and time_in_force != "immediate_or_cancel":
            # Kalshi rechaza reduce_only con cualquier otro time_in_force.
            raise ValueError("reduce_only exige time_in_force=immediate_or_cancel")

        if side == "yes":
            book_side = "bid" if action == "buy" else "ask"
            yes_price_cents = int(price_cents)
        else:
            book_side = "ask" if action == "buy" else "bid"
            yes_price_cents = 100 - int(price_cents)

        path = "/portfolio/events/orders"
        body = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4()),
            "side": book_side,
            "count": f"{int(count)}.00",
            "price": f"{yes_price_cents / 100:.4f}",
            # Por defecto IOC: lo que no se llena al instante se cancela, asi
            # no quedan ordenes viejas en el libro y la respuesta dice cuanto
            # se lleno de verdad (fill_count).
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
        }
        if reduce_only:
            body["reduce_only"] = True
        resp = self.session.post(
            self.base_url + path,
            headers={**self._headers("POST", "/trade-api/v2" + path), "Content-Type": "application/json"},
            json=body,
            timeout=10,
        )
        if not resp.ok:
            # Incluye el detalle que devuelve Kalshi para que se vea en los logs
            raise requests.HTTPError(
                f"{resp.status_code} {resp.reason}: {resp.text[:300]}", response=resp
            )
        return resp.json()


def parse_fill(resp: dict[str, Any]) -> tuple[int, int | None]:
    """Lee la respuesta de create_order (V2): contratos llenados al instante y
    precio promedio en centavos (None si no hubo fills). Acepta que la
    respuesta venga envuelta en {"order": {...}}."""
    data = resp.get("order", resp) if isinstance(resp, dict) else {}
    try:
        filled = int(round(float(data.get("fill_count") or 0)))
    except (TypeError, ValueError):
        filled = 0
    avg_raw = data.get("average_fill_price")
    try:
        avg_cents = round(float(avg_raw) * 100) if avg_raw not in (None, "") and filled > 0 else None
    except (TypeError, ValueError):
        avg_cents = None
    return filled, avg_cents
      
