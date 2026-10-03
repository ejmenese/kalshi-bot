"""Cliente minimo para la API publica de mercados de Kalshi. Los datos de
mercado son publicos (docs.kalshi.com/getting_started/quick_start_market_data),
asi que este cliente no firma requests ni maneja llaves privadas.

A diferencia de un watchlist de tickers fijos, este cliente DESCUBRE los
mercados abiertos de cada serie (KXMLBGAME, KXATPMATCH, KXWTAMATCH,
KXEPLGAME, ...) en cada corrida, via GET /markets?series_ticker=X&status=open.
Los partidos de deportes en Kalshi son mercados de corta duracion -- un
ticker fijo se queda "muerto" quue el partido termina. Esta funcion siempre
trae los mercados que estan abiertos AHORA para esa serie.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests

PROD_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
DEMO_BASE_URL = "https://external-api.demo.kalshi.co/trade-api/v2"


def base_url() -> str:
    env = os.environ.get("KALSHI_ENV", "prod").strip().lower()
    return DEMO_BASE_URL if env == "demo" else PROD_BASE_URL


class KalshiClient:
    def __init__(self, timeout_seconds: float = 10.0):
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def get_open_markets_for_series(self, series_ticker: str, limit: int = 100) -> list[dict[str, Any]]:
        """Trae todos los mercados actualmente abiertos de una serie (p. ej.
        KXMLBGAME). Pagina con cursor hasta agotar resultados. Reintenta con
        backoff si Kalshi responde 429 (rate limit)."""
        results: list[dict[str, Any]] = []
        cursor = None
        url = f"{base_url()}/markets"
        while True:
            params = {"series_ticker": series_ticker, "status": "open", "limit": limit}
            if cursor:
                params["cursor"] = cursor

            data = None
            for attempt in range(4):
                try:
                    resp = self.session.get(url, params=params, timeout=self.timeout_seconds)
                    if resp.status_code == 429:
                        wait = 2 ** attempt
                        print(f"[warn] rate limit en '{series_ticker}', reintentando en {wait}s")
                        time.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except requests.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else "?"
                    print(f"[warn] no se pudo leer la serie '{series_ticker}' (HTTP {status}): {exc}")
                    break
            if data is None:
                break

            results.extend(data.get("markets", []))
            cursor = data.get("cursor")
            if not cursor:
                break
        return results

    def get_open_markets_for_all_series(self, series_tickers: list[str]) -> list[dict[str, Any]]:
        # Si la lista incluye ALL_SPORTS, se expande a todas las series
        # deportivas de partido que Kalshi tenga activas.
        if any(s.strip().upper() == "ALL_SPORTS" for s in series_tickers):
            series_tickers = resolve_series(",".join(series_tickers))
        results: list[dict[str, Any]] = []
        for series in series_tickers:
            results.extend(self.get_open_markets_for_series(series))
        return results


# --- Descubrimiento automatico de series deportivas -----------------------

# Series de "quien gana" por partido/pelea. Se usan como respaldo si Kalshi no
# responde al pedir la lista de series.
FALLBACK_SPORTS_SERIES = [
    "KXMLBGAME", "KXNFLGAME", "KXNCAAFGAME", "KXNBAGAME", "KXWNBAGAME",
    "KXNHLGAME", "KXATPMATCH", "KXWTAMATCH", "KXEPLGAME",
]

# Sufijos de ticker que identifican mercados de ganador de un partido,
# encuentro o pelea (no futuros de campeonato, props ni lideres).
GAME_SUFFIXES = ("GAME", "MATCH", "FIGHT", "BOUT")

ALL_SPORTS_KEYWORD = "ALL_SPORTS"


def discover_sports_game_series(timeout_seconds: float = 15.0) -> list[str]:
    """Pide a Kalshi todas las series de la categoria Sports y se queda con
    las de ganador por partido (terminan en GAME, MATCH, FIGHT o BOUT).
    Asi el bot cubre cualquier deporte nuevo que Kalshi agregue sin tocar
    el codigo."""
    session = requests.Session()
    url = f"{base_url()}/series"
    found: set[str] = set()
    cursor = None
    for _ in range(20):  # tope de paginas por seguridad
        params: dict[str, Any] = {"category": "Sports"}
        if cursor:
            params["cursor"] = cursor
        data = None
        for attempt in range(4):
            try:
                resp = session.get(url, params=params, timeout=timeout_seconds)
                if resp.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.RequestException as exc:
                print(f"[warn] no se pudo listar series deportivas: {exc}")
                break
        if data is None:
            break
        for serie in data.get("series") or []:
            ticker = (serie.get("ticker") or "").upper()
            if ticker.endswith(GAME_SUFFIXES):
                found.add(ticker)
        cursor = data.get("cursor")
        if not cursor:
            break

    if not found:
        print("[warn] descubrimiento vacio; uso la lista de respaldo")
        return list(FALLBACK_SPORTS_SERIES)
    return sorted(found)


def resolve_series(raw: str) -> list[str]:
    """Convierte el valor de KALSHI_SERIES en la lista final de series.
    - "ALL_SPORTS" (o vacio): todas las series deportivas de partido.
    - Lista separada por comas: esas series. Se puede mezclar, p. ej.
      "ALL_SPORTS,KXNBA" para sumar series extra."""
    items = [s.strip().upper() for s in (raw or "").split(",") if s.strip()]
    if not items or ALL_SPORTS_KEYWORD in items:
        extras = [s for s in items if s != ALL_SPORTS_KEYWORD]
        series = discover_sports_game_series()
        for e in extras:
            if e not in series:
                series.append(e)
        print(f"[info] {len(series)} series deportivas activas: {', '.join(series)}")
        return series
    return items


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_snapshot(market: dict[str, Any]) -> dict[str, Any]:
    """Los precios de Kalshi llegan como *_dollars (FixedPointDollars) y el
    volumen como volume_fp -- no *_cents/volume 'planos'. Si Render reporta
    errores de columna al insertar, imprime `market` crudo una vez y ajusta
    este mapeo contra la respuesta real en vez de adivinar."""
    volume_raw = market.get("volume_fp")
    try:
        volume = int(float(volume_raw)) if volume_raw is not None else None
    except (TypeError, ValueError):
        volume = None
    return {
        "ticker": market.get("ticker"),
        "series_ticker": market.get("event_ticker", "").rsplit("-", 1)[0] if market.get("event_ticker") else None,
        "title": market.get("title") or market.get("yes_sub_title"),
        "yes_bid": _as_float(market.get("yes_bid_dollars")),
        "yes_ask": _as_float(market.get("yes_ask_dollars")),
        "no_bid": _as_float(market.get("no_bid_dollars")),
        "no_ask": _as_float(market.get("no_ask_dollars")),
        "volume": volume,
        "status": market.get("status"),
        "close_time": market.get("close_time"),
    }
