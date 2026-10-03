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
    """Bot de trading conservador para mercados deportivos de Kalshi.

Corre como un Cron Job separado del poller de precios (poll_markets.py).
En cada corrida:
  1. Revisa posiciones abiertas registradas en `kalshi_trades` y las cierra
     si tocaron take-profit, stop-loss, o si el mercado ya cerro.
  2. Si no se supero el limite de perdida diaria ni el maximo de posiciones
     simultaneas, busca nuevas entradas que cumplan la regla conservadora:
       - volumen del dia > MIN_VOLUME
       - spread (yes_ask - yes_bid) <= MAX_SPREAD_CENTS
       - yes_ask entre MIN_ENTRY_CENTS y MAX_ENTRY_CENTS
     y compra un tamano fijo (FIXED_CONTRACTS) del lado YES.

Todos los umbrales son variables de entorno con un default conservador --
ver la tabla al final de este archivo.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from kalshi_client import KalshiClient, resolve_series
from kalshi_trading_client import KalshiTradingClient

# --- Parametros de la estrategia (ajustables por variable de entorno) ---
MIN_VOLUME = int(os.environ.get("STRAT_MIN_VOLUME", "50"))
MAX_SPREAD_CENTS = int(os.environ.get("STRAT_MAX_SPREAD_CENTS", "3"))
MIN_ENTRY_CENTS = int(os.environ.get("STRAT_MIN_ENTRY_CENTS", "20"))
MAX_ENTRY_CENTS = int(os.environ.get("STRAT_MAX_ENTRY_CENTS", "45"))
TAKE_PROFIT_CENTS = int(os.environ.get("STRAT_TAKE_PROFIT_CENTS", "15"))
STOP_LOSS_CENTS = int(os.environ.get("STRAT_STOP_LOSS_CENTS", "10"))
FIXED_CONTRACTS = int(os.environ.get("STRAT_FIXED_CONTRACTS", "5"))
MAX_OPEN_POSITIONS = int(os.environ.get("STRAT_MAX_OPEN_POSITIONS", "3"))
DAILY_LOSS_LIMIT_CENTS = int(os.environ.get("STRAT_DAILY_LOSS_LIMIT_CENTS", "500"))  # $5.00 default

SCHEMA = """
CREATE TABLE IF NOT EXISTS kalshi_trades (
    id            BIGSERIAL PRIMARY KEY,
    ticker        TEXT NOT NULL,
    side          TEXT NOT NULL DEFAULT 'yes',
    count         INTEGER NOT NULL,
    entry_price   INTEGER NOT NULL,   -- centavos
    exit_price    INTEGER,            -- centavos
    status        TEXT NOT NULL DEFAULT 'open',  -- open | closed_tp | closed_sl | closed_settled
    pnl_cents     INTEGER,
    opened_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ
);
"""


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("falta DATABASE_URL en el entorno")
    return psycopg2.connect(database_url)


def today_realized_pnl_cents(cur) -> int:
    cur.execute(
        """
        SELECT COALESCE(SUM(pnl_cents), 0) FROM kalshi_trades
        WHERE closed_at::date = now()::date AND status != 'open'
        """
    )
    return cur.fetchone()[0]


def open_positions_count(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM kalshi_trades WHERE status = 'open'")
    return cur.fetchone()[0]


def already_holds(cur, ticker: str) -> bool:
    cur.execute("SELECT 1 FROM kalshi_trades WHERE ticker = %s AND status = 'open'", (ticker,))
    return cur.fetchone() is not None


def manage_open_positions(cur, public_client: KalshiClient, trading_client: KalshiTradingClient) -> None:
    cur.execute("SELECT id, ticker, count, entry_price FROM kalshi_trades WHERE status = 'open'")
    open_trades = cur.fetchall()
    if not open_trades:
        return

    # Trae el estado actual de todos los mercados relevantes de una vez.
    tickers = {row[1] for row in open_trades}
    series_seen = {t.rsplit("-", 1)[0] for t in tickers}
    live_by_ticker = {}
    for series in series_seen:
        for m in public_client.get_open_markets_for_series(series):
            live_by_ticker[m.get("ticker")] = m

    for trade_id, ticker, count, entry_price in open_trades:
        market = live_by_ticker.get(ticker)

        if market is None:
            # Ya no aparece como abierto -> el partido termino y se liquido solo.
            # Kalshi paga 100c o 0c segun el resultado; lo marcamos como
            # settled con pnl desconocido (se puede reconciliar luego contra
            # /portfolio/positions si hace falta precision exacta).
            cur.execute(
                "UPDATE kalshi_trades SET status = 'closed_settled', closed_at = %s WHERE id = %s",
                (datetime.now(timezone.utc), trade_id),
            )
            print(f"[info] {ticker} ya no esta abierto, marcado como liquidado")
            continue

        yes_bid_dollars = market.get("yes_bid_dollars")
        if yes_bid_dollars is None:
            continue
        current_bid_cents = round(float(yes_bid_dollars) * 100)

        hit_take_profit = current_bid_cents >= entry_price + TAKE_PROFIT_CENTS
        hit_stop_loss = current_bid_cents <= entry_price - STOP_LOSS_CENTS

        if not (hit_take_profit or hit_stop_loss):
            continue

        try:
            trading_client.create_order(
                ticker=ticker, side="yes", action="sell", count=count, price_cents=current_bid_cents
            )
        except Exception as exc:
            print(f"[warn] no se pudo vender {ticker}: {exc}")
            continue

        pnl_cents = (current_bid_cents - entry_price) * count
        status = "closed_tp" if hit_take_profit else "closed_sl"
        cur.execute(
            """UPDATE kalshi_trades
               SET status = %s, exit_price = %s, pnl_cents = %s, closed_at = %s
               WHERE id = %s""",
            (status, current_bid_cents, pnl_cents, datetime.now(timezone.utc), trade_id),
        )
        print(f"[info] {ticker} cerrado ({status}), pnl {pnl_cents}c")


def look_for_entries(cur, public_client: KalshiClient, trading_client: KalshiTradingClient, series_tickers: list[str]) -> None:
    if open_positions_count(cur) >= MAX_OPEN_POSITIONS:
        print("[info] maximo de posiciones abiertas alcanzado, no se buscan nuevas entradas")
        return
    if today_realized_pnl_cents(cur) <= -DAILY_LOSS_LIMIT_CENTS:
        print("[info] limite de perdida diaria alcanzado, no se abren nuevas posiciones hoy")
        return

    for market in public_client.get_open_markets_for_all_series(series_tickers):
        if open_positions_count(cur) >= MAX_OPEN_POSITIONS:
            break

        ticker = market.get("ticker")
        if not ticker or already_holds(cur, ticker):
            continue

        volume_raw = market.get("volume_fp") or 0
        try:
            volume = int(float(volume_raw))
        except (TypeError, ValueError):
            volume = 0
        yes_bid = market.get("yes_bid_dollars")
        yes_ask = market.get("yes_ask_dollars")
        if yes_bid is None or yes_ask is None:
            continue

        yes_bid_cents = round(float(yes_bid) * 100)
        yes_ask_cents = round(float(yes_ask) * 100)
        spread = yes_ask_cents - yes_bid_cents

        if volume < MIN_VOLUME:
            continue
        if spread > MAX_SPREAD_CENTS or spread < 0:
            continue
        if not (MIN_ENTRY_CENTS <= yes_ask_cents <= MAX_ENTRY_CENTS):
            continue

        try:
            trading_client.create_order(
                ticker=ticker, side="yes", action="buy", count=FIXED_CONTRACTS, price_cents=yes_ask_cents
            )
        except Exception as exc:
            print(f"[warn] no se pudo comprar {ticker}: {exc}")
            continue

        cur.execute(
            """INSERT INTO kalshi_trades (ticker, side, count, entry_price, status)
               VALUES (%s, 'yes', %s, %s, 'open')""",
            (ticker, FIXED_CONTRACTS, yes_ask_cents),
        )
        print(f"[info] entro en {ticker} a {yes_ask_cents}c x{FIXED_CONTRACTS}")


def main() -> None:
    series_tickers = resolve_series(os.environ.get("KALSHI_SERIES", "ALL_SPORTS"))

    public_client = KalshiClient()
    trading_client = KalshiTradingClient()

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            conn.commit()

        with conn.cursor() as cur:
            manage_open_positions(cur, public_client, trading_client)
            conn.commit()

        with conn.cursor() as cur:
            look_for_entries(cur, public_client, trading_client, series_tickers)
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()}
    """Cron job: para cada serie deportiva en KALSHI_SERIES, descubre los
mercados actualmente ABIERTOS (partidos de hoy/en curso) y guarda una fila
por mercado en Postgres. No usa una lista fija de tickers -- los partidos
de deportes en Kalshi son mercados de corta duracion, asi que cada corrida
vuelve a preguntar "que esta abierto ahora" en vez de asumir un ticker que
podria ya haber cerrado."""

from __future__ import annotations

import os

import psycopg2
import psycopg2.extras

from kalshi_client import KalshiClient, normalize_snapshot, resolve_series

SCHEMA = """
CREATE TABLE IF NOT EXISTS kalshi_snapshots (
    id            BIGSERIAL PRIMARY KEY,
    ticker        TEXT NOT NULL,
    series_ticker TEXT,
    title         TEXT,
    yes_bid       NUMERIC(6,4),
    yes_ask       NUMERIC(6,4),
    no_bid        NUMERIC(6,4),
    no_ask        NUMERIC(6,4),
    volume        BIGINT,
    status        TEXT,
    close_time    TIMESTAMPTZ,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_kalshi_snapshots_ticker_time
    ON kalshi_snapshots (ticker, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_kalshi_snapshots_series_time
    ON kalshi_snapshots (series_ticker, fetched_at DESC);
"""

INSERT = """
INSERT INTO kalshi_snapshots
    (ticker, series_ticker, title, yes_bid, yes_ask, no_bid, no_ask, volume, status, close_time)
VALUES
    (%(ticker)s, %(series_ticker)s, %(title)s, %(yes_bid)s, %(yes_ask)s,
     %(no_bid)s, %(no_ask)s, %(volume)s, %(status)s, %(close_time)s)
"""


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("falta DATABASE_URL en el entorno del cron job")
    return psycopg2.connect(database_url)


def main() -> None:
    series_tickers = resolve_series(os.environ.get("KALSHI_SERIES", "ALL_SPORTS"))

    client = KalshiClient()
    markets = client.get_open_markets_for_all_series(series_tickers)
    print(f"[info] {len(markets)} mercados abiertos encontrados en {len(series_tickers)} series")

    if not markets:
        print("[info] nada que guardar en esta corrida")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            rows = [normalize_snapshot(m) for m in markets]
            psycopg2.extras.execute_batch(cur, INSERT, rows)
        conn.commit()
        print(f"[info] {len(rows)} filas guardadas")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
