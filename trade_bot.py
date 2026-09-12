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

from kalshi_client import KalshiClient
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

        volume = market.get("volume_fp") or 0
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
    series_raw = os.environ.get("KALSHI_SERIES", "")
    series_tickers = [s.strip() for s in series_raw.split(",") if s.strip()]
    if not series_tickers:
        raise RuntimeError("falta KALSHI_SERIES en el entorno")

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
    main()
