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
import time
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
# Pausa entre series para no saturar la API de Kalshi (errores 429).
SERIES_PAUSE_SECONDS = float(os.environ.get("STRAT_SERIES_PAUSE_SECONDS", "0.25"))
# Candado de Postgres para que nunca corran dos instancias a la vez.
RUN_LOCK_ID = 774411

# Series y mercados que NO son "quien gana": marcadores exactos, empates, etc.
EXCLUDED_SERIES_WORDS = ("EXACT", "SCORE", "SPREAD", "TOTAL", "PROP")
EXCLUDED_MARKET_SUFFIXES = ("-TIE", "-DRAW")


def series_allowed(series: str) -> bool:
    s = series.upper()
    return not any(w in s for w in EXCLUDED_SERIES_WORDS)


def market_allowed(ticker: str) -> bool:
    return not ticker.upper().endswith(EXCLUDED_MARKET_SUFFIXES)


def reopen_wrongly_settled(conn, cur, trading_client: KalshiTradingClient) -> None:
    """Vuelve a abrir trades marcados como liquidados que en realidad
    siguen como posicion en tu cuenta de Kalshi."""
    try:
        data = trading_client.get_positions()
    except Exception as exc:
        print(f"[warn] no se pudo reconciliar con Kalshi: {exc}")
        return
    held = []
    for p in data.get("market_positions") or []:
        raw = p.get("position_fp", p.get("position", 0))
        try:
            qty = float(raw or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty > 0 and p.get("ticker"):
            held.append(p["ticker"])
    if not held:
        return
    cur.execute(
        """UPDATE kalshi_trades SET status = 'open', closed_at = NULL
           WHERE status = 'closed_settled' AND exit_price IS NULL
             AND ticker = ANY(%s)
             AND id IN (SELECT MAX(id) FROM kalshi_trades GROUP BY ticker)""",
        (held,),
    )
    if cur.rowcount:
        print(f"[info] {cur.rowcount} posiciones reabiertas: seguian activas en Kalshi")
    conn.commit()


def kalshi_busy_tickers(trading_client: KalshiTradingClient) -> set[str] | None:
    """Tickers donde ya tienes contratos o una orden pendiente EN KALSHI.
    Es la fuente de verdad: aunque la base de datos falle, el bot no vuelve
    a comprar lo mismo. Devuelve None si no se pudo consultar (en ese caso
    el bot no abre nada nuevo, por seguridad)."""
    busy: set[str] = set()
    try:
        data = trading_client.get_positions()
        for p in data.get("market_positions") or []:
            raw = p.get("position_fp", p.get("position", 0))
            try:
                qty = float(raw or 0)
            except (TypeError, ValueError):
                qty = 0
            if qty != 0 and p.get("ticker"):
                busy.add(p["ticker"])
    except Exception as exc:
        print(f"[warn] no se pudieron leer posiciones de Kalshi: {exc}")
        return None
    try:
        cursor = None
        for _ in range(10):
            params = {"status": "resting", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            data = trading_client._get("/portfolio/orders", params)
            for o in data.get("orders") or []:
                if o.get("ticker"):
                    busy.add(o["ticker"])
            cursor = data.get("cursor")
            if not cursor:
                break
    except Exception as exc:
        print(f"[warn] no se pudieron leer ordenes pendientes de Kalshi: {exc}")
        return None
    return busy

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
    # La serie es lo que va antes del primer guion (KXACBGAME-26OCT...-ZAR
    # -> KXACBGAME). Antes se usaba el evento completo y Kalshi devolvia
    # vacio, asi que el bot creia que todo estaba liquidado.
    tickers = {row[1] for row in open_trades}
    series_seen = {t.split("-", 1)[0] for t in tickers}
    live_by_ticker = {}
    series_ok: set[str] = set()
    for series in series_seen:
        try:
            for m in public_client.get_open_markets_for_series(series):
                live_by_ticker[m.get("ticker")] = m
            series_ok.add(series)
        except Exception as exc:
            print(f"[warn] no se pudo revisar la serie {series}: {exc}")
        time.sleep(SERIES_PAUSE_SECONDS)

    for trade_id, ticker, count, entry_price in open_trades:
        market = live_by_ticker.get(ticker)

        if market is None and ticker.split("-", 1)[0] not in series_ok:
            # No pudimos consultar su serie: no asumimos nada.
            continue
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


def look_for_entries(conn, cur, public_client: KalshiClient, trading_client: KalshiTradingClient, series_tickers: list[str]) -> None:
    busy = kalshi_busy_tickers(trading_client)
    if busy is None:
        print("[info] sin datos de Kalshi sobre tus posiciones; no se abre nada nuevo en esta corrida")
        return

    def total_open() -> int:
        cur.execute("SELECT ticker FROM kalshi_trades WHERE status = 'open'")
        return len(busy | {r[0] for r in cur.fetchall()})

    if total_open() >= MAX_OPEN_POSITIONS:
        print("[info] maximo de posiciones abiertas alcanzado, no se buscan nuevas entradas")
        return
    if today_realized_pnl_cents(cur) <= -DAILY_LOSS_LIMIT_CENTS:
        print("[info] limite de perdida diaria alcanzado, no se abren nuevas posiciones hoy")
        return

    def iter_markets():
        for series in series_tickers:
            if not series_allowed(series):
                continue
            try:
                for m in public_client.get_open_markets_for_series(series):
                    yield m
            except Exception as exc:
                print(f"[warn] no se pudo leer la serie {series}: {exc}")
            time.sleep(SERIES_PAUSE_SECONDS)

    for market in iter_markets():
        if total_open() >= MAX_OPEN_POSITIONS:
            print("[info] maximo de posiciones abiertas alcanzado")
            break

        ticker = market.get("ticker")
        if not ticker or not market_allowed(ticker):
            continue
        if ticker in busy or already_holds(cur, ticker):
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
        conn.commit()  # se guarda en el acto: si la corrida falla despues, no se pierde
        busy.add(ticker)
        print(f"[info] entro en {ticker} a {yes_ask_cents}c x{FIXED_CONTRACTS}")


def main() -> None:
    series_tickers = [s for s in resolve_series(os.environ.get("KALSHI_SERIES", "ALL_SPORTS")) if series_allowed(s)]

    public_client = KalshiClient()
    trading_client = KalshiTradingClient()

    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (RUN_LOCK_ID,))
            if not cur.fetchone()[0]:
                print("[info] otra corrida sigue activa; esta se salta")
                conn.commit()
                return
            conn.commit()

        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            conn.commit()

        with conn.cursor() as cur:
            reopen_wrongly_settled(conn, cur, trading_client)

        with conn.cursor() as cur:
            manage_open_positions(cur, public_client, trading_client)
            conn.commit()

        with conn.cursor() as cur:
            look_for_entries(conn, cur, public_client, trading_client, series_tickers)
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
