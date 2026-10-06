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
from kalshi_trading_client import KalshiTradingClient, parse_fill

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
# Tope de perdida en ventana movil (antes era por dia UTC y se reiniciaba a
# las 6 p. m. hora de Mexico/Centroamerica).
LOSS_WINDOW_HOURS = int(os.environ.get("STRAT_LOSS_WINDOW_HOURS", "24"))
# Despues de operar un partido (abierto o cerrado), no se vuelve a entrar en
# ningun mercado de ese mismo partido durante estas horas.
GAME_COOLDOWN_HOURS = int(os.environ.get("STRAT_GAME_COOLDOWN_HOURS", "24"))
# Con STRAT_ENTRIES_PAUSED=1 el bot NO abre posiciones nuevas, pero sigue
# vigilando y cerrando las que ya tiene (stop-loss / take-profit).
ENTRIES_PAUSED = os.environ.get("STRAT_ENTRIES_PAUSED", "").strip().lower() in ("1", "true", "yes", "si")
# En un stop-loss la orden de venta va unos centavos POR DEBAJO del bid para
# que cruce el libro y se llene aunque el precio siga cayendo.
SL_SLIPPAGE_CENTS = int(os.environ.get("STRAT_SL_SLIPPAGE_CENTS", "3"))
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
           WHERE status = 'closed_settled' AND exit_price IS NULL AND pnl_cents IS NULL
             AND ticker = ANY(%s)
             AND id IN (SELECT MAX(id) FROM kalshi_trades GROUP BY ticker)""",
        (held,),
    )
    if cur.rowcount:
        print(f"[info] {cur.rowcount} posiciones reabiertas: seguian activas en Kalshi")
    conn.commit()


def kalshi_held_tickers(trading_client: KalshiTradingClient) -> set[str] | None:
    """Tickers donde tienes contratos SIN liquidar en tu cuenta de Kalshi."""
    qty = kalshi_held_quantities(trading_client)
    return None if qty is None else set(qty)


def kalshi_held_quantities(trading_client: KalshiTradingClient) -> dict[str, float] | None:
    """{ticker: contratos} de tus posiciones SIN liquidar en Kalshi
    (GET /portfolio/positions solo devuelve posiciones sin liquidar). Pagina
    por si hay muchas. Devuelve None si no se pudo consultar."""
    held: dict[str, float] = {}
    cursor = None
    try:
        for _ in range(10):
            params: dict = {"limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = trading_client._get("/portfolio/positions", params)
            for p in data.get("market_positions") or []:
                raw = p.get("position_fp", p.get("position", 0))
                try:
                    qty = float(raw or 0)
                except (TypeError, ValueError):
                    qty = 0
                if qty != 0 and p.get("ticker"):
                    held[p["ticker"]] = qty
            cursor = data.get("cursor")
            if not cursor:
                break
    except Exception as exc:
        print(f"[warn] no se pudieron leer posiciones de Kalshi: {exc}")
        return None
    return held


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


def recent_realized_pnl_cents(cur) -> int:
    """Ganancia/perdida realizada en las ultimas LOSS_WINDOW_HOURS horas."""
    cur.execute(
        """
        SELECT COALESCE(SUM(pnl_cents), 0) FROM kalshi_trades
        WHERE status != 'open'
          AND closed_at >= now() - (%s * interval '1 hour')
        """,
        (LOSS_WINDOW_HOURS,),
    )
    return cur.fetchone()[0]


def event_of(ticker: str) -> str:
    """KXAPFDDHGAME-26OCT03RECLIB-REC -> KXAPFDDHGAME-26OCT03RECLIB (el partido)."""
    return ticker.rsplit("-", 1)[0]


def game_recently_traded(cur, ticker: str) -> bool:
    """True si ya hay (o hubo hace menos de GAME_COOLDOWN_HOURS) una operacion
    en cualquier mercado del mismo partido. Evita re-entrar tras un stop-loss
    y evita apostar a los dos lados del mismo partido."""
    cur.execute(
        """
        SELECT 1 FROM kalshi_trades
        WHERE ticker LIKE %s
          AND (status = 'open' OR opened_at >= now() - (%s * interval '1 hour'))
        LIMIT 1
        """,
        (event_of(ticker) + "-%", GAME_COOLDOWN_HOURS),
    )
    return cur.fetchone() is not None


def open_positions_count(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM kalshi_trades WHERE status = 'open'")
    return cur.fetchone()[0]


def already_holds(cur, ticker: str) -> bool:
    cur.execute("SELECT 1 FROM kalshi_trades WHERE ticker = %s AND status = 'open'", (ticker,))
    return cur.fetchone() is not None


def settlement_pnl_cents(public_client: KalshiClient, ticker: str, entry_price: int, count: int) -> int | None:
    """Ganancia/perdida de contratos YES que llegaron hasta el final del
    partido, segun el resultado oficial: si salio YES cada contrato paga 100c,
    si salio NO paga 0c. None si todavia no se sabe (se reintenta)."""
    try:
        market = public_client.get_market(ticker)
    except Exception as exc:
        print(f"[warn] no se pudo leer el resultado de {ticker}: {exc}")
        return None
    result = (market.get("result") or "").strip().lower()
    if result == "yes":
        return (100 - entry_price) * count
    if result == "no":
        return -entry_price * count
    return None


def backfill_settled_pnl(cur, public_client: KalshiClient, max_rows: int = 20) -> None:
    """Completa el pnl de liquidaciones viejas que quedaron vacias. Son
    justo las perdidas que el tope de perdida no estaba contando."""
    cur.execute(
        """SELECT id, ticker, count, entry_price FROM kalshi_trades
           WHERE status = 'closed_settled' AND pnl_cents IS NULL
             AND closed_at >= now() - (%s * interval '1 hour')
           ORDER BY closed_at DESC LIMIT %s""",
        (LOSS_WINDOW_HOURS, max_rows),
    )
    for trade_id, ticker, count, entry_price in cur.fetchall():
        pnl = settlement_pnl_cents(public_client, ticker, entry_price, count)
        if pnl is None:
            continue
        cur.execute("UPDATE kalshi_trades SET pnl_cents = %s WHERE id = %s", (pnl, trade_id))
        print(f"[info] pnl de liquidacion de {ticker} completado: {pnl}c")
        time.sleep(SERIES_PAUSE_SECONDS)


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

    # Posiciones reales en Kalshi; se consultan solo si hace falta y una vez.
    held_cache: dict[str, dict[str, float] | None] = {}

    def held() -> dict[str, float] | None:
        if "v" not in held_cache:
            held_cache["v"] = kalshi_held_quantities(trading_client)
        return held_cache["v"]

    for trade_id, ticker, count, entry_price in open_trades:
        market = live_by_ticker.get(ticker)

        if market is None and ticker.split("-", 1)[0] not in series_ok:
            # No pudimos consultar su serie: no asumimos nada.
            continue
        if market is None:
            # No aparece en la lista de mercados abiertos. Eso puede ser que el
            # partido termino, pero tambien un error de Kalshi (429) o un
            # mercado pausado o cerrado que aun no paga. Antes de darlo por
            # liquidado se confirma en tu cuenta: si Kalshi sigue mostrando la
            # posicion sin liquidar, se mantiene abierta.
            confirmed = held()
            if confirmed is None:
                print(f"[warn] {ticker} no aparece como abierto y no se pudo confirmar en Kalshi; se mantiene abierto")
                continue
            if ticker in confirmed:
                print(f"[info] {ticker} no aparece como abierto pero sigue sin liquidar en tu cuenta; se mantiene abierto")
                continue
            # Ya no esta en tu cuenta -> se liquido. El pnl sale del resultado
            # oficial; si aun no esta publicado se deja abierto y se reintenta.
            settle = settlement_pnl_cents(public_client, ticker, entry_price, count)
            if settle is None:
                print(f"[info] {ticker} ya no esta en tu cuenta pero aun no hay resultado; se reintenta")
                continue
            cur.execute(
                """UPDATE kalshi_trades SET status = 'closed_settled', closed_at = %s,
                       pnl_cents = COALESCE(pnl_cents, 0) + %s WHERE id = %s""",
                (datetime.now(timezone.utc), settle, trade_id),
            )
            print(f"[info] {ticker} liquidado, pnl {settle}c")
            continue

        yes_bid_dollars = market.get("yes_bid_dollars")
        if yes_bid_dollars is None:
            continue
        current_bid_cents = round(float(yes_bid_dollars) * 100)

        hit_take_profit = current_bid_cents >= entry_price + TAKE_PROFIT_CENTS
        hit_stop_loss = current_bid_cents <= entry_price - STOP_LOSS_CENTS

        if not (hit_take_profit or hit_stop_loss):
            continue

        # Vende solo los contratos que de verdad tienes en Kalshi. Si la compra
        # se lleno en parte, vender los 5 haria que Kalshi tome el sobrante
        # como apuesta nueva y rechace todo por "saldo insuficiente".
        if current_bid_cents <= 0:
            print(f"[info] {ticker}: nadie compra (bid 0); se mantiene hasta que se liquide")
            continue

        holdings = held()
        if holdings is not None:
            real = int(max(0.0, holdings.get(ticker, 0.0)))
            if real <= 0:
                print(f"[info] {ticker}: no tienes contratos en Kalshi; no se vende")
                continue
            if real < count:
                # La base decia mas contratos de los que hay: se corrige.
                print(f"[info] {ticker}: tienes {real} de {count} contratos; se corrige y se venden {real}")
                cur.execute("UPDATE kalshi_trades SET count = %s WHERE id = %s", (real, trade_id))
                count = real

        # Take-profit al bid; stop-loss unos centavos abajo para que se llene.
        sell_price = current_bid_cents if hit_take_profit else max(1, current_bid_cents - SL_SLIPPAGE_CENTS)
        try:
            resp = trading_client.create_order(
                ticker=ticker, side="yes", action="sell", count=count, price_cents=sell_price,
                reduce_only=True,  # IOC: nunca vende de mas ni deja ordenes colgadas
            )
        except Exception as exc:
            print(f"[warn] no se pudo vender {ticker}: {exc}")
            continue

        # Solo se registra lo que Kalshi confirma como vendido.
        filled, avg_cents = parse_fill(resp)
        if filled <= 0:
            print(f"[info] la venta de {ticker} no se lleno; sigue abierta y se reintenta")
            continue
        filled = min(filled, count)
        exit_price = avg_cents if avg_cents is not None else sell_price
        pnl_part = (exit_price - entry_price) * filled

        if filled < count:
            cur.execute(
                "UPDATE kalshi_trades SET count = count - %s, pnl_cents = COALESCE(pnl_cents, 0) + %s WHERE id = %s",
                (filled, pnl_part, trade_id),
            )
            print(f"[info] {ticker}: venta parcial {filled} de {count}; el resto sigue abierto (pnl parcial {pnl_part}c)")
            continue

        status = "closed_tp" if hit_take_profit else "closed_sl"
        cur.execute(
            """UPDATE kalshi_trades
               SET status = %s, exit_price = %s, pnl_cents = COALESCE(pnl_cents, 0) + %s, closed_at = %s
               WHERE id = %s""",
            (status, exit_price, pnl_part, datetime.now(timezone.utc), trade_id),
        )
        print(f"[info] {ticker} cerrado ({status}) a {exit_price}c, pnl {pnl_part}c")


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
    if recent_realized_pnl_cents(cur) <= -DAILY_LOSS_LIMIT_CENTS:
        print(f"[info] limite de perdida alcanzado en las ultimas {LOSS_WINDOW_HOURS} h, no se abren nuevas posiciones")
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
        event = event_of(ticker)
        if any(event_of(b) == event for b in busy) or game_recently_traded(cur, ticker):
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
            resp = trading_client.create_order(
                ticker=ticker, side="yes", action="buy", count=FIXED_CONTRACTS, price_cents=yes_ask_cents,
                time_in_force="immediate_or_cancel",  # compra lo que haya a ese precio; nada queda colgado
            )
        except Exception as exc:
            if "insufficient_balance" in str(exc):
                print("[info] saldo insuficiente en Kalshi; no se buscan mas compras en esta corrida")
                break
            print(f"[warn] no se pudo comprar {ticker}: {exc}")
            continue

        # Solo se registra lo que de verdad se compro, al precio real.
        filled, avg_cents = parse_fill(resp)
        if filled <= 0:
            print(f"[info] {ticker}: no habia vendedores a {yes_ask_cents}c; no se compro nada")
            continue
        filled = min(filled, FIXED_CONTRACTS)
        entry = avg_cents if avg_cents is not None else yes_ask_cents
        cur.execute(
            """INSERT INTO kalshi_trades (ticker, side, count, entry_price, status)
               VALUES (%s, 'yes', %s, %s, 'open')""",
            (ticker, filled, entry),
        )
        conn.commit()  # se guarda en el acto: si la corrida falla despues, no se pierde
        busy.add(ticker)
        print(f"[info] entro en {ticker} a {entry}c x{filled}")


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
            backfill_settled_pnl(cur, public_client)
            conn.commit()

        if ENTRIES_PAUSED:
            print("[info] STRAT_ENTRIES_PAUSED activo: no se abren posiciones nuevas (las abiertas siguen vigiladas)")
        else:
            with conn.cursor() as cur:
                look_for_entries(conn, cur, public_client, trading_client, series_tickers)
                conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
