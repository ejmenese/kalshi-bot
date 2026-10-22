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
# Tamano de entrada reducido que se usa automaticamente mientras la perdida
# realizada de las ultimas STRAT_LOSS_WINDOW_HOURS siga tocando el limite
# diario (ver mas abajo, en look_for_entries). No frena las compras, solo
# las achica, y vuelve sola a FIXED_CONTRACTS cuando esa ventana se enfria.
REDUCED_CONTRACTS = int(os.environ.get("STRAT_REDUCED_CONTRACTS", "5"))
MAX_OPEN_POSITIONS = int(os.environ.get("STRAT_MAX_OPEN_POSITIONS", "3"))
DAILY_LOSS_LIMIT_CENTS = int(os.environ.get("STRAT_DAILY_LOSS_LIMIT_CENTS", "500"))  # $5.00 default
# Tope de perdida en ventana movil (antes era por dia UTC y se reiniciaba a
# las 6 p. m. hora de Mexico/Centroamerica).
LOSS_WINDOW_HOURS = int(os.environ.get("STRAT_LOSS_WINDOW_HOURS", "24"))
# Maximo de entradas (abiertas + cerradas, sumadas) que se permiten en el
# mismo partido. Antes esto era, en la practica, 1 (cualquier operacion en el
# partido bloqueaba reentrar por 24h via game_recently_traded). Ahora permite
# montar y liquidar varias veces seguidas en el mismo partido -- nunca al
# mismo tiempo (eso lo sigue evitando el chequeo de "busy" por evento en
# look_for_entries), siempre una cerrada antes de abrir la siguiente.
MAX_ENTRIES_PER_GAME = int(os.environ.get("STRAT_MAX_ENTRIES_PER_GAME", "3"))
# Maximo de entradas nuevas por hora (ventana movil de 60 min). 0 = sin limite.
MAX_ENTRIES_PER_HOUR = int(os.environ.get("STRAT_MAX_ENTRIES_PER_HOUR", "0"))

# --- Apuesta de rescate (comeback) en futbol americano -----------------
# Regla aparte del filtro normal de entradas (que va de MIN_ENTRY_CENTS a
# MAX_ENTRY_CENTS). Busca partidos de NFL/NCAAF donde un equipo ya cotiza
# carisimo (>= RESCUE_EXPENSIVE_MIN_CENTS, "practicamente decidido") y el
# rival cotiza baratisimo (<= RESCUE_CHEAP_MAX_CENTS) con el cierre del
# mercado proximo (dentro de RESCUE_CLOSE_WINDOW_MINUTES) -- la foto que
# describe un equipo perdiendo por poco con poco tiempo en el reloj. No hay
# reloj de juego en vivo (el bot revisa cada 15 min, no segundo a segundo);
# el precio + la cercania del cierre son el sustituto. Compra el lado barato
# por un monto fijo en dolares (RESCUE_DOLLARS), no por cantidad de
# contratos fijos como el resto de la estrategia.
RESCUE_ENABLED = os.environ.get("STRAT_RESCUE_ENABLED", "1") == "1"
RESCUE_SERIES = ["KXNFLGAME", "KXNCAAFGAME"]
RESCUE_CHEAP_MAX_CENTS = int(os.environ.get("STRAT_RESCUE_CHEAP_MAX_CENTS", "10"))
RESCUE_EXPENSIVE_MIN_CENTS = int(os.environ.get("STRAT_RESCUE_EXPENSIVE_MIN_CENTS", "90"))
RESCUE_CLOSE_WINDOW_MINUTES = int(os.environ.get("STRAT_RESCUE_CLOSE_WINDOW_MINUTES", "30"))
RESCUE_DOLLARS = float(os.environ.get("STRAT_RESCUE_DOLLARS", "2"))
# El cron sigue corriendo cada 15 min (Render) para vigilar y proteger las
# posiciones abiertas, pero la BUSQUEDA de entradas nuevas solo se hace una
# vez cada STRAT_ENTRY_SCAN_MINUTES (60 por defecto = "cada hora, 24/7").
# Asi se baja la frecuencia de abrir posiciones nuevas sin dejar de vigilar
# las que ya estan abiertas con la frecuencia de siempre.
ENTRY_SCAN_INTERVAL_MINUTES = int(os.environ.get("STRAT_ENTRY_SCAN_MINUTES", "60"))
# Si un mercado se abrio hace menos de estos minutos (el partido acaba de
# arrancar), se adelanta la busqueda de entradas sin esperar a que pase la
# hora completa -- recien empezado el partido suele haber mas oportunidades
# de precio que mas tarde. El cron sigue corriendo cada 15 min, asi que en el
# peor caso se detecta dentro de esa ventana de 15 min, no al instante exacto.
NEW_GAME_WINDOW_MINUTES = int(os.environ.get("STRAT_NEW_GAME_WINDOW_MINUTES", "20"))
# Kalshi reparte el saldo en "bolsillos" (exchange_index) y cada mercado solo
# puede pagarse con el suyo. Con STRAT_POCKET_CHECK=1 (por defecto) el bot solo
# compra en mercados cuyo bolsillo tiene saldo suficiente.
POCKET_CHECK = os.environ.get("STRAT_POCKET_CHECK", "1").strip().lower() in ("1", "true", "yes", "si")
# Con STRAT_AUTO_REBALANCE=1 (por defecto) el bot le pide a Kalshi, al inicio
# de cada corrida, que reparta el saldo en partes iguales entre los bolsillos
# que existan (target_balance_allocation). Kalshi mueve fondos solo cada
# ~10s hacia ese reparto; no es instantaneo, por eso ademas existe
# STRAT_POCKET_AUTOFUND para el momento exacto de comprar.
AUTO_REBALANCE = os.environ.get("STRAT_AUTO_REBALANCE", "1").strip().lower() in ("1", "true", "yes", "si")
# Con STRAT_POCKET_AUTOFUND=1 (por defecto) el bot, antes de omitir una compra
# por falta de saldo en su bolsillo, intenta transferirle lo que falta desde
# el bolsillo con mas sobrante. Si la transferencia no llega a tiempo (es
# asincrona) o falla, se cae al comportamiento de siempre: se omite el
# mercado y sigue con el resto.
POCKET_AUTOFUND = os.environ.get("STRAT_POCKET_AUTOFUND", "1").strip().lower() in ("1", "true", "yes", "si")
# Vigila las posiciones que pones a mano en Kalshi (las que no abrio el bot) y
# las cierra en dos casos: si caen MANUAL_STOP_CENTS por debajo de su costo
# promedio (proteccion/stop-loss), o si suben MANUAL_TAKE_PROFIT_CENTS por
# encima (toma de ganancia automatica).
PROTECT_MANUAL = os.environ.get("STRAT_PROTECT_MANUAL", "1").strip().lower() in ("1", "true", "yes", "si")
MANUAL_STOP_CENTS = int(os.environ.get("STRAT_MANUAL_STOP_CENTS", str(STOP_LOSS_CENTS)))
MANUAL_TAKE_PROFIT_CENTS = int(os.environ.get("STRAT_MANUAL_TAKE_PROFIT_CENTS", str(TAKE_PROFIT_CENTS)))
# Lista puntual de posiciones manuales a "montar hasta el final": se venden
# solo cuando el precio llega casi al maximo (RIDE_SELL_CENTS, cerca del
# pago completo de $1), sin stop-loss ni take-profit normal. Funciona
# INDEPENDIENTE de STRAT_PROTECT_MANUAL (sigue activo aunque la proteccion
# general este apagada) -- es para una apuesta puntual que el usuario pide
# "cobrar" cuando esta practicamente ganada, no para vigilancia general.
RIDE_TO_WIN_TICKERS = [
    t.strip() for t in os.environ.get("STRAT_MANUAL_RIDE_TICKERS", "").split(",") if t.strip()
]
RIDE_TO_WIN_SELL_CENTS = int(os.environ.get("STRAT_MANUAL_RIDE_SELL_CENTS", "99"))
# Igual que RIDE_TO_WIN pero con objetivo en PORCENTAJE de ganancia sobre el
# costo promedio, en vez de un precio fijo cercano al maximo. Pensado para el
# flujo "yo compro a mano para que no se me escape el precio, el bot vende
# solo cuando llegue a tal % de ganancia". Tambien independiente de
# STRAT_PROTECT_MANUAL.
PCT_TARGET_TICKERS = [
    t.strip() for t in os.environ.get("STRAT_MANUAL_PCT_TICKERS", "").split(",") if t.strip()
]
PCT_TARGET_PERCENT = int(os.environ.get("STRAT_MANUAL_PCT_TARGET", "50"))
# Con STRAT_MANUAL_PCT_AUTO=1, en vez de vigilar solo los tickers de la lista
# anterior, se aplica el objetivo de % de ganancia a TODA posicion manual
# nueva automaticamente (no hace falta avisar el ticker). Sigue sin
# stop-loss y sigue independiente de STRAT_PROTECT_MANUAL.
PCT_TARGET_AUTO = os.environ.get("STRAT_MANUAL_PCT_AUTO", "0").strip().lower() in ("1", "true", "yes", "si")
# Con STRAT_ENTRIES_PAUSED=1 el bot NO abre posiciones nuevas, pero sigue
# vigilando y cerrando las que ya tiene (stop-loss / take-profit).
ENTRIES_PAUSED = os.environ.get("STRAT_ENTRIES_PAUSED", "").strip().lower() in ("1", "true", "yes", "si")
# En un stop-loss la orden de venta va unos centavos POR DEBAJO del bid para
# que cruce el libro y se llene aunque el precio siga cayendo.
SL_SLIPPAGE_CENTS = int(os.environ.get("STRAT_SL_SLIPPAGE_CENTS", "3"))
# Pausa entre series para no saturar la API de Kalshi (errores 429).
# Ambiente en el que opera esta corrida; cada trade se etiqueta con el y todas
# las consultas filtran por el, para no mezclar demo con dinero real.
# Cierre manual: STRAT_FORCE_CLOSE=ALL o una lista de tickers separados por coma.
# Vende esas posiciones al mejor bid (con el mismo margen del stop-loss).
_fc = os.environ.get("STRAT_FORCE_CLOSE", "").strip()
FORCE_CLOSE_ALL = _fc.upper() == "ALL"
FORCE_CLOSE = {t.strip() for t in _fc.split(",") if t.strip()} if not FORCE_CLOSE_ALL else set()
# Con STRAT_CANCEL_ALL_ORDERS=1, al inicio de la corrida se cancelan TODAS las
# ordenes que sigan "resting" (sin llenar) en el libro de Kalshi -- tanto las
# que puso el bot como las que el usuario puso a mano en la app. Es un
# interruptor manual: queda activo en cada corrida hasta que se quite o se
# ponga en 0 la variable en Render (no se apaga solo).
CANCEL_ALL_ORDERS = os.environ.get("STRAT_CANCEL_ALL_ORDERS", "").strip().lower() in ("1", "true", "yes", "si")
BOT_ENV = "prod" if os.environ.get("KALSHI_ENV", "demo").strip().lower() == "prod" else "demo"
SERIES_PAUSE_SECONDS = float(os.environ.get("STRAT_SERIES_PAUSE_SECONDS", "0.25"))
# Candado de Postgres para que nunca corran dos instancias a la vez.
RUN_LOCK_ID = 774411

# Series y mercados que NO son "quien gana": marcadores exactos, empates, etc.
# PICKLEBALL se excluye aqui a propósito: Kalshi lo etiqueta con "Tennis" (por
# eso pasa el filtro STRAT_INCLUDE_TAGS), pero no es tenis.
EXCLUDED_SERIES_WORDS = ("EXACT", "SCORE", "SPREAD", "TOTAL", "PROP", "PICKLEBALL")
EXCLUDED_MARKET_SUFFIXES = ("-TIE", "-DRAW")


_ew = os.environ.get("STRAT_EXCLUDE_WORDS", "")
USER_EXCLUDED_WORDS = tuple(w.strip().upper() for w in _ew.split(",") if w.strip())


def series_allowed(series: str) -> bool:
    s = series.upper()
    return not any(w in s for w in EXCLUDED_SERIES_WORDS) and not any(w in s for w in USER_EXCLUDED_WORDS)


def market_allowed(ticker: str) -> bool:
    return not ticker.upper().endswith(EXCLUDED_MARKET_SUFFIXES)


def cancel_all_resting_orders(trading_client: KalshiTradingClient) -> None:
    """Cancela toda orden que siga 'resting' (sin llenar) en el libro de
    Kalshi. Se dispara con STRAT_CANCEL_ALL_ORDERS=1; no toca posiciones ya
    llenas (eso es STRAT_FORCE_CLOSE)."""
    try:
        orders = (trading_client.get_resting_orders() or {}).get("orders", [])
    except Exception as exc:
        print(f"[warn] no se pudo leer ordenes pendientes para cancelar: {exc}")
        return
    if not orders:
        print("[info] STRAT_CANCEL_ALL_ORDERS activo: no hay ordenes pendientes en Kalshi")
        return
    print(f"[info] STRAT_CANCEL_ALL_ORDERS activo: cancelando {len(orders)} orden(es) pendiente(s)")
    for order in orders:
        order_id = order.get("order_id") or order.get("id")
        ticker = order.get("ticker", "?")
        if not order_id:
            print(f"[warn] orden de {ticker} sin order_id en la respuesta; se omite")
            continue
        try:
            trading_client.cancel_order(order_id)
            print(f"[info] cancelada: {ticker} ({order_id})")
        except Exception as exc:
            print(f"[warn] no se pudo cancelar {ticker} ({order_id}): {exc}")


def ensure_even_target_allocation(trading_client: KalshiTradingClient, pockets: dict[int, float]) -> None:
    """Le pide a Kalshi repartir el saldo en partes iguales entre los
    bolsillos que existan hoy (STRAT_AUTO_REBALANCE=1). Kalshi lo aplica de
    fondo cada ~10s; esta llamada solo fija el objetivo, no mueve dinero ella
    misma. Se llama una vez por corrida; si falla, no detiene el bot."""
    if not pockets:
        return
    n = len(pockets)
    base = 100 // n
    allocations = {idx: base for idx in pockets}
    # Reparte el residuo del redondeo en el primer bolsillo para que sume 100.
    first = sorted(allocations)[0]
    allocations[first] += 100 - base * n
    try:
        trading_client.set_target_balance_allocation(allocations)
        print(f"[info] reparto objetivo de saldo pedido a Kalshi: {allocations}")
    except Exception as exc:
        print(f"[warn] no se pudo fijar el reparto objetivo de saldo: {str(exc)[:200]}")


def rebalance_pockets_explicitly(trading_client: KalshiTradingClient, pockets: dict[int, float]) -> None:
    """Transfiere saldo de verdad entre bolsillos para acercarlos a partes
    iguales, en vez de depender solo de ensure_even_target_allocation.

    Se agrega esto porque, revisando logs de Render de mas de 24h seguidas
    (15 oct 13:30 a 16 oct 17:00), con el reparto objetivo fijado en
    25/25/25/25 en TODAS las corridas, el bolsillo 0 se quedo con el saldo
    entero sin moverse ni un centavo mientras los bolsillos 1-3 seguian en
    $0.00. target_balance_allocation parece gobernar solo saldo NUEVO que
    entra (liquidaciones, depositos) -- no reparte retroactivamente el saldo
    que ya esta quieto en un bolsillo. Por eso aqui se hacen transferencias
    explicitas (transfer_between_pockets), que si movieron plata de verdad
    segun los mismos logs."""
    if not pockets or len(pockets) < 2:
        return
    total = sum(pockets.values())
    target = total / len(pockets)
    THRESHOLD = 1.0  # no vale la pena mover centavos sueltos cada corrida
    surplus = {idx: bal - target for idx, bal in pockets.items() if bal - target > THRESHOLD}
    deficits = {idx: target - bal for idx, bal in pockets.items() if target - bal > THRESHOLD}
    if not surplus or not deficits:
        return
    for d_idx in sorted(deficits, key=lambda i: -deficits[i]):
        need = deficits[d_idx]
        for s_idx in sorted(surplus, key=lambda i: -surplus[i]):
            if need <= 0.01 or surplus.get(s_idx, 0.0) <= 0.01:
                continue
            amount = min(surplus[s_idx], need)
            try:
                # transfer_between_pockets espera el monto en CENTAVOS (lo
                # multiplica x100 para mandarselo a Kalshi en centicentavos);
                # `amount` aqui esta en DOLARES porque `pockets` viene de
                # get_balance() en dolares. Sin este x100, cada transferencia
                # quedaba 100 veces mas chica de lo pedido -- confirmado en
                # logs: se pidieron $14.31 a cada bolsillo y solo llegaron
                # $0.14 (2026-10-17, corridas de las 12:30 y 13:45 UTC).
                trading_client.transfer_between_pockets(s_idx, d_idx, amount * 100)
                print(f"[info] rebalance explicito: ${amount:.2f} del bolsillo {s_idx} al {d_idx}")
                pockets[s_idx] -= amount
                pockets[d_idx] = pockets.get(d_idx, 0.0) + amount
                surplus[s_idx] -= amount
                need -= amount
            except Exception as exc:
                print(f"[warn] no se pudo rebalancear del bolsillo {s_idx} al {d_idx}: {str(exc)[:200]}")
                break
        if need <= 0.01:
            continue


def autofund_pocket(trading_client: KalshiTradingClient, pockets: dict[int, float], pocket: int, needed_cents: float) -> bool:
    """Intenta cubrir `needed_cents` en `pocket` transfiriendolo desde el
    bolsillo con mas sobrante. Actualiza `pockets` de forma optimista si la
    transferencia se acepta (es asincrona del lado de Kalshi, puede tardar
    unos segundos en reflejarse de verdad). Devuelve True si se pidio la
    transferencia, False si no habia de donde sacar el dinero o fallo."""
    shortfall = needed_cents - pockets.get(pocket, 0.0)
    if shortfall <= 0:
        return False
    donor = max(
        (idx for idx in pockets if idx != pocket),
        key=lambda idx: pockets[idx],
        default=None,
    )
    if donor is None or pockets[donor] <= shortfall:
        return False
    amount = min(shortfall, pockets[donor])
    try:
        # Mismo bug de unidades que en rebalance_pockets_explicitly: `amount`
        # esta en DOLARES (pockets viene de get_balance() en dolares) pero
        # transfer_between_pockets espera CENTAVOS. Sin el x100 esto llevaba
        # desde el 15 oct transfiriendo 100 veces menos de lo pedido.
        trading_client.transfer_between_pockets(donor, pocket, amount * 100)
        print(f"[info] transferencia pedida: ${amount:.2f} del bolsillo {donor} al {pocket}")
        pockets[donor] -= amount
        pockets[pocket] = pockets.get(pocket, 0.0) + amount
        return True
    except Exception as exc:
        print(f"[warn] no se pudo transferir del bolsillo {donor} al {pocket}: {str(exc)[:200]}")
        return False


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
             AND ticker = ANY(%s) AND env = %s
             AND id IN (SELECT MAX(id) FROM kalshi_trades WHERE env = %s GROUP BY ticker)""",
        (held, BOT_ENV, BOT_ENV),
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
ALTER TABLE kalshi_trades ADD COLUMN IF NOT EXISTS env TEXT NOT NULL DEFAULT 'demo';
CREATE TABLE IF NOT EXISTS bot_state (
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("falta DATABASE_URL en el entorno")
    return psycopg2.connect(database_url)


def entries_scan_due(cur) -> bool:
    """True solo si ya pasaron ENTRY_SCAN_INTERVAL_MINUTES desde la ultima vez
    que esta corrida (cualquier instancia del cron) busco entradas nuevas.
    Si da luz verde, marca el intento de una vez (misma transaccion del
    llamador) para que las corridas de los proximos ~60 min se salten la
    busqueda, aunque el cron de Render siga disparando cada 15 min para
    vigilar las posiciones abiertas."""
    cur.execute(
        """
        SELECT 1 FROM bot_state
        WHERE key = 'last_entry_scan'
          AND updated_at >= now() - (%s * interval '1 minute')
        """,
        (ENTRY_SCAN_INTERVAL_MINUTES,),
    )
    if cur.fetchone():
        return False
    mark_entry_scan_done(cur)
    return True


def mark_entry_scan_done(cur) -> None:
    """Reinicia el reloj de ENTRY_SCAN_INTERVAL_MINUTES a partir de ahora."""
    cur.execute(
        """
        INSERT INTO bot_state (key, value, updated_at) VALUES ('last_entry_scan', 'ok', now())
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """
    )


def has_new_game(public_client: KalshiClient, series_tickers: list[str]) -> bool:
    """True si hay al menos un mercado (en alguna de las series ya permitidas)
    que abrio para operar hace menos de NEW_GAME_WINDOW_MINUTES -- senal de
    que un partido acaba de arrancar. Se usa para adelantar la busqueda de
    entradas sin esperar a que pase la hora completa."""
    now = datetime.now(timezone.utc)
    for series in series_tickers:
        try:
            for m in public_client.get_open_markets_for_series(series):
                open_dt = _parse_kalshi_time(m.get("open_time"))
                if open_dt is None:
                    continue
                minutes_open = (now - open_dt).total_seconds() / 60.0
                if 0 <= minutes_open <= NEW_GAME_WINDOW_MINUTES:
                    return True
        except Exception as exc:
            print(f"[warn] no se pudo revisar partidos nuevos en {series}: {exc}")
    return False


def print_pnl_summary(cur) -> None:
    """Resumen de ganancias/perdidas cerradas, por estado y por serie, para
    diagnosticar por que hay mas perdidas que ganancias. Solo lectura, se
    imprime siempre al inicio de la corrida (env=BOT_ENV)."""
    cur.execute(
        """
        SELECT status, COUNT(*), COALESCE(SUM(pnl_cents), 0), COALESCE(AVG(pnl_cents), 0)
        FROM kalshi_trades WHERE env = %s AND status != 'open'
        GROUP BY status ORDER BY status
        """,
        (BOT_ENV,),
    )
    rows = cur.fetchall()
    if not rows:
        print(f"[stats] sin trades cerrados todavia en env={BOT_ENV}")
        return
    total_n = sum(r[1] for r in rows)
    total_pnl = sum(r[2] for r in rows)
    print(f"[stats] === resumen de P&L (env={BOT_ENV}): {total_n} trades cerrados, pnl total {total_pnl}c ===")
    for status, n, total, avg in rows:
        print(f"[stats]   {status}: {n} trades, pnl total {total}c, promedio {avg:.1f}c/trade")

    cur.execute(
        """
        SELECT split_part(ticker, '-', 1) AS serie, COUNT(*),
               COALESCE(SUM(pnl_cents), 0) AS pnl_total,
               SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN pnl_cents <= 0 THEN 1 ELSE 0 END) AS losses
        FROM kalshi_trades WHERE env = %s AND status != 'open'
        GROUP BY serie ORDER BY pnl_total ASC
        """,
        (BOT_ENV,),
    )
    print(f"[stats] por serie (peor a mejor):")
    for serie, n, total, wins, losses in cur.fetchall():
        print(f"[stats]   {serie}: {n} trades ({wins}W/{losses}L), pnl total {total}c")


def recent_realized_pnl_cents(cur) -> int:
    """Ganancia/perdida realizada en las ultimas LOSS_WINDOW_HOURS horas."""
    cur.execute(
        """
        SELECT COALESCE(SUM(pnl_cents), 0) FROM kalshi_trades
        WHERE status != 'open' AND env = %s
          AND closed_at >= now() - (%s * interval '1 hour')
        """,
        (BOT_ENV, LOSS_WINDOW_HOURS),
    )
    return cur.fetchone()[0]


def event_of(ticker: str) -> str:
    """KXAPFDDHGAME-26OCT03RECLIB-REC -> KXAPFDDHGAME-26OCT03RECLIB (el partido)."""
    return ticker.rsplit("-", 1)[0]


def _parse_kalshi_time(raw) -> datetime | None:
    """Parsea un timestamp ISO8601 que viene de la API de Kalshi (open_time,
    close_time, etc), con o sin sufijo 'Z'. None si no se pudo parsear."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def game_recently_traded(cur, ticker: str) -> bool:
    """True si ya no se puede entrar mas en este partido, por cualquiera de
    estos dos motivos:
    1. Se alcanzo MAX_ENTRIES_PER_GAME contando TODAS las operaciones
       (abiertas o ya cerradas) en cualquier mercado de ese partido.
    2. La ULTIMA operacion cerrada de ese partido no cerro en ganancia
       (pnl_cents <= 0). La cadena de "montar y liquidar" solo sigue
       mientras cada paso va ganando; si una cierra en perdida o en cero,
       no se monta la siguiente en ese mismo partido (tenis y futbol
       americano por igual).
    La entrada simultanea a los dos lados (o una segunda mientras la
    primera sigue abierta) la bloquea por separado el chequeo de 'busy'
    por evento en look_for_entries."""
    event_pattern = event_of(ticker) + "-%"

    cur.execute(
        "SELECT COUNT(*) FROM kalshi_trades WHERE ticker LIKE %s AND env = %s",
        (event_pattern, BOT_ENV),
    )
    if cur.fetchone()[0] >= MAX_ENTRIES_PER_GAME:
        return True

    cur.execute(
        """
        SELECT pnl_cents FROM kalshi_trades
        WHERE ticker LIKE %s AND env = %s AND status != 'open' AND pnl_cents IS NOT NULL
        ORDER BY closed_at DESC
        LIMIT 1
        """,
        (event_pattern, BOT_ENV),
    )
    row = cur.fetchone()
    if row is not None and row[0] <= 0:
        return True

    return False


def entries_last_hour(cur) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM kalshi_trades WHERE env = %s AND opened_at >= now() - interval '1 hour'",
        (BOT_ENV,),
    )
    return cur.fetchone()[0]


def hourly_cap_reached(cur) -> bool:
    return MAX_ENTRIES_PER_HOUR > 0 and entries_last_hour(cur) >= MAX_ENTRIES_PER_HOUR


def open_positions_count(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM kalshi_trades WHERE status = 'open' AND env = %s", (BOT_ENV,))
    return cur.fetchone()[0]


def already_holds(cur, ticker: str) -> bool:
    cur.execute("SELECT 1 FROM kalshi_trades WHERE ticker = %s AND status = 'open' AND env = %s", (ticker, BOT_ENV))
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
           WHERE status = 'closed_settled' AND pnl_cents IS NULL AND env = %s
             AND closed_at >= now() - (%s * interval '1 hour')
           ORDER BY closed_at DESC LIMIT %s""",
        (BOT_ENV, LOSS_WINDOW_HOURS, max_rows),
    )
    for trade_id, ticker, count, entry_price in cur.fetchall():
        pnl = settlement_pnl_cents(public_client, ticker, entry_price, count)
        if pnl is None:
            continue
        cur.execute("UPDATE kalshi_trades SET pnl_cents = %s WHERE id = %s", (pnl, trade_id))
        print(f"[info] pnl de liquidacion de {ticker} completado: {pnl}c")
        time.sleep(SERIES_PAUSE_SECONDS)


def manage_open_positions(cur, public_client: KalshiClient, trading_client: KalshiTradingClient) -> None:
    cur.execute("SELECT id, ticker, count, entry_price FROM kalshi_trades WHERE status = 'open' AND env = %s", (BOT_ENV,))
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

        forced = FORCE_CLOSE_ALL or ticker in FORCE_CLOSE
        if not (hit_take_profit or hit_stop_loss or forced):
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
        sell_price = current_bid_cents if (hit_take_profit and not forced) else max(1, current_bid_cents - SL_SLIPPAGE_CENTS)
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

        status = "closed_manual" if forced else ("closed_tp" if hit_take_profit else "closed_sl")
        cur.execute(
            """UPDATE kalshi_trades
               SET status = %s, exit_price = %s, pnl_cents = COALESCE(pnl_cents, 0) + %s, closed_at = %s
               WHERE id = %s""",
            (status, exit_price, pnl_part, datetime.now(timezone.utc), trade_id),
        )
        print(f"[info] {ticker} cerrado ({status}) a {exit_price}c, pnl {pnl_part}c")


def _dollars_to_cents(value) -> float | None:
    try:
        return float(value) * 100.0
    except (TypeError, ValueError):
        return None


def protect_manual_positions(cur, public_client: KalshiClient, trading_client: KalshiTradingClient) -> None:
    """Vigila las posiciones YES de tu cuenta que el bot no abrio (las que
    pones a mano) y las vende en dos casos: si el precio cae MANUAL_STOP_CENTS
    por debajo de su costo promedio (proteccion), o si sube
    MANUAL_TAKE_PROFIT_CENTS por encima (toma de ganancia automatica)."""
    if not PROTECT_MANUAL:
        return
    # Se excluyen las posiciones abiertas del bot y tambien las que el bot
    # abrio o cerro hace poco: Kalshi tarda un rato en reflejar una venta y
    # sin esto se confundiria una posicion recien cerrada con una manual.
    cur.execute(
        """SELECT DISTINCT ticker FROM kalshi_trades
           WHERE env = %s AND (status = 'open'
                 OR opened_at >= now() - interval '30 minutes'
                 OR closed_at >= now() - interval '30 minutes')""",
        (BOT_ENV,),
    )
    bot_open = {r[0] for r in cur.fetchall()}
    try:
        data = trading_client._get("/portfolio/positions", {"limit": 1000})
    except Exception as exc:
        print(f"[warn] no se pudieron leer posiciones para vigilar las manuales: {exc}")
        return
    for p in data.get("market_positions") or []:
        ticker = p.get("ticker")
        if not ticker or ticker in bot_open:
            continue
        try:
            qty = float(p.get("position_fp", p.get("position", 0)) or 0)
        except (TypeError, ValueError):
            qty = 0.0
        if qty == 0:
            continue
        if 0 < qty < 1:
            continue  # restos fraccionarios que no se pueden vender
        if qty < 0:
            print(f"[info] posicion manual {ticker}: es del lado NO ({qty}); no se vigila")
            continue
        exposure_c = _dollars_to_cents(p.get("market_exposure_dollars"))
        if exposure_c is None or qty <= 0:
            print(f"[info] posicion manual {ticker}: sin costo en la API, no se puede vigilar. Datos: {str(p)[:300]}")
            continue
        avg_cost = exposure_c / qty
        try:
            market = public_client.get_market(ticker)
            market = market.get("market", market) if isinstance(market, dict) else {}
        except Exception as exc:
            print(f"[warn] posicion manual {ticker}: no se pudo leer el precio ({str(exc)[:100]})")
            continue
        bid_c = _dollars_to_cents(market.get("yes_bid_dollars"))
        if bid_c is None:
            continue
        bid_c = round(bid_c)
        print(f"[info] vigilando posicion manual {ticker}: {qty:g} contratos, costo medio {avg_cost:.0f}c, bid {bid_c}c")
        if bid_c <= 0:
            continue
        if bid_c <= avg_cost - MANUAL_STOP_CENTS:
            reason = "perdida"
        elif bid_c >= avg_cost + MANUAL_TAKE_PROFIT_CENTS:
            reason = "ganancia"
        else:
            continue
        sell_price = max(1, bid_c - SL_SLIPPAGE_CENTS)
        count = int(qty)
        if count <= 0:
            continue
        try:
            resp = trading_client.create_order(
                ticker=ticker, side="yes", action="sell", count=count, price_cents=sell_price, reduce_only=True,
            )
        except Exception as exc:
            print(f"[warn] no se pudo cerrar la posicion manual {ticker}: {exc}")
            continue
        filled, avg_exit = parse_fill(resp)
        if filled <= 0:
            print(f"[info] cierre de la posicion manual {ticker} no se lleno; se reintenta")
            continue
        exit_c = avg_exit if avg_exit is not None else sell_price
        pnl_aprox = round((exit_c - avg_cost) * filled)
        if reason == "perdida":
            print(f"[info] posicion manual {ticker} cerrada por proteccion: {filled} a {exit_c}c (costo medio {avg_cost:.0f}c, pnl aprox {pnl_aprox}c)")
        else:
            print(f"[info] posicion manual {ticker} cerrada por ganancia: {filled} a {exit_c}c (costo medio {avg_cost:.0f}c, pnl aprox {pnl_aprox}c)")


def sell_ride_to_win_positions(public_client: KalshiClient, trading_client: KalshiTradingClient) -> None:
    """Vende las posiciones puntuales de RIDE_TO_WIN_TICKERS en cuanto el bid
    llega a RIDE_TO_WIN_SELL_CENTS (cerca del pago maximo de $1). No aplica
    stop-loss ni el take-profit normal -- es solo para cobrar una apuesta que
    el usuario pidio explicitamente, aunque STRAT_PROTECT_MANUAL este en 0."""
    if not RIDE_TO_WIN_TICKERS:
        return
    try:
        data = trading_client._get("/portfolio/positions", {"limit": 1000})
    except Exception as exc:
        print(f"[warn] no se pudieron leer posiciones para RIDE_TO_WIN: {exc}")
        return
    positions_by_ticker = {p.get("ticker"): p for p in (data.get("market_positions") or [])}
    for ticker in RIDE_TO_WIN_TICKERS:
        p = positions_by_ticker.get(ticker)
        if p is None:
            continue
        try:
            qty = float(p.get("position_fp", p.get("position", 0)) or 0)
        except (TypeError, ValueError):
            qty = 0.0
        if qty < 1:
            continue
        try:
            market = public_client.get_market(ticker)
            market = market.get("market", market) if isinstance(market, dict) else {}
        except Exception as exc:
            print(f"[warn] RIDE_TO_WIN {ticker}: no se pudo leer el precio ({str(exc)[:100]})")
            continue
        bid_c = _dollars_to_cents(market.get("yes_bid_dollars"))
        if bid_c is None:
            continue
        bid_c = round(bid_c)
        print(f"[info] vigilando (cobrar al llegar al maximo) {ticker}: {qty:g} contratos, bid {bid_c}c, objetivo {RIDE_TO_WIN_SELL_CENTS}c")
        if bid_c < RIDE_TO_WIN_SELL_CENTS:
            continue
        count = int(qty)
        if count <= 0:
            continue
        sell_price = max(1, bid_c - SL_SLIPPAGE_CENTS)
        try:
            resp = trading_client.create_order(
                ticker=ticker, side="yes", action="sell", count=count, price_cents=sell_price, reduce_only=True,
            )
        except Exception as exc:
            print(f"[warn] no se pudo cobrar {ticker}: {exc}")
            continue
        filled, avg_exit = parse_fill(resp)
        if filled <= 0:
            print(f"[info] cobro de {ticker} no se lleno; se reintenta")
            continue
        exit_c = avg_exit if avg_exit is not None else sell_price
        print(f"[info] {ticker} cobrada al maximo: {filled} a {exit_c}c")


def sell_pct_target_positions(cur, public_client: KalshiClient, trading_client: KalshiTradingClient) -> None:
    """Vende posiciones manuales en cuanto el bid llegue a PCT_TARGET_PERCENT%
    de ganancia sobre el costo promedio (ej. 50% = vender cuando el precio
    sea 1.5x lo que se pago). Pensado para "yo compro a mano al precio que
    quiero, el bot vende solo al % que yo diga".

    Con PCT_TARGET_AUTO=0 (default) solo vigila los tickers puntuales de
    PCT_TARGET_TICKERS. Con PCT_TARGET_AUTO=1 vigila TODA posicion manual
    nueva automaticamente, sin que haga falta avisar el ticker.

    Sin stop-loss -- si se quiere proteccion ademas, se usa STRAT_PROTECT_MANUAL
    por separado. Independiente de ese flag."""
    if not PCT_TARGET_AUTO and not PCT_TARGET_TICKERS:
        return
    try:
        data = trading_client._get("/portfolio/positions", {"limit": 1000})
    except Exception as exc:
        print(f"[warn] no se pudieron leer posiciones para PCT_TARGET: {exc}")
        return
    positions_by_ticker = {p.get("ticker"): p for p in (data.get("market_positions") or [])}
    if PCT_TARGET_AUTO:
        cur.execute(
            """SELECT DISTINCT ticker FROM kalshi_trades
               WHERE env = %s AND (status = 'open'
                     OR opened_at >= now() - interval '30 minutes'
                     OR closed_at >= now() - interval '30 minutes')""",
            (BOT_ENV,),
        )
        bot_open = {r[0] for r in cur.fetchall()}
        target_tickers = [t for t in positions_by_ticker if t not in bot_open]
    else:
        target_tickers = PCT_TARGET_TICKERS
    for ticker in target_tickers:
        p = positions_by_ticker.get(ticker)
        if p is None:
            continue
        try:
            qty = float(p.get("position_fp", p.get("position", 0)) or 0)
        except (TypeError, ValueError):
            qty = 0.0
        if qty < 1:
            continue
        exposure_c = _dollars_to_cents(p.get("market_exposure_dollars"))
        if exposure_c is None or qty <= 0:
            print(f"[info] PCT_TARGET {ticker}: sin costo en la API, no se puede vigilar")
            continue
        avg_cost = exposure_c / qty
        target_c = min(99, avg_cost * (1 + PCT_TARGET_PERCENT / 100))
        try:
            market = public_client.get_market(ticker)
            market = market.get("market", market) if isinstance(market, dict) else {}
        except Exception as exc:
            print(f"[warn] PCT_TARGET {ticker}: no se pudo leer el precio ({str(exc)[:100]})")
            continue
        bid_c = _dollars_to_cents(market.get("yes_bid_dollars"))
        if bid_c is None:
            continue
        bid_c = round(bid_c)
        print(f"[info] vigilando (objetivo {PCT_TARGET_PERCENT}% ganancia) {ticker}: {qty:g} contratos, costo medio {avg_cost:.0f}c, bid {bid_c}c, objetivo {target_c:.0f}c")
        if bid_c < target_c:
            continue
        count = int(qty)
        if count <= 0:
            continue
        sell_price = max(1, bid_c - SL_SLIPPAGE_CENTS)
        try:
            resp = trading_client.create_order(
                ticker=ticker, side="yes", action="sell", count=count, price_cents=sell_price, reduce_only=True,
            )
        except Exception as exc:
            print(f"[warn] no se pudo cobrar {ticker} (objetivo %): {exc}")
            continue
        filled, avg_exit = parse_fill(resp)
        if filled <= 0:
            print(f"[info] cobro de {ticker} (objetivo %) no se lleno; se reintenta")
            continue
        exit_c = avg_exit if avg_exit is not None else sell_price
        pnl_aprox = round((exit_c - avg_cost) * filled)
        print(f"[info] {ticker} cobrada al {PCT_TARGET_PERCENT}% de ganancia: {filled} a {exit_c}c (costo medio {avg_cost:.0f}c, pnl aprox {pnl_aprox}c)")


def look_for_entries(conn, cur, public_client: KalshiClient, trading_client: KalshiTradingClient, series_tickers: list[str]) -> None:
    busy = kalshi_busy_tickers(trading_client)
    if busy is None:
        print("[info] sin datos de Kalshi sobre tus posiciones; no se abre nada nuevo en esta corrida")
        return

    def total_open() -> int:
        cur.execute("SELECT ticker FROM kalshi_trades WHERE status = 'open' AND env = %s", (BOT_ENV,))
        return len(busy | {r[0] for r in cur.fetchall()})

    # El reparto objetivo de saldo se pide SIEMPRE, incluso si esta corrida no
    # va a buscar entradas nuevas (limite de posiciones, de entradas por hora,
    # o de perdida). Antes esto estaba despues de esos "return" tempranos, asi
    # que si el bot estaba lleno de posiciones (lo normal con 7 abiertas de
    # limite) el rebalanceo nunca se llegaba a pedir. Pidiendolo aqui arriba,
    # Kalshi ya va dejando el saldo repartido mientras el bot espera a que se
    # libere un cupo, en vez de recien reaccionar cuando ya hace falta comprar.
    pockets: dict[int, float] | None = None
    if POCKET_CHECK:
        try:
            bd = trading_client.get_balance().get("balance_breakdown") or []
            pockets = {int(b["exchange_index"]): float(b["balance"]) for b in bd}
            print("[info] saldo por bolsillo: " + ", ".join(f"{k}=${v:.2f}" for k, v in sorted(pockets.items())))
            if AUTO_REBALANCE:
                ensure_even_target_allocation(trading_client, pockets)
                rebalance_pockets_explicitly(trading_client, pockets)
                try:
                    current_target = trading_client.get_target_balance_allocation()
                    print(f"[info] reparto objetivo que Kalshi tiene guardado ahora: {current_target}")
                except Exception as texc:
                    print(f"[warn] no se pudo leer el reparto objetivo guardado: {str(texc)[:200]}")
                try:
                    resting = trading_client.get_resting_orders().get("orders") or []
                    held_by_pocket: dict[int, float] = {}
                    for o in resting:
                        idx = int(o.get("exchange_index", -1))
                        held_by_pocket[idx] = held_by_pocket.get(idx, 0.0) + float(o.get("yes_price", o.get("no_price", 0)) or 0) * int(o.get("remaining_count", o.get("count", 0)) or 0) / 100
                    print(f"[info] {len(resting)} ordenes resting; saldo retenido estimado por bolsillo: {held_by_pocket}")
                except Exception as rexc:
                    print(f"[warn] no se pudo calcular saldo retenido por ordenes resting: {str(rexc)[:200]}")
        except Exception as bexc:
            print(f"[warn] no se pudo leer el saldo por bolsillo ({str(bexc)[:120]}); se intenta comprar sin filtrar")

    if total_open() >= MAX_OPEN_POSITIONS:
        print("[info] maximo de posiciones abiertas alcanzado, no se buscan nuevas entradas")
        return
    if hourly_cap_reached(cur):
        print(f"[info] ya hubo {MAX_ENTRIES_PER_HOUR} entrada(s) en la ultima hora; no se abren mas hasta que pase")
        return
    # Antes esto cortaba TODAS las entradas nuevas hasta que la ventana de
    # perdida se enfriara. Ahora, en vez de parar, se sigue comprando pero
    # con tamano reducido (REDUCED_CONTRACTS) -- asi se consume mas lento el
    # limite y se sigue operando. En cuanto recent_realized_pnl_cents() deje
    # de estar por debajo del limite (las perdidas viejas salen solas de la
    # ventana movil de LOSS_WINDOW_HOURS), esto vuelve a FIXED_CONTRACTS sin
    # que haga falta ningun temporizador aparte.
    contracts_this_run = FIXED_CONTRACTS
    if recent_realized_pnl_cents(cur) <= -DAILY_LOSS_LIMIT_CENTS:
        contracts_this_run = REDUCED_CONTRACTS
        print(f"[info] limite de perdida alcanzado en las ultimas {LOSS_WINDOW_HOURS} h; se sigue comprando pero con tamano reducido ({REDUCED_CONTRACTS} contratos) hasta que esa ventana se enfrie")

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

    skipped_pockets: set[int] = set()
    insufficient_fails = 0
    for market in iter_markets():
        if total_open() >= MAX_OPEN_POSITIONS:
            print("[info] maximo de posiciones abiertas alcanzado")
            break
        if hourly_cap_reached(cur):
            print(f"[info] limite de {MAX_ENTRIES_PER_HOUR} entrada(s) por hora alcanzado")
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

        pocket = market.get("exchange_index")
        if pockets is not None and pocket is not None:
            pocket = int(pocket)
            cost = contracts_this_run * yes_ask_cents / 100.0
            if pockets.get(pocket, 0.0) < cost + 0.05:
                moved = POCKET_AUTOFUND and autofund_pocket(trading_client, pockets, pocket, cost + 0.05)
                if moved:
                    # La transferencia recien pedida es asincrona del lado de
                    # Kalshi (tarda ~10s en reflejarse de verdad). Comprar
                    # ahora mismo, en la misma corrida, todavia pega contra el
                    # saldo viejo y rebota con insufficient_balance -- eso es
                    # justo lo que se veia en los logs: se gastaban los 3
                    # intentos de la corrida entera en esto y se cortaba la
                    # busqueda para TODOS los bolsillos, no solo el corto.
                    # En vez de intentar comprar igual, se omite este mercado
                    # y se sigue con el resto; la proxima corrida (15 min
                    # despues, de sobra para que la transferencia se asiente)
                    # ya va a tener el saldo real.
                    if pocket not in skipped_pockets:
                        skipped_pockets.add(pocket)
                        print(f"[info] bolsillo {pocket} sin saldo suficiente; transferencia pedida, se reintenta en la proxima corrida")
                    continue
                if pockets.get(pocket, 0.0) < cost + 0.05:
                    if pocket not in skipped_pockets:
                        skipped_pockets.add(pocket)
                        print(f"[info] bolsillo {pocket} sin saldo suficiente (${pockets.get(pocket, 0.0):.2f}); se omiten sus mercados")
                    continue

        try:
            resp = trading_client.create_order(
                ticker=ticker, side="yes", action="buy", count=contracts_this_run, price_cents=yes_ask_cents,
                time_in_force="immediate_or_cancel",  # compra lo que haya a ese precio; nada queda colgado
            )
        except Exception as exc:
            if "insufficient_balance" in str(exc):
                try:
                    bal = trading_client.get_balance()
                except Exception as bexc:
                    bal = f"no se pudo leer ({bexc})"
                print(f"[info] saldo insuficiente en Kalshi al comprar {ticker} ({contracts_this_run} x {yes_ask_cents}c); no se buscan mas compras en esta corrida")
                print(f"[info] detalle del error: {str(exc)[:300]}")
                print(f"[info] balance segun la API: {bal}")
                if insufficient_fails == 0:
                    try:
                        ro = trading_client.get_resting_orders()
                        orders = ro.get("orders", [])
                        print(f"[info] ordenes pendientes en Kalshi: {len(orders)}")
                        for o in orders[:10]:
                            print(f"[info]   pendiente: {str(o)[:300]}")
                    except Exception as oexc:
                        print(f"[info] no se pudieron leer ordenes pendientes: {str(oexc)[:200]}")
                insufficient_fails += 1
                if insufficient_fails >= 3:
                    print("[info] 3 rechazos por saldo seguidos; no se buscan mas compras en esta corrida")
                    break
                continue
            print(f"[warn] no se pudo comprar {ticker}: {exc}")
            continue

        # Solo se registra lo que de verdad se compro, al precio real.
        filled, avg_cents = parse_fill(resp)
        if filled <= 0:
            print(f"[info] {ticker}: no habia vendedores a {yes_ask_cents}c; no se compro nada")
            continue
        filled = min(filled, contracts_this_run)
        if pockets is not None and pocket is not None:
            pockets[int(pocket)] = pockets.get(int(pocket), 0.0) - filled * (avg_cents if avg_cents is not None else yes_ask_cents) / 100.0
        entry = avg_cents if avg_cents is not None else yes_ask_cents
        cur.execute(
            """INSERT INTO kalshi_trades (ticker, side, count, entry_price, status, env)
               VALUES (%s, 'yes', %s, %s, 'open', %s)""",
            (ticker, filled, entry, BOT_ENV),
        )
        conn.commit()  # se guarda en el acto: si la corrida falla despues, no se pierde
        busy.add(ticker)
        print(f"[info] entro en {ticker} a {entry}c x{filled}")


def look_for_comeback_bets(conn, cur, public_client: KalshiClient, trading_client: KalshiTradingClient) -> None:
    """Apuesta de rescate en futbol americano: un equipo cotiza carisimo
    (mercado lo da casi por ganador) y el rival cotiza baratisimo, con el
    cierre del mercado cerca -- la foto de un equipo perdiendo por poco con
    poco tiempo en el reloj. Compra el lado barato por RESCUE_DOLLARS fijos.
    No depende de entries_scan_due(): se revisa en CADA corrida (cada 15 min)
    porque la ventana real puede ser de minutos."""
    if not RESCUE_ENABLED:
        return

    busy = kalshi_busy_tickers(trading_client)
    if busy is None:
        return

    now = datetime.now(timezone.utc)
    events: dict[str, list[dict]] = {}
    for series in RESCUE_SERIES:
        if not series_allowed(series):
            continue
        try:
            for m in public_client.get_open_markets_for_series(series):
                events.setdefault(event_of(m.get("ticker", "")), []).append(m)
        except Exception as exc:
            print(f"[warn] rescate: no se pudo leer la serie {series}: {exc}")

    for event, markets in events.items():
        if len(markets) < 2:
            continue
        if any(event_of(b) == event for b in busy):
            continue  # ya hay una posicion abierta en este partido

        expensive_ok = any(
            (m.get("yes_ask_dollars") is not None and round(float(m["yes_ask_dollars"]) * 100) >= RESCUE_EXPENSIVE_MIN_CENTS)
            for m in markets
        )
        if not expensive_ok:
            continue

        for market in markets:
            ticker = market.get("ticker")
            if not ticker or ticker in busy or already_holds(cur, ticker) or game_recently_traded(cur, ticker):
                continue

            yes_ask = market.get("yes_ask_dollars")
            if yes_ask is None:
                continue
            yes_ask_cents = round(float(yes_ask) * 100)
            if yes_ask_cents <= 0 or yes_ask_cents > RESCUE_CHEAP_MAX_CENTS:
                continue

            close_dt = _parse_kalshi_time(market.get("close_time"))
            if close_dt is None:
                continue
            minutes_to_close = (close_dt - now).total_seconds() / 60.0
            if not (0 <= minutes_to_close <= RESCUE_CLOSE_WINDOW_MINUTES):
                continue

            count = int((RESCUE_DOLLARS * 100) // yes_ask_cents)
            if count < 1:
                continue

            try:
                resp = trading_client.create_order(
                    ticker=ticker, side="yes", action="buy", count=count, price_cents=yes_ask_cents,
                    time_in_force="immediate_or_cancel",
                )
            except Exception as exc:
                print(f"[warn] rescate: no se pudo comprar {ticker}: {exc}")
                continue

            filled, avg_cents = parse_fill(resp)
            if filled <= 0:
                print(f"[info] rescate: {ticker}: no habia vendedores a {yes_ask_cents}c; no se compro nada")
                continue
            filled = min(filled, count)
            entry = avg_cents if avg_cents is not None else yes_ask_cents
            cur.execute(
                """INSERT INTO kalshi_trades (ticker, side, count, entry_price, status, env)
                   VALUES (%s, 'yes', %s, %s, 'open', %s)""",
                (ticker, filled, entry, BOT_ENV),
            )
            conn.commit()
            busy.add(ticker)
            print(f"[info] RESCATE: entro en {ticker} a {entry}c x{filled} (rival cotizaba >= {RESCUE_EXPENSIVE_MIN_CENTS}c, cierra en {minutes_to_close:.1f} min)")
            break  # un solo lado por partido en esta corrida


def main() -> None:
    print(f"[info] ambiente: {BOT_ENV} (KALSHI_ENV={os.environ.get('KALSHI_ENV', 'demo')!r})")
    all_series = resolve_series(os.environ.get("KALSHI_SERIES", "ALL_SPORTS"))
    series_tickers = [s for s in all_series if series_allowed(s)]
    skipped = [s for s in all_series if not series_allowed(s)]
    if USER_EXCLUDED_WORDS:
        print(f"[info] {len(skipped)} series excluidas por palabra; quedan {len(series_tickers)}")

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
            print_pnl_summary(cur)

        if CANCEL_ALL_ORDERS:
            cancel_all_resting_orders(trading_client)

        with conn.cursor() as cur:
            reopen_wrongly_settled(conn, cur, trading_client)

        with conn.cursor() as cur:
            manage_open_positions(cur, public_client, trading_client)
            conn.commit()

        with conn.cursor() as cur:
            protect_manual_positions(cur, public_client, trading_client)

        sell_ride_to_win_positions(public_client, trading_client)
        with conn.cursor() as cur:
            sell_pct_target_positions(cur, public_client, trading_client)

        with conn.cursor() as cur:
            backfill_settled_pnl(cur, public_client)
            conn.commit()

        if ENTRIES_PAUSED:
            print("[info] STRAT_ENTRIES_PAUSED activo: no se abren posiciones nuevas (las abiertas siguen vigiladas)")
        else:
            # La apuesta de rescate se revisa SIEMPRE (cada 15 min), no solo
            # cuando toca la busqueda horaria normal -- la ventana de "equipo
            # perdiendo con poco tiempo" puede durar pocos minutos.
            with conn.cursor() as cur:
                look_for_comeback_bets(conn, cur, public_client, trading_client)
                conn.commit()

            with conn.cursor() as cur:
                due = entries_scan_due(cur)
                conn.commit()
            if not due:
                # Aunque no haya pasado la hora completa, si un partido acaba
                # de arrancar se adelanta la busqueda igual -- ahi suele haber
                # mas oportunidades de precio que esperando a que pase la hora.
                if has_new_game(public_client, series_tickers):
                    print(f"[info] partido nuevo detectado (abrio hace <= {NEW_GAME_WINDOW_MINUTES} min); se adelanta la busqueda de entradas sin esperar a que pase la hora")
                    with conn.cursor() as cur:
                        mark_entry_scan_done(cur)
                        look_for_entries(conn, cur, public_client, trading_client, series_tickers)
                        conn.commit()
                else:
                    print(f"[info] todavia no pasan los {ENTRY_SCAN_INTERVAL_MINUTES} min desde la ultima busqueda de entradas nuevas; esta corrida solo vigilo las posiciones abiertas")
            else:
                with conn.cursor() as cur:
                    look_for_entries(conn, cur, public_client, trading_client, series_tickers)
                    conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
