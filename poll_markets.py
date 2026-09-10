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

from kalshi_client import KalshiClient, normalize_snapshot

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
    series_raw = os.environ.get("KALSHI_SERIES", "")
    series_tickers = [s.strip() for s in series_raw.split(",") if s.strip()]
    if not series_tickers:
        raise RuntimeError("falta KALSHI_SERIES (series separadas por coma) en el entorno")

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
