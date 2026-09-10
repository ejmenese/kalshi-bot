"""Web service minimo que expone lo ultimo guardado por poll_markets.py.
Endpoints:
  GET /health                     -> estado de la conexion a la base
  GET /markets                    -> ultimo snapshot de cada ticker abierto
  GET /markets/series/<series>    -> ultimo snapshot de una serie (p. ej. KXMLBGAME)
  GET /markets/<ticker>/history   -> historial de un ticker especifico
"""

from __future__ import annotations

import os

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request

app = Flask(__name__)


def get_conn():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("falta DATABASE_URL en el entorno del servicio")
    return psycopg2.connect(database_url)


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify(status="ok")
    except Exception as exc:
        return jsonify(status="error", detail=str(exc)), 503


@app.get("/markets")
def latest_markets():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (ticker) *
                FROM kalshi_snapshots
                ORDER BY ticker, fetched_at DESC
                """
            )
            rows = cur.fetchall()
        return jsonify(markets=rows)
    finally:
        conn.close()


@app.get("/markets/series/<series_ticker>")
def latest_markets_by_series(series_ticker: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (ticker) *
                FROM kalshi_snapshots
                WHERE series_ticker = %s
                ORDER BY ticker, fetched_at DESC
                """,
                (series_ticker,),
            )
            rows = cur.fetchall()
        return jsonify(series_ticker=series_ticker, markets=rows)
    finally:
        conn.close()


@app.get("/markets/<ticker>/history")
def market_history(ticker: str):
    limit = min(int(request.args.get("limit", 200)), 2000)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM kalshi_snapshots
                WHERE ticker = %s
                ORDER BY fetched_at DESC
                LIMIT %s
                """,
                (ticker, limit),
            )
            rows = cur.fetchall()
        if not rows:
            return jsonify(error=f"sin datos para el ticker '{ticker}' todavia"), 404
        return jsonify(ticker=ticker, snapshots=rows)
    finally:
        conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
