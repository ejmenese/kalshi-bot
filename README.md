# kalshi-bot

Monitor automático de mercados deportivos de Kalshi (tenis, fútbol, béisbol),
corriendo en Render.

- **poll_markets.py** (Cron Job, cada 15 min): descubre los mercados
  actualmente abiertos de cada serie en `KALSHI_SERIES` y guarda un snapshot
  de cada uno en Postgres. No usa tickers fijos — cada corrida vuelve a
  preguntarle a Kalshi qué partidos están abiertos ahora, así el monitor
  sigue vivo aunque los partidos de hoy ya hayan cerrado.
- **webservice.py** (Web Service): expone lo último guardado vía HTTP.
- **kalshi_client.py**: cliente HTTP a la API pública de Kalshi (sin
  necesidad de credenciales — los datos de mercado son públicos).

## Variables de entorno

- `DATABASE_URL` — connection string de Postgres (Render la inyecta si
  conectas la base al servicio).
- `KALSHI_SERIES` — series a vigilar, separadas por coma. Por defecto:
  `KXMLBGAME,KXATPMATCH,KXWTAMATCH,KXEPLGAME`
- `KALSHI_ENV` — opcional, `demo` para probar contra el sandbox de Kalshi.

## Endpoints del Web Service

- `GET /health`
- `GET /markets` — último snapshot de cada mercado abierto
- `GET /markets/series/<series_ticker>` — filtrado por serie (ej. `KXMLBGAME`)
- `GET /markets/<ticker>/history?limit=200` — historial de un mercado
