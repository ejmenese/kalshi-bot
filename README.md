# kalshi-bot

Monitor automático de mercados deportivos de Kalshi (tenis, fútbol, béisbol),
corriendo en Render, con un bot de trading opcional sobre los mismos datos.

## Componentes

- **poll_markets.py** (Cron Job, cada 15 min): descubre los mercados
  actualmente abiertos de cada serie en `KALSHI_SERIES` y guarda un snapshot
  de cada uno en Postgres. No usa tickers fijos — cada corrida vuelve a
  preguntarle a Kalshi qué partidos están abiertos ahora, así el monitor
  sigue vivo aunque los partidos de hoy ya hayan cerrado.
- **trade_bot.py** (Cron Job, cada 15 min): aplica la estrategia sobre los
  mercados abiertos y coloca órdenes. Requiere credenciales de trading.
- **webservice.py** (Web Service): expone lo último guardado vía HTTP.
- **kalshi_client.py**: cliente HTTP a la API pública de Kalshi (sin
  necesidad de credenciales — los datos de mercado son públicos).
- **kalshi_trading_client.py**: cliente autenticado (firma RSA-PSS) para
  leer balance/posiciones y colocar órdenes.

## Variables de entorno

- `DATABASE_URL` — connection string de Postgres (Render la inyecta si
  conectas la base al servicio).
- `KALSHI_SERIES` — series a vigilar, separadas por coma. Por defecto:
  `KXMLBGAME,KXATPMATCH,KXWTAMATCH,KXEPLGAME`
- `KALSHI_ENV` — `demo` (default) o `prod`. Decide contra qué ambiente de
  Kalshi opera el bot de trading.

Credenciales de trading (nunca en el repo, solo como variables de entorno
en Render):

- Ambiente demo: `KALSHI_API_KEY_ID` y `KALSHI_PRIVATE_KEY_PEM`
- Ambiente producción: `KALSHI_PROD_API_KEY_ID` y `KALSHI_PROD_PRIVATE_KEY_PEM`

Las llaves de demo y de producción no son intercambiables: cada ambiente
tiene su propio par generado en la cuenta correspondiente.

## Hosts de la API

- Producción: `https://external-api.kalshi.com/trade-api/v2`
- Demo: `https://external-api.demo.kalshi.co/trade-api/v2`

## Endpoints del Web Service

- `GET /health`
- `GET /balance` — balance de la cuenta (requiere credenciales)
- `GET /markets` — último snapshot de cada mercado abierto
- `GET /markets/series/<series_ticker>` — filtrado por serie (ej. `KXMLBGAME`)
- `GET /markets/<ticker>/history?limit=200` — historial de un mercado
- `GET /trades` — operaciones registradas por el bot
- 
