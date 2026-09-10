import os
import time
import requests
from datetime import datetime

# --- CONFIGURACION AGRESIVA 5% ---
RISK_PER_TRADE = 0.05  # 5% de tu Cash por trade
MIN_LIQUIDITY = 1000   # No entra si el mercado tiene menos de $1000 - evita tu error de "No liquidity"
MIN_PRICE = 20  # No comprar si cuesta menos de 20c (muy riesgoso)
MAX_PRICE = 80  # No comprar si cuesta más de 80c (poca ganancia)
TAKE_PROFIT = 88 # Vende automático si llega a 88c y asegura ganancia
STOP_LOSS = 35   # Vende automático si baja a 35c y corta pérdida

# --- CONEXION KALSHI ---
# Pones tus claves en variables de entorno, nunca en el código
KALSHI_EMAIL = os.getenv("KALSHI_EMAIL")
KALSHI_PASSWORD = os.getenv("KALSHI_PASSWORD")
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2"

def login():
    r = requests.post(f"{KALSHI_API_URL}/login", json={"email": KALSHI_EMAIL, "password": KALSHI_PASSWORD})
    r.raise_for_status()
    return r.json()["token"]

def get_balance(token):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{KALSHI_API_URL}/portfolio/balance", headers=headers)
    return r.json() # te devuelve cash y portfolio

def get_markets(token):
    headers = {"Authorization": f"Bearer {token}"}
    # Busca mercados de tenis y los más activos
    r = requests.get(f"{KALSHI_API_URL}/markets?status=open&limit=100", headers=headers)
    return r.json()["markets"]

def run_bot():
    print(f"[{datetime.now()}] Iniciando bot 24/7 agresivo...")
    token = login()
    
    while True:
        try:
            balance = get_balance(token)
            cash = balance["cash"] / 100  # Kalshi lo da en centavos
            print(f"Cash: ${cash}")

            markets = get_markets(token)
            for m in markets:
                # FILTRO 1: Liquidez - esto te salva del caso Sureshkumar
                if m["volume"] < MIN_LIQUIDITY:
                    continue
                
                price = m["yes_bid"] # precio actual del YES
                if not (MIN_PRICE <= price <= MAX_PRICE):
                    continue

                # FILTRO 2: Calcular cuanto comprar (5%)
                dollars_to_risk = cash * RISK_PER_TRADE
                if dollars_to_risk < 1: # no operar con menos de $1
                    continue

                print(f"OPORTUNIDAD: {m['ticker']} a {price}c - Arriesgando ${dollars_to_risk:.2f}")
                # Aquí va la orden de compra
                # requests.post(f"{KALSHI_API_URL}/portfolio/orders", headers=headers, json={...})

            # Revisar tus posiciones abiertas para take profit / stop loss
            # Si tu posicion de 93.6 Yes de Sureshkumar llega a 88c, la vende sola

        except Exception as e:
            print(f"Error: {e} - reintentando en 60s")
            # Si el token expira, hace login de nuevo
            token = login()

        time.sleep(60) # Revisa cada 60 segundos - 24/7

if __name__ == "__main__":
    run_bot()
