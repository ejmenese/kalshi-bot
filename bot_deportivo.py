import time
import requests
from kalshi_python import TradingClient  # pip install kalshi-python

# CONFIGURACION 5% AGRESIVO
TAKE_PROFIT = 0.25  # vende cuando ganes 25% sin esperar que acabe el juego
STOP_LOSS = 0.15    # vende si pierdes 15%
TRADE_SIZE_PCT = 0.05  # 5% de tu balance por trade

DEPORTES = {
    "TENIS": ["KXTENNIS", "ATP", "WTA"],
    "FUTBOL_AMERICANO": ["KXNFL", "KXCFB", "NFL"],
    "BEISBOL": ["KXMLB", "MLB"],
    "PELEAS": ["KXUFC", "KXMMA", "UFC"]
}

class BotDeportivo:
    def __init__(self, email, password):
        self.client = TradingClient(email, password)
        self.client.login()
        print("Login OK - Balance:", self.get_balance())

    def get_balance(self):
        bal = self.client.get_balance()
        return bal['balance'] / 100  # Kalshi viene en centavos

    def buscar_mercados_deporte(self):
        mercados = self.client.get_markets()
        activos = []
        for m in mercados['markets']:
            ticker = m['ticker']
            for deporte, keywords in DEPORTES.items():
                if any(k in ticker for k in keywords) and m['status'] == 'open':
                    # Solo mercados con volumen
                    if m.get('volume', 0) > 100:
                        m['deporte'] = deporte
                        activos.append(m)
        return activos

    def comprar(self, market, lado="yes"):
        balance = self.get_balance()
        if balance < 1:
            print("Saldo 0, no puedo comprar")
            return
        
        monto = balance * TRADE_SIZE_PCT
        # Precio actual
        precio = market['yes_bid'] if lado == "yes" else market['no_bid']
        cantidad = int(monto / (precio/100))
        
        if cantidad < 1: cantidad = 1
        
        print(f"COMPRANDO {market['deporte']} | {market['ticker']} | {lado} @ {precio}c x {cantidad}")
        order = self.client.create_order(
            ticker=market['ticker'],
            side=lado,
            count=cantidad,
            type='limit',
            yes_price=precio
        )
        return order

    def checar_ganancia_y_vender(self):
        posiciones = self.client.get_positions()
        for pos in posiciones['positions']:
            # Ganancia no realizada
            costo = pos['total_traded'] / 100
            valor_actual = pos['market_value'] / 100
            if costo == 0: continue
            
            ganancia_pct = (valor_actual - costo) / costo
            
            # VENTA ANTICIPADA - no espera final del juego
            if ganancia_pct >= TAKE_PROFIT:
                print(f"TAKE PROFIT {ganancia_pct*100:.1f}% en {pos['ticker']} - VENDIENDO!")
                self.client.create_order(
                    ticker=pos['ticker'],
                    side='no' if pos['position'] > 0 else 'yes', # lado opuesto para cerrar
                    count=abs(pos['position']),
                    type='market'
                )
            elif ganancia_pct <= -STOP_LOSS:
                print(f"STOP LOSS {ganancia_pct*100:.1f}% en {pos['ticker']} - CORTANDO!")
                self.client.create_order(
                    ticker=pos['ticker'],
                    side='no' if pos['position'] > 0 else 'yes',
                    count=abs(pos['position']),
                    type='market'
                )

    def correr(self):
        while True:
            try:
                print("\n--- Checando mercados deportivos ---")
                mercados = self.buscar_mercados_deporte()
                print(f"Encontrados {len(mercados)} mercados: {[m['deporte'] for m in mercados]}")
                
                # Lógica de entrada simple: si YES < 40c y subiendo = compra
                for m in mercados[:3]: # top 3
                    if m['yes_ask'] < 40 and m['yes_ask'] > m['yes_bid']:
                        self.comprar(m, "yes")
                    elif m['no_ask'] < 40:
                        self.comprar(m, "no")
                
                # Checar si vendemos antes de que termine el juego
                self.checar_ganancia_y_vender()
                
                time.sleep(10)
            except Exception as e:
                print("Error:", e)
                time.sleep(15)

# USO
if __name__ == "__main__":
    bot = BotDeportivo("TU_EMAIL_KALSHI", "TU_PASSWORD")
    bot.correr()
