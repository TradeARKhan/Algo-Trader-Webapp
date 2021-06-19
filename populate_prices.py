import sqlite3, config
import tulipy, numpy
import alpaca_trade_api as tradeapi
from datetime import date

connection = sqlite3.connect('app.db')

connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol, name FROM stock
""")

rows = cursor.fetchall()

symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

chunk_size = 200
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]

    barsets = api.get_barset(symbol_chunk, 'day', after=date.today().isoformat())

    for symbol in barsets:
        print(f"processing symbol {symbol}") 

        # print(barsets[symbol])
        
        recent_closes = [bar.c for bar in barsets[symbol]]

        for bar in barsets[symbol]:
            stock_id = stock_dict[symbol]

            if len(recent_closes) >= 50 and date.today().isoformat() == bar.t.date().isoformat():
                sma_21 = tulipy.sma(numpy.array(recent_closes), peroid=21)[-1]
                sma_50 = tulipy.sma(numpy.array(recent_closes), peroid=50)[-1]
                rsi_14 = tupily.rsi(numpy.array(recent_closes), peroid=14)[-1]
            else:
                sma_21, sma_50, rsi_14 = None, None, None

            cursor.execute("""
                INSERT INTO stock_price (stock_id, date, open, high, low, close, volume, sma_21, sma_50, rsi_14)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v, sma_21, sma_50, rsi_14))

connection.commit()
