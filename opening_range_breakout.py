import sqlite3, config
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from datetime import date

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    select * from strategy where name = 'opening_range_breakout'
""")

strategy_id = cursor.fetchone()['id']

cursor.execute("""
    select symbol, name
    from stock
    join stock_strategy on stock_strategy.stock_id = stock.id
    where stock_strategy.strategy_id = ?
    """, (strategy_id,))

stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

api=tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

current_date = date.today().isoformat()
start_minute_bar = f"{current_date} 09:30:00"
end_minute_bar = f"{current_date} 09:45:00"

orders = api.list_orders(status='all', limit=200)
existing_order_symbols = [order.symbol for order in orders]
print(existing_order_symbols)


for symbol in symbols:
    minute_bars = api.get_barset(symbol, '15Min', start=current_date, end=current_date).df
    
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]
    print(opening_range_bars)
    opening_range_low = opening_range_bars[symbol,'low'].min()
    opening_range_high = opening_range_bars[symbol,'high'].max()
    opening_range = opening_range_high - opening_range_low

    after_opening_range_mask = minute_bars.index >= end_minute_bar
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]
    after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars[symbol,'close'] > opening_range_bars[symbol,'high']]

    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:
            limit_price = after_opening_range_breakout.iloc[0][symbol,'close']

            print(f"placing order for {symbol} at {limit_price}, closed above {opening_range_high}")

            api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='100',
                time_in_force='day',
                order_class='bracket',
                limit_price=limit_price,
                take_profit=dict(limit_price=limit_price + opening_range,),
                stop_loss=dict(stop_price=limit_price - opening_range,)
            )
        else:
            print(f"Already an order for {symbol}, skipping")

    