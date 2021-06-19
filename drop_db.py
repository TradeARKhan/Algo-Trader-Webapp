import sqlite3

connection = sqlite3.connect('app.db')
    
cursor = connection.cursor()

cursor.execute("""
    DROP TABLE stock
""")
cursor.execute("""
    DROP TABLE stock_price
""")
cursor.execute("""
    DROP TABLE strategy
""")
cursor.execute("""
    DROP TABLE stock_strategy
""")

connection.commit()