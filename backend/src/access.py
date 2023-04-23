from db.src.auxiliary.connection import connect_to_db

from typing import Generator, Dict


# ---------- Price Data ---------- #
def get_price_data(symbol: str = "ES") -> Generator[Dict[str, float], None, None]:
    with connect_to_db() as db_connection:
        cursor = db_connection.cursor()
        sql_query = 'SELECT Time, Open_Price, High_Price, Low_Price, Close_Price, Symbol FROM PriceData WHERE Symbol = ?'
        cursor.prepare(sql_query)
        cursor.execute(None, (symbol,))
        for row in cursor:
            result = {
                'time': row[0],
                'open_price': row[1],
                'high_price': row[2],
                'low_price': row[3],
                'close_price': row[4],
                'symbol': row[5]
            }
            yield result