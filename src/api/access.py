from database.connection import connect_to_db

from typing import List, Dict


def get_price_data(symbol: str = "ES") -> List[Dict[str, float]]:
    with connect_to_db() as db_connection:
        sql_query = 'SELECT Time, Open_Price, High_Price, Low_Price, Close_Price, Symbol FROM PriceData WHERE Symbol = ?'
        cursor = db_connection.cursor()
        cursor.execute(sql_query)

        rows = cursor.fetchall()

        results = []
        for row in rows:
            result = {
                'time': row[0],
                'open_price': row[1],
                'high_price': row[2],
                'low_price': row[3],
                'close_price': row[4],
                'symbol': row[5]
            }
            results.append(result)

        return results