from typing import List

import pyodbc


# ---------- Insert Price Data ---------- #
def insert_price_data(db_connection: pyodbc.Connection, filepaths: List[str]) -> None:
    """
    Example Usage:
    insert_price_data(db_connection, filepaths)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        CREATE TABLE StagingPriceData (
            Time VARCHAR(50), 
            OpenPrice VARCHAR(12), 
            HighPrice VARCHAR(12), 
            LowPrice VARCHAR(12), 
            ClosePrice VARCHAR(12),
            Symbol VARCHAR(6)
        )
    """)

    for filepath in filepaths:
        cursor.execute(f"""
            BULK INSERT StagingPriceData 
            FROM '{filepath}' 
            WITH (FORMAT = 'CSV', FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK, FIRSTROW = 2)
        """)

        cursor.execute("""
            INSERT INTO PriceData 
            (Time, OpenPrice, HighPrice, LowPrice, ClosePrice, Symbol) 
            SELECT CONVERT(datetime2(0), Time, 5), OpenPrice, HighPrice, LowPrice, ClosePrice, Symbol 
            FROM StagingPriceData
        """)

        cursor.execute("TRUNCATE TABLE StagingPriceData")

    cursor.execute("DROP TABLE StagingPriceData")