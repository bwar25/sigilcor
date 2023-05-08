import pyodbc


# ---------- Create Table for Price Data ---------- #
def create_price_data_table(db_connection: pyodbc.Connection) -> None:
    """
    Example usage:
    create_price_data_table(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        CREATE TABLE PriceData (
            Time datetime,
            OpenPrice FLOAT,
            HighPrice FLOAT,
            LowPrice FLOAT,
            ClosePrice FLOAT,
            Symbol varchar(6)
        );
    """)