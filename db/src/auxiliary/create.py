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
            OpenPrice varchar(12),
            HighPrice varchar(12),
            LowPrice varchar(12),
            ClosePrice varchar(12),
            Symbol varchar(6)
        );
    """)