import pyodbc


# ---------- Time Columns ---------- #
def separate_time_columns(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    separate_time_columns(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData 
        ADD Day INT, 
        Month INT, 
        Year INT, 
        SessionTime VARCHAR(8)
    """)

    cursor.execute("""
        UPDATE PriceData 
        SET 
            Day = DATEPART(day, Time), 
            Month = DATEPART(month, Time), 
            Year = DATEPART(year, Time), 
            SessionTime = FORMAT(CONVERT(datetime2(0), Time), 'HH:mm:ss')
    """)


# ---------- ETH Open ---------- #
def open_eth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    open_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD OpenETH VARCHAR(12)
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.OpenETH = subq.OpenPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, OpenPrice
        FROM PriceData
        WHERE SessionTime = '07:01:00'
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
""")
    

# ---------- RTH Open ---------- #
def open_rth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    open_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD OpenRTH VARCHAR(12)
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.OpenRTH = subq.OpenPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, OpenPrice
        FROM PriceData
        WHERE SessionTime = '08:30:00'
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
""")


# ---------- ETH High ---------- #
def high_eth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    high_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD HighETH VARCHAR(12)
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.HighETH = subq.HighPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MAX(HighPrice) AS HighPrice
        FROM PriceData
        WHERE SessionTime >= '07:01:00' AND SessionTime <= '08:30:00'
        GROUP BY Day, Month, Year, Symbol
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
    """)


# ---------- ETH Low ---------- #
def low_eth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    low_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD LowETH VARCHAR(12)
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.LowETH = subq.LowPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MIN(LowPrice) AS LowPrice
        FROM PriceData
        WHERE SessionTime >= '07:01:00' AND SessionTime <= '08:30:00'
        GROUP BY Day, Month, Year, Symbol
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
    """)


# ---------- RTH High ---------- #
def high_rth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    high_rth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD HighRTH VARCHAR(12)
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.HighRTH = subq.HighPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MAX(HighPrice) AS HighPrice
        FROM PriceData
        WHERE SessionTime >= '08:30:00' AND SessionTime <= '15:00:00'
        GROUP BY Day, Month, Year, Symbol
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
    """)


# ---------- RTH Low ---------- #
def low_rth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    low_rth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD LowRTH VARCHAR(12)
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.LowRTH = subq.LowPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MIN(LowPrice) AS LowPrice
        FROM PriceData
        WHERE SessionTime >= '08:30:00' AND SessionTime <= '15:00:00'
        GROUP BY Day, Month, Year, Symbol
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
    """)


# ---------- Holidays ---------- #
def eliminate_holidays(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    eliminate_holidays(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        SELECT Day, Month, Year
        FROM PriceData
        GROUP BY Day, Month, Year
        HAVING MAX(SessionTime) <> '15:00:00'
    """)

    incomplete_days = cursor.fetchall()
    for day in incomplete_days:
        cursor.execute("""
            DELETE FROM PriceData
            WHERE Day = ? AND Month = ? AND Year = ?
        """, day[0], day[1], day[2])


# ---------- Session Close ---------- #
def session_close(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    session_close(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD SessionClose VARCHAR(12)
    """)
    cursor.execute("""
        UPDATE pd
        SET pd.SessionClose = (
            SELECT subq.ClosePrice
            FROM PriceData subq
            WHERE subq.Day = pd.Day
                AND subq.Month = pd.Month
                AND subq.Year = pd.Year
                AND subq.Symbol = pd.Symbol
                AND subq.SessionTime = (
                    SELECT MAX(SessionTime)
                    FROM PriceData subq2
                    WHERE subq2.Day = pd.Day
                        AND subq2.Month = pd.Month
                        AND subq2.Year = pd.Year
                        AND subq2.Symbol = pd.Symbol
                        AND subq2.SessionTime <= '15:00:00'
                )
        )
        FROM PriceData pd;
    """)
