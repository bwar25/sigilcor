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


# ---------- ETH Opening Range ---------- #
def or_eth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    or_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute(
        """ALTER TABLE PriceData
        ADD ORHETH FLOAT, ORLETH FLOAT"""
    )

    cursor.execute("""
    UPDATE pd 
    SET pd.ORHETH = subq.HighPrice, pd.ORLETH = subq.LowPrice 
    FROM PriceData pd 
    JOIN ( 
        SELECT Symbol, Day, Month, Year, 
            COALESCE(MAX(CASE WHEN SessionTime = '02:00:00' THEN HighPrice END), 
                     MAX(CASE WHEN SessionTime > '02:00:00' THEN HighPrice END)) AS HighPrice, 
            COALESCE(MAX(CASE WHEN SessionTime = '02:00:00' THEN LowPrice END), 
                     MAX(CASE WHEN SessionTime > '02:00:00' THEN LowPrice END)) AS LowPrice 
        FROM (
            SELECT pd.*,
                ROW_NUMBER() OVER (
                    PARTITION BY Symbol, Day, Month, Year
                    ORDER BY
                        CASE WHEN SessionTime >= '02:00:00' THEN SessionTime END ASC,
                        CASE WHEN SessionTime < '02:00:00' THEN SessionTime END DESC
                ) AS rn
            FROM PriceData pd
        ) subq2
        WHERE subq2.rn = 1
        GROUP BY Symbol, Day, Month, Year
    ) subq 
    ON pd.Symbol = subq.Symbol 
        AND pd.Day = subq.Day 
        AND pd.Month = subq.Month 
        AND pd.Year = subq.Year
    """)


# ---------- RTH Opening Range ---------- #
def or_rth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    or_rth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute(
        """ALTER TABLE PriceData
        ADD ORHRTH FLOAT, ORLRTH FLOAT"""
    )

    cursor.execute("""
    UPDATE pd 
    SET pd.ORHRTH = subq.HighPrice, pd.ORLRTH = subq.LowPrice 
    FROM PriceData pd 
    JOIN (
        SELECT Day, Month, Year, Symbol, 
            COALESCE(MAX(CASE WHEN SessionTime = '02:00:00' THEN HighPrice END), 
                     MAX(CASE WHEN SessionTime > '02:00:00' THEN HighPrice END)) AS HighPrice, 
            COALESCE(MAX(CASE WHEN SessionTime = '02:00:00' THEN LowPrice END), 
                     MAX(CASE WHEN SessionTime > '02:00:00' THEN LowPrice END)) AS LowPrice 
        FROM (
            SELECT pd.*,
                ROW_NUMBER() OVER (
                    PARTITION BY Symbol, Day, Month, Year
                    ORDER BY
                        CASE WHEN SessionTime >= '08:31:00' THEN SessionTime END ASC,
                        CASE WHEN SessionTime < '08:31:00' THEN SessionTime END DESC
                ) AS rn
            FROM PriceData pd
        ) subq2
        WHERE subq2.rn = 1
        GROUP BY Day, Month, Year, Symbol
    ) subq 
    ON pd.Symbol = subq.Symbol 
        AND pd.Day = subq.Day 
        AND pd.Month = subq.Month 
        AND pd.Year = subq.Year
    """)


# ---------- ETH High/Low ---------- #
def high_eth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    high_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData 
        ADD HighETH FLOAT
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.HighETH = subq.HighPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MAX(HighPrice) AS HighPrice
        FROM PriceData
        WHERE SessionTime >= '02:00:00' AND SessionTime <= '15:00:00'
        GROUP BY Day, Month, Year, Symbol
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
    """)


def low_eth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    low_eth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD LowETH FLOAT
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.LowETH = subq.LowPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MIN(LowPrice) AS LowPrice
        FROM PriceData
        WHERE SessionTime >= '02:00:00' AND SessionTime <= '15:00:00'
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
        ALTER TABLE PriceData ADD SessionClose FLOAT
    """)

    cursor.execute("""
        WITH max_session_times AS (
            SELECT Symbol, Day, Month, Year, MAX(SessionTime) AS MaxSessionTime
            FROM PriceData
            WHERE SessionTime <= '15:00:00'
            GROUP BY Symbol, Day, Month, Year
        )
        UPDATE pd
        SET pd.SessionClose = subq.ClosePrice
        FROM PriceData pd
        JOIN (
            SELECT p.Symbol, p.Day, p.Month, p.Year, p.ClosePrice
            FROM PriceData p
            JOIN max_session_times mst
            ON p.Symbol = mst.Symbol
            AND p.Day = mst.Day
            AND p.Month = mst.Month
            AND p.Year = mst.Year
            AND p.SessionTime = mst.MaxSessionTime
        ) AS subq
        ON pd.Symbol = subq.Symbol
        AND pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
    """)


# ---------- Prior Day ---------- #
def prior_day_high(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    prior_day_high(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD PriorDayHigh FLOAT
    """)

    cursor.execute("""
        UPDATE PriceData
        SET PriorDayHigh = subquery.PriorDayHigh
        FROM (
            SELECT Symbol, Day, Month, Year, HighETH,
                   LAG(HighETH) OVER (PARTITION BY Symbol ORDER BY Year, Month, Day) AS PriorDayHigh
            FROM PriceData
            GROUP BY Symbol, Day, Month, Year, HighETH
        ) AS subquery
        WHERE PriceData.Symbol = subquery.Symbol
        AND PriceData.Day = subquery.Day
        AND PriceData.Month = subquery.Month
        AND PriceData.Year = subquery.Year
    """)


def prior_day_low(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    prior_day_low(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD PriorDayLow FLOAT
    """)

    cursor.execute("""
        UPDATE PriceData
        SET PriorDayLow = subquery.PriorDayLow
        FROM (
            SELECT Symbol, Day, Month, Year, LowETH,
                   LAG(LowETH) OVER (PARTITION BY Symbol ORDER BY Year, Month, Day) AS PriorDayLow
            FROM PriceData
            GROUP BY Symbol, Day, Month, Year, LowETH
        ) AS subquery
        WHERE PriceData.Symbol = subquery.Symbol
        AND PriceData.Day = subquery.Day
        AND PriceData.Month = subquery.Month
        AND PriceData.Year = subquery.Year
    """)


def prior_day_close(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    prior_day_close(db_connection)
    """
    cursor = db_connection.cursor()
    cursor.execute("""
        ALTER TABLE PriceData ADD PriorDayClose FLOAT
    """)
    cursor.execute("""
        UPDATE PriceData
        SET PriorDayClose = subquery.PriorDayClose
        FROM (
            SELECT Symbol, Day, Month, Year, ClosePrice,
                   LAG(ClosePrice) OVER (PARTITION BY Symbol ORDER BY Year, Month, Day) AS PriorDayClose
            FROM PriceData
            WHERE SessionTime = '15:00:00'
            GROUP BY Symbol, Day, Month, Year, ClosePrice
        ) AS subquery
        WHERE PriceData.Symbol = subquery.Symbol
        AND PriceData.Day = subquery.Day
        AND PriceData.Month = subquery.Month
        AND PriceData.Year = subquery.Year
    """)


# ---------- Time of Session High/Low ---------- #
def session_hl(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    session_hl(db_connection)
    """
    cursor = db_connection.cursor()

    query = """
        SELECT 
            pd1.Day, 
            pd1.Month, 
            pd1.Year, 
            pd1.Symbol, 
            pd1.SessionTime AS SessionHigh, 
            pd1.HighPrice, 
            pd2.SessionTime AS SessionLow, 
            pd2.LowPrice
        FROM 
            (
                SELECT 
                    Day, 
                    Month, 
                    Year, 
                    Symbol, 
                    SessionTime, 
                    HighPrice,
                    LowPrice,
                    ROW_NUMBER() OVER (
                        PARTITION BY Day, Month, Year, Symbol 
                        ORDER BY HighPrice DESC
                    ) AS HighPriceRank,
                    ROW_NUMBER() OVER (
                        PARTITION BY Day, Month, Year, Symbol 
                        ORDER BY LowPrice ASC
                    ) AS LowPriceRank
                FROM 
                    PriceData
                WHERE 
                    SessionTime >= ? AND SessionTime <= ?
            ) pd1
            LEFT JOIN (
                SELECT 
                    Day, 
                    Month, 
                    Year, 
                    Symbol, 
                    SessionTime, 
                    LowPrice,
                    ROW_NUMBER() OVER (
                        PARTITION BY Day, Month, Year, Symbol 
                        ORDER BY LowPrice ASC
                    ) AS LowPriceRank
                FROM 
                    PriceData
                WHERE 
                    SessionTime >= ? AND SessionTime <= ?
            ) pd2 ON pd1.Day = pd2.Day
                    AND pd1.Month = pd2.Month
                    AND pd1.Year = pd2.Year
                    AND pd1.Symbol = pd2.Symbol
                    AND pd2.LowPriceRank = 1
        WHERE 
            pd1.HighPriceRank = 1
        ORDER BY 
            Year, 
            Month, 
            Day, 
            Symbol
    """
    params = ['02:00:00', '15:00:00', '02:00:00', '15:00:00']
    cursor.execute(query, params)
    
    rows = cursor.fetchall()
    
    update_query = """
        UPDATE PriceData 
        SET SessionTimeHigh=?, SessionHighPrice=?, SessionTimeLow=?, SessionLowPrice=? 
        WHERE Day=? AND Month=? AND Year=? AND Symbol=?
    """
    batch_size = 1000
    updates = []
    for i, row in enumerate(rows):
        day, month, year, symbol, session_high, high_price, session_low, low_price = row
        updates.append((session_high, high_price, session_low, low_price, day, month, year, symbol))
        if len(updates) == batch_size or i == len(rows) - 1:
            cursor.executemany(update_query, updates)
            db_connection.commit()
            updates = []
