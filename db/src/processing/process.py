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
            COALESCE(MAX(CASE WHEN SessionTime = '08:31:00' THEN HighPrice END), 
                     MAX(CASE WHEN SessionTime > '08:31:00' THEN HighPrice END)) AS HighPrice, 
            COALESCE(MAX(CASE WHEN SessionTime = '08:31:00' THEN LowPrice END), 
                     MAX(CASE WHEN SessionTime > '08:31:00' THEN LowPrice END)) AS LowPrice 
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
        ALTER TABLE PriceData ADD HighETH FLOAT
    """)

    cursor.execute("""
    UPDATE pd
    SET pd.HighETH = subq.HighPrice
    FROM PriceData pd
    JOIN (
        SELECT Day, Month, Year, Symbol, MAX(HighPrice) AS HighPrice
        FROM PriceData
        WHERE SessionTime >= '02:00:00' AND SessionTime <= '08:30:00'
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
        WHERE SessionTime >= '02:00:00' AND SessionTime <= '08:30:00'
        GROUP BY Day, Month, Year, Symbol
    ) subq
    ON pd.Day = subq.Day
        AND pd.Month = subq.Month
        AND pd.Year = subq.Year
        AND pd.Symbol = subq.Symbol
    """)


# ---------- RTH High/Low ---------- #
def high_rth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    high_rth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD HighRTH FLOAT
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


def low_rth(db_connection: pyodbc.Connection) -> None:
    """
    Example Usage:
    low_rth(db_connection)
    """
    cursor = db_connection.cursor()

    cursor.execute("""
        ALTER TABLE PriceData ADD LowRTH FLOAT
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
        ALTER TABLE PriceData ADD SessionClose FLOAT
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
            SELECT Symbol, Day, Month, Year, HighRTH,
                   LAG(HighRTH) OVER (PARTITION BY Symbol ORDER BY Year, Month, Day) AS PriorDayHigh
            FROM PriceData
            GROUP BY Symbol, Day, Month, Year, HighRTH
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
            SELECT Symbol, Day, Month, Year, LowRTH,
                   LAG(LowRTH) OVER (PARTITION BY Symbol ORDER BY Year, Month, Day) AS PriorDayLow
            FROM PriceData
            GROUP BY Symbol, Day, Month, Year, LowRTH
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
    db_connection.commit()