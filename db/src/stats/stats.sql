-- Count of Session H/L per Symbol per Hour

SELECT 
    q1.Symbol, 
    q1.Hour, 
    q1.HighCount, 
    q2.LowCount,
    CONVERT(decimal(5,2), q1.HighCount * 100.0 / t.TotalCount) AS HighPct,
    CONVERT(decimal(5,2), q2.LowCount * 100.0 / t.TotalCount) AS LowPct
FROM 
    (
        SELECT 
            Symbol, 
            DATEPART(hour, SessionTimeHigh) AS Hour, 
            COUNT(DISTINCT SessionTimeHigh) AS HighCount
        FROM 
            PriceData
        WHERE 
            SessionTimeHigh IS NOT NULL AND 
            SessionHighPrice IS NOT NULL AND 
            SessionTimeHigh >= '02:00:00' AND SessionTimeHigh <= '15:00:00'
        GROUP BY 
            Symbol, 
            DATEPART(hour, SessionTimeHigh)
    ) AS q1
    JOIN (
        SELECT 
            Symbol, 
            DATEPART(hour, SessionTimeLow) AS Hour, 
            COUNT(DISTINCT SessionTimeLow) AS LowCount
        FROM 
            PriceData
        WHERE 
            SessionTimeLow IS NOT NULL AND 
            SessionLowPrice IS NOT NULL AND 
            SessionTimeLow >= '02:00:00' AND SessionTimeLow <= '15:00:00'
        GROUP BY 
            Symbol, 
            DATEPART(hour, SessionTimeLow)
    ) AS q2 ON q1.Symbol = q2.Symbol AND q1.Hour = q2.Hour
    JOIN (
        SELECT 
            Symbol, 
            COUNT(DISTINCT SessionTimeHigh) + COUNT(DISTINCT SessionTimeLow) AS TotalCount
        FROM 
            PriceData
        WHERE 
            SessionTimeHigh IS NOT NULL AND 
            SessionLowPrice IS NOT NULL AND 
            SessionTimeHigh >= '02:00:00' AND SessionTimeHigh <= '15:00:00'
        GROUP BY 
            Symbol
    ) AS t ON q1.Symbol = t.Symbol
ORDER BY 
    q1.Symbol, 
    q1.Hour


-- Percentage of SessionClose Above/Below PriorDayClose w/ Filter of ORRTH > ORETH

SELECT 
    p.Symbol, 
    COUNT(CASE WHEN p.SessionClose > p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) AS AboveCount,
    COUNT(CASE WHEN p.SessionClose < p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) AS BelowCount,
    CONVERT(decimal(5,2), COUNT(CASE WHEN p.SessionClose > p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) * 100.0 / COUNT(*) ) AS AbovePct,
    CONVERT(decimal(5,2), COUNT(CASE WHEN p.SessionClose < p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) * 100.0 / COUNT(*) ) AS BelowPct
FROM 
    PriceData p
WHERE 
    p.SessionClose IS NOT NULL AND 
    p.PriorDayClose IS NOT NULL AND 
    p.SessionTimeHigh >= '02:00:00' AND p.SessionTimeHigh <= '15:00:00' AND
    p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH
GROUP BY 
    p.Symbol
ORDER BY 
    p.Symbol
