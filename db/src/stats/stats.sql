-- Percentage of SessionClose Above/Below PriorDayClose w/ Filter of ORRTH > ORETH

WITH subq AS (
    SELECT p.Symbol, 
           MIN(p.SessionTime) AS FirstSessionTime
    FROM PriceData p
    WHERE p.SessionClose IS NOT NULL 
        AND p.PriorDayClose IS NOT NULL 
        AND p.SessionTimeHigh >= '02:00:00' 
        AND p.SessionTimeHigh <= '15:00:00' 
        AND p.ORHRTH > p.ORHETH 
        AND p.ORLRTH > p.ORLETH
    GROUP BY p.Symbol
)
SELECT 
    p.Symbol, 
    COUNT(CASE WHEN p.SessionClose > p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) AS AboveCount,
    COUNT(CASE WHEN p.SessionClose < p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) AS BelowCount,
    CONVERT(decimal(5,2), COUNT(CASE WHEN p.SessionClose > p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) * 100.0 / COUNT(*) ) AS AbovePct,
    CONVERT(decimal(5,2), COUNT(CASE WHEN p.SessionClose < p.PriorDayClose AND p.ORHRTH > p.ORHETH AND p.ORLRTH > p.ORLETH THEN 1 END) * 100.0 / COUNT(*) ) AS BelowPct
FROM 
    PriceData p
JOIN subq ON p.Symbol = subq.Symbol AND p.SessionTime = subq.FirstSessionTime
WHERE 
    p.SessionClose IS NOT NULL 
    AND p.PriorDayClose IS NOT NULL 
    AND p.SessionTimeHigh >= '02:00:00' 
    AND p.SessionTimeHigh <= '15:00:00' 
    AND p.ORHRTH > p.ORHETH 
    AND p.ORLRTH > p.ORLETH
GROUP BY 
    p.Symbol
ORDER BY 
    p.Symbol;



-- Total Difference & Cumulative Difference of ...

WITH subq AS (
    SELECT p.Symbol, 
           MIN(p.SessionTime) AS FirstSessionTime, 
           p.Day, 
           p.Month, 
           p.Year, 
           p.SessionClose,
           p.ORHRTH,
           (p.SessionClose - p.ORHRTH) AS TotalDifference
    FROM PriceData p
    GROUP BY p.Symbol, p.Day, p.Month, p.Year, p.SessionClose, p.ORHRTH
), subq2 AS (
    SELECT Symbol, FirstSessionTime, Day, Month, Year, SessionClose, ORHRTH, TotalDifference,
           SUM(TotalDifference) OVER (ORDER BY Symbol, Year, Month, Day, SessionClose, ORHRTH) AS CumulativeDifference
    FROM subq
)
SELECT Symbol, FirstSessionTime, Day, Month, Year, SessionClose, ORHRTH, TotalDifference, CumulativeDifference
FROM subq2
ORDER BY Symbol, Year, Month, Day, SessionClose, ORHRTH;