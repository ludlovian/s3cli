
-- Finds files where we do not have a stored hash
-- that matches the file name and given stats.

-- We need to recalculate the has for these.

SELECT a.url AS url

FROM sync a
LEFT JOIN hash b
    ON  a.url    = b.url
    AND a.mtime  = b.mtime
    AND a."size" = b."size"

WHERE b.hash IS NULL

ORDER BY a.url
