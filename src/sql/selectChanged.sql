
-- Finds files where the src and dest have different hashes

-- A four way join, because each side has the file in "sync"
-- and the stored hash in "hash". And there's a pair for
-- each of the source and destination files

-- This tells us which files must be copied from source
-- to destination

SELECT  src.url AS "from",
        dst.url AS "to"

FROM sync src

JOIN hash srcHash
    ON srcHash.url = src.url

JOIN sync dst
    ON dst.path = src.path

JOIN hash dstHash
    ON dstHash.url = dst.url

WHERE src."type"   = 'src'
  AND dst."type"   = 'dst'
  AND srcHash.hash != dstHash.hash

ORDER BY src.url
