
-- Finds files which exist on the destination, but which
-- do not exist on the source

-- These files need to be deleted to get back in sync

SELECT dst.url AS url

FROM sync dst

LEFT JOIN sync src
    ON  src.path   = dst.path
    AND src."type" = 'src'

WHERE dst."type"   = 'dst'
  AND src.path     IS NULL

ORDER BY dst.url
