
-- Retrieves the url & path of files which were found on the
-- source side of a sync, but were not found on the destination
-- side.

-- So these are files which need to be copied

SELECT src.url  AS url,
       src.path AS path

FROM      sync src

LEFT JOIN sync dst
    ON  src.path   = dst.path
    AND dst."type" = 'dst'

WHERE src."type" = 'src'
  AND dst.path   IS NULL

ORDER BY src.url
