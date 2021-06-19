
-- Retrieves the last known hash for a given file (& stats)

SELECT hash
FROM   hash
WHERE  url = $url
  AND  mtime = $mtime
  AND  "size" = $size

