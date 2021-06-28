
-- Retrieves the last known hash for a given file (& stats)

SELECT hash
FROM   hash
WHERE  url = $url
  AND  mtime = datetime($mtime)
  AND  "size" = $size

