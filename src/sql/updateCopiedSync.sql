--
-- Updates the sync file record after a copy
--
UPDATE sync
SET
    mtime = (
        SELECT mtime FROM hash
        WHERE url = $url
    ),
    "size" = (
        SELECT "size" FROM hash
        WHERE url = $url
    )
WHERE url = $url
