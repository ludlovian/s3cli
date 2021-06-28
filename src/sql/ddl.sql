-- MAIN database ---------------------------------


CREATE VIEW IF NOT EXISTS dbVersion AS
SELECT 3 AS version;

CREATE TABLE IF NOT EXISTS hash (
    url    TEXT    NOT NULL PRIMARY KEY,
    mtime  TEXT    NOT NULL,
    size   INTEGER NOT NULL,
    hash   TEXT
);

-- TEMP database ---------------------------------

CREATE TEMP TABLE sync (
    type   TEXT    NOT NULL,
    path   TEXT    NOT NULL,
    url    TEXT    NOT NULL UNIQUE,
    mtime  TEXT    NOT NULL,
    size   INTEGER NOT NULL,
    PRIMARY KEY (type, path)
);

-- Finds files where we do not have a stored hash
-- that matches the file name and given stats.
-- We need to recalculate the hash for these.

CREATE TEMP VIEW missingHashes AS
SELECT a.url AS url

FROM sync a
LEFT JOIN hash b
    ON  b.url   = a.url
    AND b.mtime = a.mtime
    AND b.size  = a.size

WHERE b.hash IS NULL

ORDER BY a.url;


-- Retrieves the url & path of files which were found on the
-- source side of a sync, but were not found on the destination
-- side.
-- So these are files which need to be copied

CREATE TEMP VIEW missingFiles AS
SELECT src.url  AS url,
       src.path AS path

FROM      sync src

LEFT JOIN sync dst
    ON  src.path = dst.path
    AND dst.type = 'dst'

WHERE src.type = 'src'
  AND dst.path   IS NULL

ORDER BY src.url;


-- Finds files where the src and dest have different hashes
-- A four way join, because each side has the file in "sync"
-- and the stored hash in "hash". And there's a pair for
-- each of the source and destination files
-- This tells us which files must be copied from source
-- to destination

CREATE TEMP VIEW changedFiles AS
SELECT  src.url AS src,
        dst.url AS dst

FROM sync src

JOIN hash srcHash
    ON  srcHash.url     = src.url
    AND srcHash.size    = src.size
    AND srcHash.mtime   = src.mtime

JOIN sync dst
    ON  dst.path    = src.path
    AND dst.type    = 'dst'

JOIN hash dstHash
    ON  dstHash.url     = dst.url
    AND dstHash.size    = dst.size
    AND dstHash.mtime   = dst.mtime

WHERE src.type      = 'src'
  AND srcHash.hash  != dstHash.hash

ORDER BY src.url;

-- Finds files which exist on the destination, but which
-- do not exist on the source
-- These files need to be deleted to get back in sync

CREATE TEMP VIEW surplusFiles AS
SELECT dst.url AS url

FROM sync dst

LEFT JOIN sync src
    ON  src.path   = dst.path
    AND src.type   = 'src'

WHERE dst.type     = 'dst'
  AND src.path     IS NULL

ORDER BY dst.url;
