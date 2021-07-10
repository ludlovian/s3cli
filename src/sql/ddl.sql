PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- MAIN database ---------------------------------


CREATE VIEW IF NOT EXISTS dbVersion AS
SELECT 4 AS version;

CREATE TABLE IF NOT EXISTS content(
    contentId   INTEGER PRIMARY KEY NOT NULL,
    md5Hash     TEXT NOT NULL,
    size        INTEGER NOT NULL,
    contentType TEXT,
    updated     TEXT DEFAULT (datetime('now')),
    UNIQUE (md5Hash, size)
);

CREATE TABLE IF NOT EXISTS s3_file(
    bucket      TEXT NOT NULL,
    path        TEXT NOT NULL,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    mtime       TEXT NOT NULL,
    storage     TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (bucket, path)
);

CREATE TABLE IF NOT EXISTS local_file(
    path        TEXT NOT NULL PRIMARY KEY,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    mtime       TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now'))
);

-- VIEWS ----------------------------------------

CREATE VIEW IF NOT EXISTS s3_file_view AS
SELECT  f.bucket        AS bucket,
        f.path          AS path,
        c.size          AS size,
        f.mtime         AS mtime,
        f.storage       AS storage,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash
FROM    s3_file f
JOIN    content c USING (contentId)
ORDER BY f.bucket, f.path;

CREATE VIEW IF NOT EXISTS local_file_view AS
SELECT  f.path          AS path,
        c.size          AS size,
        f.mtime         AS mtime,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash
FROM    local_file f
JOIN    content c USING (contentId)
ORDER BY f.path;

CREATE VIEW IF NOT EXISTS content_use_view AS
  WITH lc AS (
    SELECT   contentId        AS contentId,
             count(contentId) AS use
    FROM     local_file
    GROUP BY contentId
  ),
  rc AS (
    SELECT   contentId        AS contentId,
             count(contentId) AS use
    FROM     s3_file
    GROUP BY contentId
  )

  SELECT c.contentId         AS contentId,
         coalesce(lc.use, 0) AS local_use,
         coalesce(rc.use, 0) AS remote_use
  FROM content c
  LEFT JOIN lc USING (contentId)
  LEFT JOIN rc USING (contentId);
