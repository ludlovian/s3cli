import { homedir } from 'os'
import { resolve } from 'path'

import SQLite from 'better-sqlite3'

import once from 'pixutil/once'

import { tidy, statement, transaction } from './util.mjs'

const DBVERSION = 4

const db = once(() => {
  const dbFile =
    process.env.DB || resolve(homedir(), '.databases', 'files4.sqlite')
  const db = new SQLite(dbFile)
  db.exec(ddl)
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get()
  if (version !== DBVERSION) {
    throw new Error('Wrong version of database: ' + dbFile)
  }
  return db
})

export function sql (text) {
  return statement(tidy(text), { db })
}
sql.transaction = fn => transaction(fn, db)

const ddl = tidy(`
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- MAIN database ---------------------------------

-- Version of the database schema we are using

CREATE VIEW IF NOT EXISTS dbVersion AS
SELECT 4 AS version;

-- Unique item of content, as identified by (md5, size)
--

CREATE TABLE IF NOT EXISTS content(
    contentId   INTEGER PRIMARY KEY NOT NULL,
    md5Hash     TEXT NOT NULL,
    size        INTEGER NOT NULL,
    contentType TEXT,
    updated     TEXT DEFAULT (datetime('now')),
    UNIQUE (md5Hash, size)
);

-- A local file containing some content

CREATE TABLE IF NOT EXISTS local_file(
    path        TEXT NOT NULL PRIMARY KEY,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    mtime       TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now'))
);

-- A file held on S3 with some content

CREATE TABLE IF NOT EXISTS s3_file(
    bucket      TEXT NOT NULL,
    path        TEXT NOT NULL,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    mtime       TEXT NOT NULL,
    storage     TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (bucket, path)
);

-- A file on gdrive containing some content

CREATE TABLE IF NOT EXISTS gdrive_file(
    path        TEXT NOT NULL PRIMARY KEY,
    contentId   INTEGER NOT NULL REFERENCES content(contentId),
    googleId    TEXT NOT NULL,
    mtime       TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now'))
);

-- Indexes ------------------------------------------

CREATE INDEX IF NOT EXISTS local_file_i1
  ON local_file (contentId);

CREATE INDEX IF NOT EXISTS s3_file_i1
  ON s3_file (contentId);

CREATE INDEX IF NOT EXISTS gdrive_file_i1
  ON gdrive_file (contentId);

-- Triggers on Tables ------------------------------

-- Content is deleted once orphaned
-- Check performed after updates and deletes of files tables

CREATE TRIGGER IF NOT EXISTS local_file_td
AFTER DELETE ON local_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND NOT EXISTS (
        SELECT contentId FROM local_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM s3_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM gdrive_file
        WHERE  contentId = OLD.contentId);
END;

CREATE TRIGGER IF NOT EXISTS local_file_tu
AFTER UPDATE OF contentId ON local_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND NOT EXISTS (
        SELECT contentId FROM local_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM s3_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM gdrive_file
        WHERE  contentId = OLD.contentId);
END;

CREATE TRIGGER IF NOT EXISTS s3_file_td
AFTER DELETE ON s3_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND NOT EXISTS (
        SELECT contentId FROM local_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM s3_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM gdrive_file
        WHERE  contentId = OLD.contentId);
END;

CREATE TRIGGER IF NOT EXISTS s3_file_tu
AFTER UPDATE OF contentId ON s3_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND NOT EXISTS (
        SELECT contentId FROM local_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM s3_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM gdrive_file
        WHERE  contentId = OLD.contentId);
END;

CREATE TRIGGER IF NOT EXISTS gdrive_file_td
AFTER DELETE ON gdrive_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND NOT EXISTS (
        SELECT contentId FROM local_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM s3_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM gdrive_file
        WHERE  contentId = OLD.contentId);
END;

CREATE TRIGGER IF NOT EXISTS gdrive_file_tu
AFTER UPDATE OF contentId ON gdrive_file
BEGIN
    DELETE FROM content
    WHERE   contentId = OLD.contentId
    AND NOT EXISTS (
        SELECT contentId FROM local_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM s3_file
        WHERE  contentId = OLD.contentId)
    AND NOT EXISTS (
        SELECT contentId FROM gdrive_file
        WHERE  contentId = OLD.contentId);
END;

-- Main views of files --------------------------

-- locally held file

CREATE VIEW IF NOT EXISTS local_file_view AS
SELECT  f.path          AS path,
        c.size          AS size,
        f.mtime         AS mtime,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash,
        f.contentId     AS contentId
FROM    local_file f
JOIN    content c USING (contentId)
ORDER BY f.path;

-- S3 file

CREATE VIEW IF NOT EXISTS s3_file_view AS
SELECT  f.bucket        AS bucket,
        f.path          AS path,
        c.size          AS size,
        f.mtime         AS mtime,
        f.storage       AS storage,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash,
        f.contentId     AS contentId
FROM    s3_file f
JOIN    content c USING (contentId)
ORDER BY f.bucket, f.path;

-- gdrive file

CREATE VIEW IF NOT EXISTS gdrive_file_view AS
SELECT  f.path          AS path,
        f.googleId      AS googleId,
        c.size          AS size,
        f.mtime         AS mtime,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash,
        f.contentId     AS contentId
FROM    gdrive_file f
JOIN    content c USING (contentId)
ORDER BY f.path;

-- view inserts ---------------------------------

-- local file

CREATE TRIGGER IF NOT EXISTS local_file_view_ti
INSTEAD OF INSERT ON local_file_view
BEGIN
    INSERT INTO content
        (md5Hash, size, contentType)
    VALUES
        (NEW.md5Hash, NEW.size, NEW.contentType)
    ON CONFLICT DO UPDATE
        SET contentType = excluded.contentType,
            updated     = excluded.updated
        WHERE contentType != excluded.contentType;

    INSERT INTO local_file
        (path, contentId, mtime)
    SELECT  NEW.path,
            contentId,
            datetime(NEW.mtime)
    FROM    content
    WHERE   md5Hash = NEW.md5Hash
    AND     size    = NEW.size

    ON CONFLICT DO UPDATE
        SET contentId   = excluded.contentId,
            mtime       = excluded.mtime,
            updated     = excluded.updated
        WHERE contentId != excluded.contentId
        OR    mtime     != excluded.mtime;
END;

-- s3 file

CREATE TRIGGER IF NOT EXISTS s3_file_view_ti
INSTEAD OF INSERT ON s3_file_view
BEGIN
    INSERT INTO content
        (md5Hash, size, contentType)
    VALUES
        (NEW.md5Hash, NEW.size, NEW.contentType)
    ON CONFLICT DO UPDATE
        SET contentType = excluded.contentType,
            updated     = excluded.updated
        WHERE contentType != excluded.contentType;

    INSERT INTO s3_file
        (bucket, path, contentId, mtime, storage)
    SELECT  NEW.bucket,
            NEW.path,
            contentId,
            datetime(NEW.mtime),
            NEW.storage
    FROM    content
    WHERE   md5Hash = NEW.md5Hash
    AND     size    = NEW.size

    ON CONFLICT DO UPDATE
        SET contentId   = excluded.contentId,
            mtime       = excluded.mtime,
            storage     = excluded.storage,
            updated     = excluded.updated
        WHERE contentId != excluded.contentId
        OR    mtime     != excluded.mtime
        OR    storage   != excluded.storage;
END;

-- gdrive file

CREATE TRIGGER IF NOT EXISTS gdrive_file_view_ti
INSTEAD OF INSERT ON gdrive_file_view
BEGIN
    INSERT INTO content
        (md5Hash, size, contentType)
    VALUES
        (NEW.md5Hash, NEW.size, NEW.contentType)
    ON CONFLICT DO UPDATE
        SET contentType = excluded.contentType,
            updated     = excluded.updated
        WHERE contentType != excluded.contentType;

    INSERT INTO gdrive_file
        (path, contentId, mtime, googleId)
    SELECT  NEW.path,
            contentId,
            datetime(NEW.mtime),
            NEW.googleId
    FROM    content
    WHERE   md5Hash = NEW.md5Hash
    AND     size    = NEW.size

    ON CONFLICT DO UPDATE
        SET contentId   = excluded.contentId,
            mtime       = excluded.mtime,
            googleId    = excluded.googleId,
            updated     = excluded.updated
        WHERE contentId != excluded.contentId
        OR    mtime     != excluded.mtime
        OR    googleId  != excluded.googleId;
END;

-- Sync helping views -------------------------------

-- On local but not on S3 --

CREATE VIEW IF NOT EXISTS local_not_s3_view AS
SELECT *
FROM   local_file_view
WHERE  contentId NOT IN (
  SELECT contentId
  FROM   s3_file);

-- On S3 but not on local --

CREATE VIEW IF NOT EXISTS s3_not_local_view AS
SELECT *
FROM   s3_file_view
WHERE  contentId NOT IN (
  SELECT contentId
  FROM   local_file);

-- On local but not on gdrive --

CREATE VIEW IF NOT EXISTS local_not_gdrive_view AS
SELECT *
FROM   local_file_view
WHERE  contentId NOT IN (
  SELECT contentId
  FROM   gdrive_file);

-- On gdrive but not on local --

CREATE VIEW IF NOT EXISTS gdrive_not_local_view AS
SELECT *
FROM   gdrive_file_view
WHERE  contentId NOT IN (
  SELECT contentId
  FROM   local_file);


-- On both local and S3 once

CREATE VIEW IF NOT EXISTS local_and_s3_view AS
SELECT l.path       AS localPath,
       s.bucket     AS s3Bucket,
       s.path       AS s3Path,
       l.contentId  as contentId
FROM   local_file l
JOIN   s3_file s USING (contentId)
GROUP BY contentId
HAVING count(l.path) = 1
AND    count(s.path) = 1;

-- On both local and gdrive once

CREATE VIEW IF NOT EXISTS local_and_gdrive_view AS
SELECT l.path       AS localPath,
       g.path       AS gdrivePath,
       l.contentId  as contentId
FROM   local_file l
JOIN   gdrive_file g USING (contentId)
GROUP BY contentId
HAVING count(l.path) = 1
AND    count(g.path) = 1;

-- Multiple copies exist somewhere

CREATE VIEW IF NOT EXISTS duplicates_view AS
SELECT  contentId AS contentId
FROM    local_file
GROUP BY contentId
HAVING count(contentId) > 1

UNION

SELECT  contentId AS contentId
FROM    s3_file
GROUP BY contentId
HAVING count(contentId) > 1

UNION

SELECT  contentId AS contentId
FROM    gdrive_file
GROUP BY contentId
HAVING count(contentId) > 1;

COMMIT;
`)
