PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

---- Version ---------------------------

DROP VIEW IF EXISTS dbversion;
CREATE VIEW dbversion AS SELECT 5 AS version;

----------------------------------------
--  TABLES  ----------------------------
----------------------------------------

-- Content -----------------------------

CREATE TABLE IF NOT EXISTS tbl_content(
    contentId   INTEGER PRIMARY KEY NOT NULL,
    md5Hash     TEXT NOT NULL,
    size        INTEGER NOT NULL,
    contentType TEXT,
    updated     TEXT DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS ix1_content ON tbl_content(md5Hash, size);

-- A locally held file -----------------

CREATE TABLE IF NOT EXISTS tbl_local_file(
    path        TEXT NOT NULL PRIMARY KEY,
    contentId   INTEGER NOT NULL REFERENCES tbl_content(contentId),
    mtime       TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now'))
) WITHOUT ROWID;
DROP INDEX IF EXISTS ix1_local_file;
CREATE INDEX ix1_local_file ON tbl_local_file(contentId);
DROP TRIGGER IF EXISTS td_local_file;
DROP TRIGGER IF EXISTS tu_local_file;
CREATE TRIGGER td_local_file AFTER DELETE ON tbl_local_file
BEGIN
  INSERT INTO sp_cleanContent VALUES(OLD.contentId);
END;
CREATE TRIGGER tu_local_file AFTER UPDATE OF contentId ON tbl_local_file
BEGIN
  INSERT INTO sp_cleanContent VALUES(OLD.contentId);
END;

-- A file held on S3 -------------------

CREATE TABLE IF NOT EXISTS tbl_s3_file(
    bucket      TEXT NOT NULL,
    path        TEXT NOT NULL,
    contentId   INTEGER NOT NULL REFERENCES tbl_content(contentId),
    mtime       TEXT NOT NULL,
    storage     TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (bucket, path)
) WITHOUT ROWID;
DROP INDEX IF EXISTS ix1_s3_file;
CREATE INDEX ix1_s3_file ON tbl_s3_file(contentId);
DROP TRIGGER IF EXISTS td_s3_file;
DROP TRIGGER IF EXISTS tu_s3_file;
CREATE TRIGGER td_s3_file AFTER DELETE ON tbl_s3_file
BEGIN
  INSERT INTO sp_cleanContent VALUES(OLD.contentId);
END;
CREATE TRIGGER tu_s3_file AFTER UPDATE OF contentId ON tbl_s3_file
BEGIN
  INSERT INTO sp_cleanContent VALUES(OLD.contentId);
END;

-- A file held on GDrive ---------------

CREATE TABLE IF NOT EXISTS tbl_gdrive_file(
    path        TEXT NOT NULL PRIMARY KEY,
    contentId   INTEGER NOT NULL REFERENCES tbl_content(contentId),
    googleId    TEXT NOT NULL,
    mtime       TEXT NOT NULL,
    updated     TEXT DEFAULT (datetime('now'))
) WITHOUT ROWID;
DROP INDEX IF EXISTS ix1_gdrive_file;
CREATE INDEX ix1_gdrive_file ON tbl_gdrive_file(contentId);
DROP TRIGGER IF EXISTS td_gdrive_file;
DROP TRIGGER IF EXISTS tu_gdrive_file;
CREATE TRIGGER td_gdrive_file AFTER DELETE ON tbl_gdrive_file
BEGIN
  INSERT INTO sp_cleanContent VALUES(OLD.contentId);
END;
CREATE TRIGGER tu_gdrive_file AFTER UPDATE OF contentId ON tbl_gdrive_file
BEGIN
  INSERT INTO sp_cleanContent VALUES(OLD.contentId);
END;

----------------------------------------
--  Stored Procedures  -----------------
----------------------------------------

--- Clean content ----------------------

DROP VIEW IF EXISTS sp_cleanContent;
CREATE VIEW sp_cleanContent AS
  SELECT 0 contentId
  WHERE 0;
CREATE TRIGGER sp_cleanContent_t INSTEAD OF INSERT ON sp_cleanContent
BEGIN
  DELETE FROM tbl_content
  WHERE  contentId = NEW.contentId
  AND NOT EXISTS (
    SELECT contentId from tbl_local_file
    WHERE  contentId = NEW.contentId
    UNION ALL
    SELECT contentId from tbl_s3_file
    WHERE  contentId = NEW.contentId
    UNION ALL
    SELECT contentId from tbl_gdrive_file
    WHERE  contentId = NEW.contentId
  );
END;

--- Insert local file ------------------

DROP VIEW IF EXISTS sp_insertLocalFile;
CREATE VIEW sp_insertLocalFile AS
  SELECT 0 path, 0 size, 0 mtime, 0 contentType, 0 md5Hash
  WHERE 0;
CREATE TRIGGER sp_insertLocalFile_t INSTEAD OF INSERT ON sp_insertLocalFile
BEGIN
  INSERT INTO tbl_content
    (md5Hash, size, contentType)
  VALUES
    (NEW.md5Hash, NEW.size, NEW.contentType)
  ON CONFLICT(md5Hash, size) DO UPDATE
    SET contentType = NEW.contentType,
        updated = excluded.updated
    WHERE contentType IS NOT NEW.contentType;

  INSERT INTO tbl_local_file
    (path, contentId, mtime)
    SELECT  NEW.path,
            contentId,
            datetime(NEW.mtime)
    FROM  tbl_content
    WHERE md5Hash = NEW.md5Hash
    AND   size = NEW.size
  ON CONFLICT(path) DO UPDATE
    SET (contentId, mtime) = (excluded.contentId, excluded.mtime),
        updated = excluded.updated
    WHERE (contentId, mtime) IS NOT (excluded.contentId, excluded.mtime);
END;

--- Delete local file ------------------

DROP VIEW IF EXISTS sp_deleteLocalFile;
CREATE VIEW sp_deleteLocalFile AS
  SELECT 0 path
  WHERE 0;
CREATE TRIGGER sp_deleteLocalFile_t INSTEAD OF INSERT ON sp_deleteLocalFile
BEGIN
  DELETE FROM tbl_local_file
  WHERE path = NEW.path;
END;

--- Insert S3 file ---------------------

DROP VIEW IF EXISTS sp_insertS3File;
CREATE VIEW sp_insertS3File AS
  SELECT 0 bucket, 0 path, 0 size, 0 mtime, 0 storage, 0 contentType, 0 md5Hash
  WHERE 0;
CREATE TRIGGER sp_insertS3File_t INSTEAD OF INSERT ON sp_insertS3File
BEGIN
  INSERT INTO tbl_content
    (md5Hash, size, contentType)
  VALUES
    (NEW.md5Hash, NEW.size, NEW.contentType)
  ON CONFLICT(md5Hash, size) DO UPDATE
    SET contentType = NEW.contentType,
        updated = excluded.updated
    WHERE contentType != NEW.contentType;

  INSERT INTO tbl_s3_file
    (bucket, path, contentId, mtime, storage)
    SELECT  NEW.bucket,
            NEW.path,
            contentId,
            datetime(NEW.mtime),
            NEW.storage
    FROM  tbl_content
    WHERE md5Hash = NEW.md5Hash
    AND   size = NEW.size
  ON CONFLICT(bucket, path) DO UPDATE
    SET (contentId, mtime, storage) =
          (excluded.contentId, datetime(NEW.mtime), NEW.storage),
        updated = excluded.updated
    WHERE (contentId, mtime, storage) IS NOT
            (excluded.contentId, datetime(NEW.mtime), NEW.storage);
END;

--- Delete S3 file ---------------------

DROP VIEW IF EXISTS sp_deleteS3File;
CREATE VIEW sp_deleteS3File AS
  SELECT 0 bucket, 0 path
  WHERE 0;
CREATE TRIGGER sp_deleteS3File_t INSTEAD OF INSERT ON sp_deleteS3File
BEGIN
  DELETE FROM tbl_s3_file
  WHERE bucket = NEW.bucket
  AND   path = NEW.path;
END;

--- Insert Gdrive file -----------------

DROP VIEW IF EXISTS sp_insertGdriveFile;
CREATE VIEW sp_insertGdriveFile AS
  SELECT 0 path, 0 size, 0 mtime, 0 googleId, 0 contentType, 0 md5Hash
  WHERE 0;
CREATE TRIGGER sp_insertGdriveFile_t INSTEAD OF INSERT ON sp_insertGdriveFile
BEGIN
  INSERT INTO tbl_content
    (md5Hash, size, contentType)
  VALUES
    (NEW.md5Hash, NEW.size, NEW.contentType)
  ON CONFLICT(md5Hash, size) DO UPDATE
    SET contentType = NEW.contentType,
        updated = excluded.updated
    WHERE contentType != NEW.contentType;

  INSERT INTO tbl_gdrive_file
    (path, contentId, mtime, googleId)
    SELECT  NEW.path,
            contentId,
            datetime(NEW.mtime),
            NEW.googleId
    FROM  tbl_content
    WHERE md5Hash = NEW.md5Hash
    AND   size = NEW.size
  ON CONFLICT(path) DO UPDATE
    SET (contentId, mtime, googleId) =
          (excluded.contentId, datetime(NEW.mtime), NEW.googleId),
        updated = excluded.updated
    WHERE (contentId, mtime, googleId) IS NOT
            (excluded.contentId, datetime(NEW.mtime), NEW.googleId);
END;

--- Delete Gdrive file ---------------------

DROP VIEW IF EXISTS sp_deleteGdriveFile;
CREATE VIEW sp_deleteGdriveFile AS
  SELECT 0 path
  WHERE 0;
CREATE TRIGGER sp_deleteGdriveFile_t INSTEAD OF INSERT ON sp_deleteGdriveFile
BEGIN
  DELETE FROM tbl_gdrive_file
  WHERE path = NEW.path;
END;

----------------------------------------
--  Queries  ---------------------------
----------------------------------------

--- Local files ------------------------

DROP VIEW IF EXISTS qry_localFiles;
CREATE VIEW qry_localFiles AS
  SELECT f.path             AS path,
         f.mtime            AS mtime,
         c.size             AS size,
         c.contentType      AS contentType,
         c.md5Hash          AS md5Hash,
         f.contentId        AS contentId
  FROM   tbl_local_file f
  JOIN   tbl_content c USING (contentId)
  ORDER BY path;

--- S3 files ---------------------------

DROP VIEW IF EXISTS qry_s3Files;
CREATE VIEW qry_s3Files AS
  SELECT f.bucket           AS bucket,
         f.path             AS path,
         f.mtime            AS mtime,
         c.size             AS size,
         f.storage          AS storage,
         c.contentType      AS contentType,
         c.md5Hash          AS md5Hash,
         f.contentId        AS contentId
  FROM   tbl_s3_file f
  JOIN   tbl_content c USING (contentId)
  ORDER BY bucket, path;

--- GDrive files -----------------------

DROP VIEW IF EXISTS qry_gdriveFiles;
CREATE VIEW qry_gdriveFiles AS
  SELECT f.path             AS path,
         f.mtime            AS mtime,
         c.size             AS size,
         f.googleId         AS googleId,
         c.contentType      AS contentType,
         c.md5Hash          AS md5Hash,
         f.contentId        AS contentId
  FROM   tbl_gdrive_file f
  JOIN   tbl_content c USING (contentId)
  ORDER BY path;

--- Files on both local and S3 ---------

DROP VIEW IF EXISTS qry_localAndS3;
CREATE VIEW qry_localAndS3 AS
  SELECT l.path      AS localPath,
         s.bucket    AS s3Bucket,
         s.path      AS s3Path,
         l.contentId AS contentId
  FROM   tbl_local_file l
  JOIN   tbl_s3_file s USING (contentId)
  GROUP BY contentId
  HAVING count(l.path) = 1
  AND    count(s.path) = 1;

--- Files on both local and Gdrive -----

DROP VIEW IF EXISTS qry_localAndGdrive;
CREATE VIEW qry_localAndGdrive AS
  SELECT l.path      AS localPath,
         r.path      AS gdrivePath,
         l.contentId AS contentId
  FROM   tbl_local_file l
  JOIN   tbl_gdrive_file r USING (contentId)
  GROUP BY contentId
  HAVING count(l.path) = 1
  AND    count(r.path) = 1;

--- Files with multiple copies ---------

DROP VIEW IF EXISTS qry_duplicates;
CREATE VIEW qry_duplicates(contentId) AS
  SELECT   contentId
  FROM     tbl_local_file
  GROUP BY contentId
  HAVING   count(contentId) > 1
  UNION DISTINCT
  SELECT   contentId
  FROM     tbl_s3_file
  GROUP BY contentId
  HAVING   count(contentId) > 1
  UNION DISTINCT
  SELECT   contentId
  FROM     tbl_gdrive_file
  GROUP BY contentId
  HAVING   count(contentId) > 1;

COMMIT;

ANALYZE;
VACUUM;
