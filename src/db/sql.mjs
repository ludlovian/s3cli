import SQL from '../lib/sql.mjs'
export const cleanup=SQL.from("DELETE FROM s3_file WHERE updated IS NULL;DELETE FROM local_file WHERE updated IS NULL;DELETE FROM content WHERE contentId NOT IN ( SELECT contentId FROM s3_file UNION SELECT contentId FROM local_file )");
export const clearFilesBeforeScan=SQL.from("UPDATE s3_file SET updated = NULL WHERE $url LIKE 's3://%' AND 's3://' || bucket || '/' || path LIKE $url || '%';UPDATE local_file SET updated = NULL WHERE $url LIKE 'file://%' AND 'file://' || path LIKE $url || '%'");
export const ddl=SQL.from("PRAGMA journal_mode = WAL;PRAGMA foreign_keys = ON;CREATE VIEW IF NOT EXISTS dbVersion AS SELECT 4 AS version;CREATE TABLE IF NOT EXISTS content( contentId INTEGER PRIMARY KEY NOT NULL, md5Hash TEXT NOT NULL, size INTEGER NOT NULL, contentType TEXT, updated TEXT DEFAULT (datetime('now')), UNIQUE (md5Hash, size) );CREATE TABLE IF NOT EXISTS s3_file( bucket TEXT NOT NULL, path TEXT NOT NULL, contentId INTEGER NOT NULL REFERENCES content(contentId), mtime TEXT NOT NULL, storage TEXT NOT NULL, updated TEXT DEFAULT (datetime('now')), PRIMARY KEY (bucket, path) );CREATE TABLE IF NOT EXISTS local_file( path TEXT NOT NULL PRIMARY KEY, contentId INTEGER NOT NULL REFERENCES content(contentId), mtime TEXT NOT NULL, updated TEXT DEFAULT (datetime('now')) );CREATE VIEW IF NOT EXISTS s3_file_view AS SELECT f.bucket AS bucket, f.path AS path, c.size AS size, f.mtime AS mtime, f.storage AS storage, c.contentType AS contentType, c.md5Hash AS md5Hash FROM s3_file f JOIN content c USING (contentId) ORDER BY f.bucket, f.path;CREATE VIEW IF NOT EXISTS local_file_view AS SELECT f.path AS path, c.size AS size, f.mtime AS mtime, c.contentType AS contentType, c.md5Hash AS md5Hash FROM local_file f JOIN content c USING (contentId) ORDER BY f.path");
export const findDifferentPaths=SQL.from("WITH loc_path AS ( SELECT path, substr(path, length($localRoot) - 6) AS rel_path FROM local_file ),  rem_path AS ( SELECT bucket, path, substr(path, length($s3Root) - length(bucket) - 5) AS rel_path FROM s3_file )  SELECT lp.rel_path AS localPath, l.mtime AS localMtime, rp.rel_path AS remotePath, r.mtime AS remoteMtime, r.storage AS storage, c.size AS size, c.contentType AS contentType, c.md5Hash AS md5Hash  FROM local_file l JOIN s3_file r USING (contentId) JOIN content c USING (contentId)  JOIN loc_path lp ON lp.path = l.path  JOIN rem_path rp ON rp.bucket = r.bucket AND rp.path = r.path  WHERE 'file://' || l.path LIKE $localRoot || '%' AND 's3://' || r.bucket || '/' || r.path LIKE $s3Root || '%' AND lp.rel_path != rp.rel_path  ORDER BY lp.rel_path");
export const findDuplicates=SQL.from("WITH dups AS ( SELECT contentId FROM local_file GROUP BY contentId HAVING count(contentId) > 1 UNION SELECT contentId FROM s3_file GROUP BY contentId HAVING count(contentId) > 1 ) SELECT contentId AS contentId, 's3://' || bucket || '/' || path AS url FROM s3_file WHERE contentId IN (SELECT contentId FROM dups)  UNION ALL  SELECT contentId AS contentId, 'file://' || path AS url FROM local_file WHERE contentId IN (SELECT contentId FROM dups) ORDER BY contentId");
export const findLocalNotRemote=SQL.from("WITH remoteContent AS ( SELECT contentId FROM s3_file WHERE 's3://' || bucket || '/' || path LIKE $s3Root || '%' )  SELECT f.path AS path, f.mtime AS mtime, c.size AS size, c.contentType AS contentType, c.md5Hash AS md5Hash  FROM local_file f JOIN content c USING (contentId) WHERE f.contentId NOT IN ( SELECT contentId FROM remoteContent ) AND 'file://' || f.path LIKE $localRoot || '%'  ORDER BY f.path");
export const findRemoteNotLocal=SQL.from("WITH localContent AS ( SELECT contentId FROM local_file WHERE 'file://' || path LIKE $localRoot || '%' )  SELECT f.bucket AS bucket, f.path AS path, f.mtime AS mtime, c.size AS size, c.contentType AS contentType, c.md5Hash AS md5Hash, f.storage AS storage  FROM s3_file f JOIN content c USING (contentId)  WHERE f.contentId NOT IN ( SELECT contentId FROM localContent ) AND 's3://' || f.bucket || '/' || f.path LIKE $s3Root || '%'  ORDER BY f.bucket, f.path");
export const insertLocalFile=SQL.from("INSERT INTO content (md5Hash, size, contentType) VALUES ($md5Hash, $size, $contentType)  ON CONFLICT DO UPDATE SET contentType = excluded.contentType, updated = excluded.updated;INSERT INTO local_file (path, contentId, mtime) SELECT $path, contentId, datetime($mtime) FROM content WHERE md5Hash = $md5Hash AND size = $size  ON CONFLICT DO UPDATE SET contentId = excluded.contentId, mtime = excluded.mtime, updated = excluded.updated");
export const insertS3File=SQL.from("INSERT INTO content (md5Hash, size, contentType) VALUES ($md5Hash, $size, $contentType)  ON CONFLICT DO UPDATE SET contentType = excluded.contentType, updated = excluded.updated;INSERT INTO s3_file (bucket, path, contentId, mtime, storage) SELECT $bucket, $path, contentId, datetime($mtime), $storage FROM content WHERE md5Hash = $md5Hash AND size = $size  ON CONFLICT DO UPDATE SET contentId = excluded.contentId, mtime = excluded.mtime, storage = excluded.storage, updated = excluded.updated");
export const listLocalFiles=SQL.from("SELECT path AS path, size AS size, mtime AS mtime, contentType AS contentType, NULL AS storage, md5Hash AS md5Hash  FROM local_file_view  WHERE path LIKE $path || '%'  ORDER BY path");
export const listS3files=SQL.from("SELECT bucket AS bucket, path AS path, size AS size, mtime AS mtime, contentType AS contentType, storage AS storage, md5Hash AS md5Hash  FROM s3_file_view  WHERE bucket = $bucket AND path LIKE $path || '%'  ORDER BY bucket, path");
export const moveLocalFile=SQL.from("UPDATE local_file SET path = $newPath WHERE path = $oldPath");
export const moveS3file=SQL.from("UPDATE s3_file  SET path = $newPath  WHERE bucket = $bucket AND path = $oldPath");
export const removeLocalFile=SQL.from("DELETE FROM local_file WHERE path = $path");
export const removeS3file=SQL.from("DELETE FROM s3_file WHERE bucket = $bucket AND path = $path");
export const selectLocalHash=SQL.from("SELECT c.md5Hash  FROM content c JOIN local_file f USING (contentId)  WHERE f.path = $path AND c.size = $size AND f.mtime = datetime($mtime)");
