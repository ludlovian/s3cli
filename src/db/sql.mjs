import SQL from '../lib/sql.mjs'
export const clearSync=SQL.from("DELETE FROM sync");
export const countFiles=SQL.from("SELECT count(*)  FROM sync  WHERE type = 'src'");
export const ddl=SQL.from("CREATE VIEW IF NOT EXISTS dbVersion AS SELECT 3 AS version;CREATE TABLE IF NOT EXISTS hash ( url TEXT NOT NULL PRIMARY KEY, mtime TEXT NOT NULL, size INTEGER NOT NULL, hash TEXT );CREATE TEMP TABLE sync ( type TEXT NOT NULL, path TEXT NOT NULL, url TEXT NOT NULL UNIQUE, mtime TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (type, path) );CREATE TEMP VIEW missingHashes AS SELECT a.url AS url  FROM sync a LEFT JOIN hash b ON b.url = a.url AND b.mtime = a.mtime AND b.size = a.size  WHERE b.hash IS NULL  ORDER BY a.url;CREATE TEMP VIEW missingFiles AS SELECT src.url AS url, src.path AS path  FROM sync src  LEFT JOIN sync dst ON src.path = dst.path AND dst.type = 'dst'  WHERE src.type = 'src' AND dst.path IS NULL  ORDER BY src.url;CREATE TEMP VIEW changedFiles AS SELECT src.url AS src, dst.url AS dst  FROM sync src  JOIN hash srcHash ON srcHash.url = src.url AND srcHash.size = src.size AND srcHash.mtime = src.mtime  JOIN sync dst ON dst.path = src.path AND dst.type = 'dst'  JOIN hash dstHash ON dstHash.url = dst.url AND dstHash.size = dst.size AND dstHash.mtime = dst.mtime  WHERE src.type = 'src' AND srcHash.hash != dstHash.hash  ORDER BY src.url;CREATE TEMP VIEW surplusFiles AS SELECT dst.url AS url  FROM sync dst  LEFT JOIN sync src ON src.path = dst.path AND src.type = 'src'  WHERE dst.type = 'dst' AND src.path IS NULL  ORDER BY dst.url");
export const deleteHash=SQL.from("DELETE FROM hash  WHERE url = $url");
export const insertHash=SQL.from("INSERT INTO hash (url, mtime, size, hash) VALUES ($url, datetime($mtime), $size, $hash) ON CONFLICT DO UPDATE SET mtime = excluded.mtime, size = excluded.size, hash = excluded.hash");
export const insertSync=SQL.from("INSERT INTO sync (type, path, url, mtime, size) VALUES ($type, $path, $url, datetime($mtime), $size)");
export const selectChanged=SQL.from("SELECT src AS \"from\", dst AS \"to\" from changedFiles");
export const selectHash=SQL.from("SELECT hash  FROM hash  WHERE url = $url AND mtime = datetime($mtime) AND size = $size");
export const selectMissingFiles=SQL.from("SELECT url, path FROM missingFiles");
export const selectMissingHashes=SQL.from("SELECT url FROM missingHashes");
export const selectSurplusFiles=SQL.from("SELECT url FROM surplusFiles");
