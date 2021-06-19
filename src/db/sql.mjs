export const sql={};
sql.clearSync="DELETE FROM sync";
sql.countFiles="SELECT count(*) FROM sync WHERE \"type\" = 'src'";
export const ddl="CREATE TABLE IF NOT EXISTS hash ( url TEXT NOT NULL PRIMARY KEY, mtime TEXT NOT NULL, \"size\" INTEGER NOT NULL, hash TEXT ); DROP TABLE IF EXISTS sync; CREATE TEMP TABLE IF NOT EXISTS sync ( \"type\" TEXT NOT NULL, path TEXT NOT NULL, url TEXT NOT NULL, mtime TEXT NOT NULL, \"size\" INTEGER NOT NULL, PRIMARY KEY (\"type\", path) );"
sql.deleteHash="DELETE FROM hash WHERE url = $url";
sql.insertHash="INSERT INTO hash (url, mtime, \"size\", hash) VALUES ($url, $mtime, $size, $hash) ON CONFLICT DO UPDATE SET mtime = excluded.mtime, \"size\" = excluded.\"size\", hash = excluded.hash";
sql.insertSync="INSERT INTO sync (\"type\", path, url, mtime, \"size\") VALUES ($type, $path, $url, $mtime, $size)";
sql.selectChanged="SELECT src.url AS \"from\", dst.url AS \"to\" FROM sync src JOIN hash srcHash ON srcHash.url = src.url JOIN sync dst ON dst.path = src.path JOIN hash dstHash ON dstHash.url = dst.url WHERE src.\"type\" = 'src' AND dst.\"type\" = 'dst' AND srcHash.hash != dstHash.hash ORDER BY src.url";
sql.selectHash="SELECT hash FROM hash WHERE url = $url AND mtime = $mtime AND \"size\" = $size";
sql.selectMissingFiles="SELECT src.url AS url, src.path AS path FROM sync src LEFT JOIN sync dst ON src.path = dst.path AND dst.\"type\" = 'dst' WHERE src.\"type\" = 'src' AND dst.path IS NULL ORDER BY src.url";
sql.selectMissingHashes="SELECT a.url AS url FROM sync a LEFT JOIN hash b ON a.url = b.url AND a.mtime = b.mtime AND a.\"size\" = b.\"size\" WHERE b.hash IS NULL ORDER BY a.url";
sql.selectSurplusFiles="SELECT dst.url AS url FROM sync dst LEFT JOIN sync src ON src.path = dst.path AND src.\"type\" = 'src' WHERE dst.\"type\" = 'dst' AND src.path IS NULL ORDER BY dst.url";
