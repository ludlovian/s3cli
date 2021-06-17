const ddl = t`
  CREATE TABLE IF NOT EXISTS hash (
    url TEXT NOT NULL PRIMARY KEY,
    mtime TEXT NOT NULL,
    size INTEGER NOT NULL,
    hash TEXT
  );
  DROP TABLE IF EXISTS sync;
  CREATE TEMP TABLE IF NOT EXISTS sync (
    type TEXT NOT NULL,
    path TEXT NOT NULL,
    url  TEXT NOT NULL,
    mtime TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (type, path)
  );
`
const sql = {}

sql.insertHash = t`
  INSERT INTO hash
    (url, mtime, size, hash)
  VALUES
    ($url, $mtime, $size, $hash)
  ON CONFLICT DO UPDATE
  SET mtime = excluded.mtime,
      size  = excluded.size,
      hash  = excluded.hash
`

sql.insertSync = t`
  INSERT INTO sync
    (type, path, url, mtime, size)
  VALUES
    ($type, $path, $url, $mtime, $size)
`

sql.selectHash = t`
  SELECT hash
    FROM hash
    WHERE url = $url
      AND mtime = $mtime
      AND size = $size
`

sql.selectMissingFiles = t`
  SELECT a.url,
         a.path
    FROM sync a
    LEFT JOIN sync b
      ON  a.path = b.path
      AND b.type = 'dst'
    WHERE a.type = 'src'
      AND b.path IS NULL
    ORDER BY a.url
`

sql.selectMissingHashes = t`
  SELECT a.url
    FROM sync a
    LEFT JOIN hash b
      ON a.url    = b.url
      AND a.mtime = b.mtime
      AND a.size  = b.size
    WHERE b.hash IS NULL
    ORDER BY a.url
`

sql.selectChanged = t`
  SELECT a.url AS "from",
         b.url AS "to"
    FROM sync a
    JOIN sync b ON b.path = a.path
    JOIN hash c ON c.url = a.url
    JOIN hash d ON d.url = b.url
    WHERE a.type = 'src'
      AND b.type = 'dst'
      AND c.hash != d.hash
    ORDER BY a.url
`

sql.selectSurplusFiles = t`
  SELECT b.url AS url
    FROM sync b
    LEFT JOIN sync a
      ON  a.path = b.path
      AND a.type = 'src'
    WHERE b.type = 'dst'
      AND a.path IS NULL
    ORDER BY b.url
`

sql.countFiles = t`
  SELECT count(*)
    FROM sync
    WHERE type = 'src'
`

function t (strings) {
  return strings
    .map(s => s.split('\n'))
    .flat()
    .map(x => x.trim())
    .join(' ')
}

export { sql, ddl }
