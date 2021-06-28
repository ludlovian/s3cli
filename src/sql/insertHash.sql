--
-- Inserts / updates a new hash record
--

INSERT INTO hash
    (url, mtime, "size", hash)
VALUES
    ($url, datetime($mtime), $size, $hash)
ON CONFLICT DO UPDATE
  SET mtime  = excluded.mtime,
      "size" = excluded."size",
      hash   = excluded.hash
