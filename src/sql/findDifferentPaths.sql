-- find files that have different paths on S3 and local
-- params
--
--      localRoot
--      s3Root

WITH
    loc_path AS (
        SELECT  path,
                substr(path, length($localRoot) - 6)
                        AS rel_path
        FROM    local_file
    ),

    rem_path AS (
        SELECT  bucket,
                path,
                substr(path, length($s3Root) - length(bucket) - 5)
                        AS rel_path
        FROM    s3_file
    )

SELECT  lp.rel_path     AS localPath,
        l.mtime         AS localMtime,
        rp.rel_path     AS remotePath,
        r.mtime         AS remoteMtime,
        r.storage       AS storage,
        c.size          AS size,
        c.contentType   AS contentType,
        c.md5Hash       AS md5Hash

FROM    local_file l
JOIN    s3_file r USING (contentId)
JOIN    content c USING (contentId)

JOIN    loc_path lp
    ON  lp.path = l.path

JOIN    rem_path rp
    ON  rp.bucket = r.bucket
    AND rp.path   = r.path

WHERE   'file://' || l.path LIKE $localRoot || '%'
AND     's3://' || r.bucket || '/' || r.path LIKE $s3Root || '%'
AND     lp.rel_path != rp.rel_path

ORDER BY lp.rel_path;


