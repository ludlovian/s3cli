-- Find files on local that are not on S3

-- params
--      localRoot
--      s3Root

WITH remoteContent AS (
    SELECT  contentId
    FROM    s3_file
    WHERE   's3://' || bucket || '/' || path LIKE $s3Root || '%'
)

SELECT  f.path      AS path,
        f.mtime     AS mtime,
        c.size      AS size,
        c.contentType
                    AS contentType,
        c.md5Hash   AS md5Hash

FROM    local_file f
JOIN    content c USING (contentId)
WHERE   f.contentId NOT IN (
            SELECT  contentId
            FROM    remoteContent
        )
AND     'file://' || f.path
            LIKE $localRoot || '%'

ORDER BY f.path;
