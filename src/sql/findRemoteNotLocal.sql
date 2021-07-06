-- Find files on S3 that are not on local

-- params
--      localRoot
--      s3Root

WITH localContent AS (
    SELECT  contentId
    FROM    local_file
    WHERE   'file://' || path LIKE $localRoot || '%'
)

SELECT  f.bucket    AS bucket,
        f.path      AS path,
        f.mtime     AS mtime,
        c.size      AS size,
        c.contentType
                    AS contentType,
        c.md5Hash   AS md5Hash,
        f.storage   AS storage

FROM    s3_file f
JOIN    content c USING (contentId)

WHERE   f.contentId NOT IN (
            SELECT  contentId
            FROM    localContent
        )
AND     's3://' || f.bucket || '/' || f.path
            LIKE $s3Root || '%'

ORDER BY    f.bucket,
            f.path;
