-- Find files on S3 that are not on local

-- params
--      localRoot
--      s3Root

SELECT  f.bucket    AS bucket,
        f.path      AS path,
        f.mtime     AS mtime,
        c.size      AS size,
        c.contentType
                    AS contentType,
        c.md5Hash   AS md5Hash,
        f.storage   AS storage

FROM    s3_file          f
JOIN    content          c USING (contentId)
JOIN    content_use_view u USING (contentId)

WHERE   u.local_use = 0
AND     u.remote_use = 1
AND     's3://' || f.bucket || '/' || f.path
            LIKE $s3Root || '%'

ORDER BY    f.bucket,
            f.path;
